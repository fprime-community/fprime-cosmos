"""Tests for the ``cosmos`` fprime-gds data-transport plugin (fake JSON-RPC + fake ActionCable stream)"""

from __future__ import annotations

import json
import logging
import threading
from importlib.metadata import entry_points
from unittest.mock import MagicMock, patch

import pytest
import requests
from fprime_gds.common.data_types.ch_data import ChData
from fprime_gds.common.data_types.cmd_data import CmdData
from fprime_gds.common.models.dictionaries import Dictionaries
from fprime_gds.common.models.serialize.bool_type import BoolType
from fprime_gds.common.models.serialize.enum_type import EnumType
from fprime_gds.common.models.serialize.numerical_types import F32Type, I32Type, U8Type
from fprime_gds.common.models.serialize.string_type import StringType
from fprime_gds.common.models.serialize.time_type import TimeType
from fprime_gds.common.transport import TransportationException

from fprime_cosmos import emit
from fprime_cosmos.dictionary import FprimeDictionary
from fprime_cosmos.gds import transport
from fprime_cosmos.gds.transport import (
    PASSWORD_ENV,
    CosmosApi,
    CosmosNaming,
    CosmosTransport,
    CosmosTransportError,
    StreamingSocket,
    assemble_value,
    build_value_object,
    coerce_scalar,
    redact_url,
    split_stream_packet,
)
from tests.conftest import REFERENCE_DICTIONARY

STREAM_TIME = 1_700_000_000_000_000_000
SECRET = "s3cr3t-Passw0rd"
FPRIME_TIME = {
    "FPRIME_TIME_BASE": "TB_WORKSTATION_TIME",
    "FPRIME_TIME_CONTEXT": 0,
    "FPRIME_TIME_SECONDS": 1234,
    "FPRIME_TIME_USECONDS": 5678,
}
COSMOS_DERIVED = {
    "PACKET_TIMESECONDS": 1762744000.5,
    "PACKET_TIMEFORMATTED": "2025/11/10 02:26:40.500",
    "RECEIVED_TIMESECONDS": 1762744000.5,
    "RECEIVED_TIMEFORMATTED": "2025/11/10 02:26:40.500",
    "RECEIVED_COUNT": 9,
}


@pytest.fixture
def dictionaries():
    dicts = Dictionaries()
    dicts.load_dictionaries(str(REFERENCE_DICTIONARY), None, None)
    return dicts


@pytest.fixture
def client(dictionaries):
    client = CosmosTransport(cosmos_url="http://cosmos:2900", cosmos_target="FPRIME")
    client.set_pipeline_references(dictionaries, MagicMock(), MagicMock())
    client._index_channels()
    return client


def delivered(client) -> list[ChData]:
    return [call.args[0] for call in client.channel_decoder.send_to_all.call_args_list]


def channel_packet(name: str, items: dict, time_items: dict | None = FPRIME_TIME, derived: bool = True) -> dict:
    """A COSMOS 7 DECOM stream packet for one of the generator's per-channel packets"""
    packet = {"__time": STREAM_TIME, "__type": "PACKET", "__packet": f"DECOM__TLM__FPRIME__{name.upper()}__CONVERTED"}
    if derived:
        packet.update(COSMOS_DERIVED)  # COSMOS lists its derived items ahead of the plugin's
    packet.update(time_items or {})
    packet.update(items)
    return packet


class FakeResponse:
    def __init__(self, status=200, payload=None, text=None):
        self.status_code = status
        self._payload = payload
        self.text = text if text is not None else json.dumps(payload)
        self.request = MagicMock(method="GET")
        self.url = "http://cosmos:2900/x"

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class TestPluginContract:
    def test_entry_point_registers_the_cosmos_transport(self):
        eps = entry_points()
        group = eps.select(group="fprime_gds") if hasattr(eps, "select") else eps.get("fprime_gds", [])
        matches = [ep for ep in group if ep.name == "cosmos"]
        assert [ep.value for ep in matches] == ["fprime_cosmos.gds.transport:CosmosTransport"]
        assert matches[0].load() is CosmosTransport

    def test_plugin_system_discovers_cosmos_when_the_category_exists(self):
        from fprime_gds.plugin.system import Plugins

        if "data_transport" not in Plugins.get_all_categories():
            pytest.skip("installed fprime-gds predates the data_transport category")
        plugins = Plugins(["data_transport"]).get_plugins("data_transport")
        names = {plugin.plugin_class.get_name(): plugin.plugin_class for plugin in plugins}
        assert names["cosmos"] is CosmosTransport

    def test_arguments_flow_through_the_gds_command_line(self, capsys):
        from fprime_gds.plugin.system import Plugins

        if "data_transport" not in Plugins.get_all_categories():
            pytest.skip("installed fprime-gds predates the data_transport category")
        from fprime_gds.executables.cli import MiddleWareParser, ParserBase

        arguments = [
            "--data-transport-selection", "cosmos",
            "--cosmos-url", "https://cosmos.example:2943",
            "--cosmos-scope", "OPS",
            "--cosmos-target", "FPRIME_APP",
            "--cosmos-layout", "cfs",
        ]  # fmt: skip
        args, _ = ParserBase.parse_args([MiddleWareParser], arguments=arguments, client=True)
        built = args.connection_transport()
        assert type(built) is CosmosTransport
        assert (built.scope, built.target, built.naming.layout) == ("OPS", "FPRIME_APP", "cfs")
        assert args.connection_uri == "cosmos+https://cosmos.example:2943"
        reproduced = MiddleWareParser().reproduce_cli_args(args)
        for flag, value in zip(arguments[::2], arguments[1::2], strict=True):
            assert reproduced[reproduced.index(flag) + 1] == value, (flag, reproduced)
        with pytest.raises(SystemExit) as exit_info:
            ParserBase.parse_args(
                [MiddleWareParser],
                arguments=["--data-transport-selection", "cosmos", "--cosmos-url", f"http://u:{SECRET}@host"],
                client=True,
            )
        assert exit_info.value.code not in (None, 0)
        err = capsys.readouterr().err
        assert "must not carry credentials" in err and SECRET not in err

    def test_name_arguments_and_uri(self):
        assert CosmosTransport.get_name() == "cosmos"
        assert CosmosTransport.register_data_transport_plugin() is CosmosTransport
        dests = {spec["dest"] for spec in CosmosTransport.get_arguments().values()}
        assert dests == {"cosmos_url", "cosmos_scope", "cosmos_target", "cosmos_layout"}
        assert "password" not in " ".join(dests)
        assert CosmosTransport.get_connection_uri(cosmos_url="http://host:2900") == "cosmos://host:2900"
        assert CosmosTransport.get_connection_uri(cosmos_url="host:2900") == "cosmos://host:2900"
        assert CosmosTransport.get_connection_uri(cosmos_url="https://host") == "cosmos+https://host"
        assert CosmosTransport._parse_uri("cosmos+https://host") == "https://host"
        assert CosmosTransport._parse_uri("cosmos://host:2900") == "http://host:2900"
        assert issubclass(CosmosTransportError, TransportationException)

    @pytest.mark.parametrize(
        "kwargs, message",
        [
            ({"cosmos_url": "ftp://host"}, "http:// or https://"),
            ({"cosmos_url": "http://user:pw@host"}, "must not carry credentials"),
            ({"cosmos_layout": "yamcs"}, "--cosmos-layout"),
            ({"cosmos_target": ""}, "must not be empty"),
        ],
    )
    def test_check_arguments_rejects_bad_input(self, kwargs, message):
        with pytest.raises(ValueError, match=message) as info:
            CosmosTransport.check_arguments(**kwargs)
        assert "pw" not in str(info.value)

    @pytest.mark.parametrize("url", [f"ftp://user:{SECRET}@host", f"http://user:{SECRET}@host", f"user:{SECRET}@host"])
    def test_credentials_in_the_url_are_rejected_without_being_echoed(self, url):
        for check in (CosmosTransport.check_arguments, CosmosTransport.get_connection_uri):
            with pytest.raises(ValueError) as info:
                check(cosmos_url=url)
            assert SECRET not in str(info.value)

    def test_connect_rejects_a_uri_carrying_credentials(self, client, monkeypatch):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        with pytest.raises(ValueError, match="must not carry credentials") as info:
            client.connect("cosmos://user:s3cret@host:2900")
        assert "s3cret" not in str(info.value)

    def test_redact_url(self):
        assert redact_url("http://u:p@host:1/x") == "http://host:1/x"
        assert redact_url("u:p@host") == "host"
        assert redact_url("http://host") == "http://host"


class TestCommands:
    def test_issue_command_uses_structured_json_rpc_parameters(self, client, dictionaries):
        template = dictionaries.command_name["CdhCore.cmdDisp.CMD_NO_OP_STRING"]
        client.api = MagicMock()
        client.data_callback(CmdData(("it's \\ quoted",), template))
        client.api.call.assert_called_once_with(
            "cmd_no_hazardous_check", "FPRIME", "CdhCore.cmdDisp.CMD_NO_OP_STRING", {"ARG1": "it's \\ quoted"}
        )
        client.send(b"\x00\x01")  # encoded bytes from the pipeline are not routed anywhere
        assert client.recv() == b""

    def test_issue_command_without_arguments(self, client, dictionaries):
        client.api = MagicMock()
        client.data_callback(CmdData((), dictionaries.command_name["CdhCore.cmdDisp.CMD_NO_OP"]))
        client.api.call.assert_called_once_with("cmd_no_hazardous_check", "FPRIME", "CdhCore.cmdDisp.CMD_NO_OP", {})

    def test_typed_arguments_are_unwrapped_and_paired_by_name(self, client, dictionaries):
        client.api = MagicMock()
        client.data_callback(CmdData(("-1", "2.5", "7"), dictionaries.command_name["CdhCore.cmdDisp.CMD_TEST_CMD_1"]))
        client.data_callback(
            CmdData(("WARNING_LO", "DISABLED"), dictionaries.command_name["CdhCore.events.SET_EVENT_FILTER"])
        )
        client.data_callback(
            CmdData(("/tmp/x", "FALSE"), dictionaries.command_name["FileHandling.fileManager.RemoveFile"])
        )
        calls = [call.args for call in client.api.call.call_args_list]
        assert calls == [
            (
                "cmd_no_hazardous_check",
                "FPRIME",
                "CdhCore.cmdDisp.CMD_TEST_CMD_1",
                {"ARG1": -1, "ARG2": 2.5, "ARG3": 7},
            ),
            (
                "cmd_no_hazardous_check",
                "FPRIME",
                "CdhCore.events.SET_EVENT_FILTER",
                {"FILTERLEVEL": "WARNING_LO", "FILTERENABLED": "DISABLED"},
            ),
            (
                "cmd_no_hazardous_check",
                "FPRIME",
                "FileHandling.fileManager.RemoveFile",
                {"FILENAME": "/tmp/x", "IGNOREERRORS": False},
            ),
        ]
        assert type(calls[0][3]["ARG2"]) is float and type(calls[2][3]["IGNOREERRORS"]) is bool

    def test_non_command_data_goes_to_the_base_transport(self, client):
        with patch.object(client, "send") as send:
            client.data_callback(b"\x01\x02")
        send.assert_called_once_with(b"\x01\x02")

    def test_cosmos_rejection_is_logged_and_raised(self, client, dictionaries, caplog):
        template = dictionaries.command_name["CdhCore.cmdDisp.CMD_NO_OP"]
        client.api = MagicMock()
        client.api.call.side_effect = CosmosTransportError("boom")
        with (
            caplog.at_level(logging.ERROR, logger="cosmos_transport"),
            pytest.raises(CosmosTransportError, match="boom"),
        ):
            client.data_callback(CmdData((), template))
        assert "CMD_NO_OP" in caplog.text and "boom" in caplog.text

    def test_command_before_connect_fails(self, client, dictionaries):
        with pytest.raises(CosmosTransportError, match="not connected"):
            client.data_callback(CmdData((), dictionaries.command_name["CdhCore.cmdDisp.CMD_NO_OP"]))


class TestNaming:
    def test_fprime_layout(self, dictionaries):
        naming = CosmosNaming("fprime")
        template = dictionaries.channel_name["CdhCore.cmdDisp.CommandsDispatched"]
        assert naming.channel_packet(template) == "CDHCORE.CMDDISP.COMMANDSDISPATCHED"
        assert naming.dedicated_item(template) == "COMMANDSDISPATCHED"
        assert naming.channel_key("CdhCore.cmdDisp.CommandsDispatched") == "CDHCORE.CMDDISP.COMMANDSDISPATCHED"
        assert naming.command_name("CdhCore.cmdDisp.CMD_NO_OP") == "CdhCore.cmdDisp.CMD_NO_OP"
        assert naming.command_parameter("arg1") == "ARG1"

    def test_cfs_layout(self, dictionaries):
        naming = CosmosNaming("cfs")
        template = dictionaries.channel_name["CdhCore.cmdDisp.CommandsDispatched"]
        assert naming.channel_packet(template) is None
        assert naming.dedicated_item(template) == "CDHCORE.CMDDISP.COMMANDSDISPATCHED"
        with pytest.raises(ValueError, match="Unknown COSMOS layout"):
            CosmosNaming("yamcs")

    def test_generator_round_trip(self, dictionaries):
        """The item names the generator emits for a channel are the ones the transport reassembles"""
        fprime_dictionary = FprimeDictionary(REFERENCE_DICTIONARY)
        template = dictionaries.channel_name["CdhCore.version.CustomVersion10"]
        lines = emit.emit_channel(template, fprime_dictionary.layout)
        assert lines[0].startswith("TELEMETRY <%= target_name %> CdhCore.version.CustomVersion10 ")
        item_names = [line.split()[1].upper() for line in lines if line.strip().startswith("APPEND_")]
        assert item_names[0] == transport.CHANNEL_ID_ITEM
        value_items = [name for name in item_names if name not in transport.NON_VALUE_ITEMS]
        assert value_items == [
            "CUSTOMVERSION10.VERSION_ENUM",
            "CUSTOMVERSION10.VERSION_VALUE_LENGTH",
            "CUSTOMVERSION10.VERSION_VALUE",
            "CUSTOMVERSION10.VERSION_STATUS",
        ]
        items = {"CUSTOMVERSION10.VERSION_ENUM": "PROJECT_VERSION_01", "CUSTOMVERSION10.VERSION_VALUE_LENGTH": 3}
        items.update({"CUSTOMVERSION10.VERSION_VALUE": "1.2", "CUSTOMVERSION10.VERSION_STATUS": "OK"})
        assert assemble_value(items, "CUSTOMVERSION10", template.get_type_obj()) == {
            "version_enum": "PROJECT_VERSION_01",
            "version_value": "1.2",
            "version_status": "OK",
        }


class TestStreamDecoding:
    def test_split_stream_packet_parses_cosmos7_packet_key(self):
        name, items = split_stream_packet(
            {
                "__packet": "DECOM__TLM__FPRIME__CDHCORE.CMDDISP.COMMANDSDISPATCHED__CONVERTED",
                "__time": 5,
                "__type": "PACKET",
                "CommandsDispatched": 3,
            }
        )
        assert name == "CDHCORE.CMDDISP.COMMANDSDISPATCHED"
        assert items == {"__time": 5, "COMMANDSDISPATCHED": 3}

    def test_split_stream_packet_handles_long_item_keys_and_metadata(self):
        packet = {
            "__time": STREAM_TIME,
            "__type": "DECOM",
            "DECOM__TLM__FPRIME__CdhCore.cmdDisp.CommandsDispatched__COMMANDSDISPATCHED__CONVERTED": 7,
            "DECOM__TLM__FPRIME__CdhCore.cmdDisp.CommandsDispatched__FPRIME_TIME_SECONDS__CONVERTED": 10,
        }
        name, items = split_stream_packet(packet)
        assert name == "CdhCore.cmdDisp.CommandsDispatched"
        assert items == {"__time": STREAM_TIME, "COMMANDSDISPATCHED": 7, "FPRIME_TIME_SECONDS": 10}

    def test_split_stream_packet_handles_plain_item_keys(self):
        name, items = split_stream_packet({"__packet": "FPRIME__HEALTH", "CfsCore.cmdDisp.CommandsDispatched": 1})
        assert name == "HEALTH"
        assert items == {"CFSCORE.CMDDISP.COMMANDSDISPATCHED": 1}

    def test_channel_packet_becomes_ch_data_with_fprime_time(self, client):
        client._on_packet(
            channel_packet(
                "CdhCore.cmdDisp.CommandsDispatched", {"FPRIME_CHANNEL_ID": 16777216, "COMMANDSDISPATCHED": 42}
            )
        )
        (ch,) = delivered(client)
        assert isinstance(ch, ChData)
        assert ch.get_template().get_full_name() == "CdhCore.cmdDisp.CommandsDispatched"
        assert ch.get_val() == 42
        assert (ch.get_time().seconds, ch.get_time().useconds) == (1234, 5678)
        assert ch.get_time().timeBase.val == "TB_WORKSTATION_TIME"

    def test_channel_id_wins_over_packet_name(self, client):
        packet = channel_packet("SOME.RENAMED.PACKET", {"FPRIME_CHANNEL_ID": 16777217, "COMMANDERRORS": 2})
        client._on_packet(packet)
        (ch,) = delivered(client)
        assert ch.get_template().get_full_name() == "CdhCore.cmdDisp.CommandErrors" and ch.get_val() == 2

    def test_unknown_channel_id_is_reported_not_guessed(self, client, caplog):
        with caplog.at_level(logging.WARNING, logger="cosmos_transport"):
            client._on_packet(channel_packet("CdhCore.cmdDisp.CommandErrors", {"FPRIME_CHANNEL_ID": 99, "X": 1}))
        assert delivered(client) == [] and "unknown F Prime channel id 99" in caplog.text

    def test_packet_without_channel_id_is_resolved_by_packet_name(self, client):
        client._on_packet(channel_packet("CdhCore.cmdDisp.CommandErrors", {"COMMANDERRORS": 5}))
        (ch,) = delivered(client)
        assert ch.get_template().get_full_name() == "CdhCore.cmdDisp.CommandErrors" and ch.get_val() == 5

    def test_string_channel_ignores_the_length_item_and_derived_items(self, client):
        items = {"FPRIME_CHANNEL_ID": 16789505, "PROJECTVERSION_LENGTH": 6, "PROJECTVERSION": "v1.2.3"}
        client._on_packet(channel_packet("CdhCore.version.ProjectVersion", items))
        (ch,) = delivered(client)
        assert ch.get_val() == "v1.2.3"

    def test_struct_channel_is_rebuilt_from_member_items(self, client):
        items = {
            "FPRIME_CHANNEL_ID": 16789515,
            "CUSTOMVERSION10.VERSION_ENUM": "PROJECT_VERSION_02",
            "CUSTOMVERSION10.VERSION_VALUE_LENGTH": 5,
            "CUSTOMVERSION10.VERSION_VALUE": "1.0.0",
            "CUSTOMVERSION10.VERSION_STATUS": 1,
        }
        client._on_packet(channel_packet("CdhCore.version.CustomVersion10", items))
        (ch,) = delivered(client)
        assert ch.get_val() == {
            "version_enum": "PROJECT_VERSION_02",
            "version_value": "1.0.0",
            "version_status": "FAILURE",
        }

    def test_numeric_array_channel_is_rebuilt_from_the_array_item(self, client):
        client._on_packet(
            channel_packet("ComCcsds.comQueue.comQueueDepth", {"FPRIME_CHANNEL_ID": 33554432, "COMQUEUEDEPTH": [3, 4]})
        )
        (ch,) = delivered(client)
        assert ch.get_val() == [3, 4]
        client.channel_decoder.reset_mock()
        items = {"FPRIME_CHANNEL_ID": 33554432, "COMQUEUEDEPTH[0]": 7, "COMQUEUEDEPTH[1]": 8}
        client._on_packet(channel_packet("ComCcsds.comQueue.comQueueDepth", items))
        (ch,) = delivered(client)
        assert ch.get_val() == [7, 8]

    def test_unconvertible_value_is_skipped_with_one_warning(self, client, caplog):
        with caplog.at_level(logging.WARNING, logger="cosmos_transport"):
            for _ in range(2):
                client._on_packet(
                    channel_packet(
                        "CdhCore.cmdDisp.CommandsDispatched", {"FPRIME_CHANNEL_ID": 16777216, "COMMANDSDISPATCHED": 1.5}
                    )
                )
        assert delivered(client) == []
        assert caplog.text.count("Cannot rebuild channel CdhCore.cmdDisp.CommandsDispatched") == 1

    def test_packetized_layout_maps_items_by_full_name(self, dictionaries):
        client = CosmosTransport(cosmos_url="http://c", cosmos_target="FPRIME_APP", cosmos_layout="cfs")
        client.set_pipeline_references(dictionaries, MagicMock(), None)
        client._index_channels()
        client._on_packet(
            {
                "__time": STREAM_TIME,
                "__packet": "FPRIME_APP__HEALTH",
                **COSMOS_DERIVED,
                "FPRIME_TIME_BASE": 1,
                "FPRIME_TIME_SECONDS": 1,
                "FPRIME_TIME_USECONDS": 2,
                "CDHCORE.CMDDISP.COMMANDSDISPATCHED": 3,
                "CDHCORE.CMDDISP.COMMANDERRORS": 4,
                "CDHCORE.VERSION.CUSTOMVERSION10.VERSION_ENUM": "PROJECT_VERSION_00",
                "CDHCORE.VERSION.CUSTOMVERSION10.VERSION_VALUE_LENGTH": 1,
                "CDHCORE.VERSION.CUSTOMVERSION10.VERSION_VALUE": "x",
                "CDHCORE.VERSION.CUSTOMVERSION10.VERSION_STATUS": "OK",
                "UNRELATED.ITEM": 5,
            }
        )
        received = {ch.get_template().get_full_name(): ch.get_val() for ch in delivered(client)}
        assert received == {
            "CdhCore.cmdDisp.CommandErrors": 4,
            "CdhCore.cmdDisp.CommandsDispatched": 3,
            "CdhCore.version.CustomVersion10": {
                "version_enum": "PROJECT_VERSION_00",
                "version_value": "x",
                "version_status": "OK",
            },
        }
        times = {
            (ch.get_time().timeBase.val, ch.get_time().seconds, ch.get_time().useconds) for ch in delivered(client)
        }
        assert times == {(TimeType(time_base=1).timeBase.val, 1, 2)}
        assert TimeType(time_base=1).timeBase.val != "TB_WORKSTATION_TIME"

    def test_missing_fprime_time_falls_back_to_the_stream_time(self, client):
        time_obj = CosmosTransport._packet_time({"__time": 1_700_000_000_500_000_000})
        assert time_obj.seconds == 1_700_000_000 and time_obj.useconds == 500_000
        assert time_obj.timeBase.val == "TB_WORKSTATION_TIME"
        garbage = CosmosTransport._packet_time(
            {"__time": "soon", "FPRIME_TIME_BASE": "TB_NOPE", "FPRIME_TIME_SECONDS": "x"}
        )
        assert garbage.seconds == 0

    def test_coerce_scalar_types(self):
        enum = EnumType.construct_type("E", {"A": 0, "B": 1})
        assert coerce_scalar("TRUE", BoolType) is True and coerce_scalar("off", BoolType) is False
        assert coerce_scalar(0, BoolType) is False
        assert coerce_scalar("7", U8Type) == 7 and coerce_scalar(7.0, I32Type) == 7
        assert coerce_scalar("1.5", F32Type) == 1.5
        assert coerce_scalar(3, StringType.construct_type("S", 40)) == "3"
        assert coerce_scalar("B", enum) == "B" and coerce_scalar(1, enum) == "B"
        for value, type_class in [("maybe", BoolType), (1.5, I32Type), ("C", enum), (7, enum)]:
            with pytest.raises(ValueError):
                coerce_scalar(value, type_class)

    def test_build_value_object_instantiates_the_dictionary_type(self):
        assert build_value_object(5, U8Type).val == 5
        obj = build_value_object(-3, I32Type())
        assert obj.val == -3 and build_value_object(4, I32Type).val == 4  # no shared-class mutation


class TestApi:
    def test_authenticate_requires_a_password_and_keeps_it_out_of_errors(self):
        api = CosmosApi("http://cosmos:2900", "DEFAULT", None)
        with pytest.raises(CosmosTransportError, match=PASSWORD_ENV):
            api.authenticate()

    def test_authenticate_exchanges_the_password_for_a_token(self):
        api = CosmosApi("http://cosmos:2900/", "DEFAULT", "s3cret")
        api.session = MagicMock()
        api.session.headers = {}
        api.session.get.return_value = FakeResponse(payload={"result": True})
        api.session.post.return_value = FakeResponse(text="tok3n\n")
        assert api.authenticate() == "tok3n"
        assert api.session.headers["Authorization"] == "tok3n"
        assert api.session.post.call_args.kwargs["data"] == {"password": "s3cret"}
        assert api.cable_url() == "ws://cosmos:2900/openc3-api/cable?scope=DEFAULT"

    def test_password_only_travels_in_the_verify_form_body(self):
        api = CosmosApi("http://cosmos:2900", "DEFAULT", SECRET)
        api.session = MagicMock()
        api.session.headers = {}
        api.session.get.return_value = FakeResponse(payload={"result": True})
        api.session.post.return_value = FakeResponse(text="tok3n")
        api.authenticate()
        (url,), kwargs = api.session.post.call_args
        assert url == "http://cosmos:2900/openc3-api/auth/verify"
        assert set(kwargs) == {"data", "timeout"} and kwargs["data"] == {"password": SECRET}
        assert SECRET not in api.cable_url() and SECRET not in api.url

    @pytest.mark.parametrize(
        "get, post, message",
        [
            (requests.ConnectionError("refused"), None, "Cannot reach COSMOS"),
            (FakeResponse(payload={"result": False}), None, "no password set"),
            (FakeResponse(status=503, payload={}), None, r"failed \(503\)"),
            (FakeResponse(payload={"result": True}), requests.Timeout("slow"), "Cannot reach COSMOS"),
            (FakeResponse(payload={"result": True}), FakeResponse(status=401, text="nope"), r"failed \(401\)"),
            (FakeResponse(payload={"result": True}), FakeResponse(text=" \n"), "rejected the password"),
        ],
    )
    def test_authenticate_failures_are_reported_without_the_password(self, get, post, message):
        api = CosmosApi("http://cosmos:2900", "DEFAULT", SECRET)
        api.session = MagicMock()
        api.session.headers = {}
        api.session.get.side_effect = [get]
        api.session.post.side_effect = [post]
        with pytest.raises(CosmosTransportError, match=message) as info:
            api.authenticate()
        assert SECRET not in str(info.value)
        assert api.token in (None, "") and "Authorization" not in api.session.headers

    @pytest.mark.parametrize(
        "response, message",
        [
            (FakeResponse(status=200, payload={"jsonrpc": "2.0", "error": {"message": "nope"}, "id": 1}), "nope"),
            (FakeResponse(status=400, text="bad request"), "bad request"),
        ],
    )
    def test_call_raises_on_a_json_rpc_error_or_an_http_error_alone(self, response, message):
        api = CosmosApi("http://cosmos", "DEFAULT", "pw")
        api.session = MagicMock()
        api.session.post.return_value = response
        with pytest.raises(CosmosTransportError, match=message):
            api.call("cmd_no_hazardous_check", "FPRIME", "X", {})

    def test_call_builds_json_rpc_and_surfaces_errors(self):
        api = CosmosApi("https://cosmos", "SCOPE1", "pw")
        api.session = MagicMock()
        api.session.post.return_value = FakeResponse(payload={"jsonrpc": "2.0", "result": ["A", "B"], "id": 1})
        assert api.call("get_all_tlm_names", "FPRIME") == ["A", "B"]
        body = json.loads(api.session.post.call_args.kwargs["data"])
        assert body["method"] == "get_all_tlm_names" and body["params"] == ["FPRIME"]
        assert body["keyword_params"] == {"scope": "SCOPE1"}
        assert api.session.post.call_args.kwargs["headers"]["Content-Type"] == "application/json-rpc"
        assert api.cable_url() == "wss://cosmos/openc3-api/cable?scope=SCOPE1"
        api.session.post.return_value = FakeResponse(
            status=500, payload={"error": {"code": -1, "message": "x", "data": {"message": "no such cmd"}}}
        )
        with pytest.raises(CosmosTransportError, match="no such cmd"):
            api.call("cmd", "FPRIME", "NOPE", {})
        api.session.post.side_effect = requests.ConnectionError("refused")
        with pytest.raises(CosmosTransportError, match="Cannot reach COSMOS"):
            api.call("cmd", "FPRIME", "NOPE", {})


class FakeSocket:
    """Scripted ActionCable server: confirms the subscription then plays frames"""

    def __init__(self, frames, reject=False):
        self.sent = []
        self.frames = list(frames)
        self.reject = reject
        self.closed = threading.Event()

    def send(self, text):
        self.sent.append(json.loads(text))
        if self.sent[-1]["command"] == "subscribe":
            kind = "reject_subscription" if self.reject else "confirm_subscription"
            self.frames.insert(0, json.dumps({"type": kind}))
            self.frames.insert(0, json.dumps({"type": "welcome"}))

    def recv(self):
        import websocket

        if self.frames:
            return self.frames.pop(0)
        if self.closed.is_set():
            raise websocket.WebSocketConnectionClosedException()
        raise websocket.WebSocketTimeoutException()

    def close(self):
        self.closed.set()


class TestStreamingSocket:
    def test_subscribe_add_and_deliver(self):
        packet = {"__packet": "FPRIME__X", "A": 1}
        fake = FakeSocket([json.dumps({"type": "ping"}), json.dumps({"identifier": "i", "message": [packet]})])
        delivered_packets = []
        got = threading.Event()

        def callback(p):
            delivered_packets.append(p)
            got.set()

        stream = StreamingSocket(
            "ws://c/openc3-api/cable?scope=DEFAULT",
            lambda: "tok",
            "DEFAULT",
            ["DECOM__TLM__FPRIME__X__CONVERTED"],
            callback,
        )
        with patch("websocket.create_connection", return_value=fake) as create:
            stream.start()
            assert got.wait(5)
            stream.stop()
        assert delivered_packets == [packet]
        assert create.call_args.args[0] == "ws://c/openc3-api/cable?scope=DEFAULT"
        assert (
            "Sec-WebSocket-Protocol: actioncable-v1-json, actioncable-unsupported" in create.call_args.kwargs["header"]
        )
        subscribe, add = fake.sent
        assert subscribe == {
            "command": "subscribe",
            "identifier": json.dumps({"channel": "StreamingChannel", "token": "tok"}),
        }
        assert add["command"] == "message" and add["identifier"] == subscribe["identifier"]
        assert json.loads(add["data"]) == {
            "action": "add",
            "packets": ["DECOM__TLM__FPRIME__X__CONVERTED"],
            "scope": "DEFAULT",
            "token": "tok",
        }
        assert fake.closed.is_set()

    def test_callback_error_drops_the_packet_but_keeps_the_subscription(self, caplog):
        frames = [
            json.dumps({"identifier": "i", "message": [{"__packet": "P1", "bad": True}]}),
            json.dumps({"identifier": "i", "message": [{"__packet": "P2"}]}),
        ]
        fake = FakeSocket(frames)
        got = threading.Event()
        seen = []

        def callback(packet):
            seen.append(packet["__packet"])
            if packet.get("bad"):
                raise KeyError("boom")
            got.set()

        stream = StreamingSocket("ws://c", lambda: "tok", "DEFAULT", [], callback)
        with patch("websocket.create_connection", return_value=fake) as create, caplog.at_level(logging.WARNING):
            stream.start()
            assert got.wait(5)
            stream.stop()
        assert seen == ["P1", "P2"] and create.call_count == 1
        assert "Dropped COSMOS packet P1" in caplog.text

    def test_rejected_subscription_closes_the_socket_and_retries_with_a_fresh_token(self, caplog):
        sockets = []

        def create(*_args, **_kwargs):
            sockets.append(FakeSocket([], reject=len(sockets) < 2))
            return sockets[-1]

        tokens = iter(["old", "new", "newer"])
        stream = StreamingSocket("ws://c", lambda: next(tokens), "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            deadline = threading.Event()
            for _ in range(100):
                if len(sockets) >= 3 and len(sockets[2].sent) == 2:
                    break
                deadline.wait(0.02)
            stream.stop()
        assert "rejected the streaming subscription" in caplog.text
        assert sockets[0].closed.is_set() and sockets[1].closed.is_set()
        used = [json.loads(s.sent[0]["identifier"])["token"] for s in sockets[:3]]
        assert used == ["old", "new", "newer"]
        assert "old" not in caplog.text and "new" not in caplog.text


class TestStreamingHandshakeFailures:
    @pytest.mark.parametrize(
        "first_frames, expected",
        [
            ([json.dumps({"type": "disconnect", "reason": "unauthorized"})], "closed the stream: unauthorized"),
            ([], "did not confirm"),
            (["not json", json.dumps({"type": "confirm_subscription"})], None),
        ],
    )
    def test_subscribe_outcomes(self, first_frames, expected, caplog):
        class HandshakeSocket(FakeSocket):
            def send(self, text):
                self.sent.append(json.loads(text))
                if self.sent[-1]["command"] == "subscribe":
                    self.frames = list(first_frames) + self.frames

        sockets = []

        def create(*_args, **_kwargs):
            sockets.append(HandshakeSocket([json.dumps({"identifier": "i", "message": [{"__packet": "P"}]})]))
            return sockets[-1]

        got = threading.Event()
        stream = StreamingSocket("ws://c", lambda: "tok", "DEFAULT", [], lambda p: got.set())
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            patch.object(transport, "SUBSCRIBE_TIMEOUT_SECONDS", 0.05),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            if expected is None:
                assert got.wait(5)
            else:
                for _ in range(200):
                    if len(sockets) >= 2:
                        break
                    threading.Event().wait(0.01)
            stream.stop()
        if expected is None:
            assert len(sockets) == 1 and len(sockets[0].sent) == 2  # junk frame skipped, subscription kept
        else:
            assert expected in caplog.text and len(sockets) >= 2 and sockets[0].closed.is_set()


class TestConnectLifecycle:
    def test_frames_reach_the_channel_decoder_and_secrets_stay_out_of_the_log(self, client, monkeypatch, caplog):
        monkeypatch.setenv(PASSWORD_ENV, SECRET)
        frame = {"identifier": "i", "message": [channel_packet("CdhCore.cmdDisp.CommandErrors", {"COMMANDERRORS": 9})]}
        fake = FakeSocket([json.dumps(frame)])
        api = CosmosApi.__new__(CosmosApi)
        got = threading.Event()
        client.channel_decoder.send_to_all.side_effect = lambda _ch: got.set()

        def build_api(url, scope, password):
            CosmosApi.__init__(api, url, scope, password)
            api.session = MagicMock()
            api.session.headers = {}
            api.session.get.return_value = FakeResponse(payload={"result": True})
            api.session.post.side_effect = [
                FakeResponse(text="t0k3n-S3ntinel"),
                FakeResponse(payload={"result": ["CDHCORE.CMDDISP.COMMANDERRORS"]}),
            ]
            return api

        with (
            patch.object(transport, "CosmosApi", side_effect=build_api),
            patch("websocket.create_connection", return_value=fake) as create,
            caplog.at_level(logging.DEBUG, logger="cosmos_transport"),
        ):
            client.connect("cosmos://cosmos.example:2900")
            assert got.wait(5)
            client.disconnect()
        assert api.url == "http://cosmos.example:2900"
        assert create.call_args.args[0] == "ws://cosmos.example:2900/openc3-api/cable?scope=DEFAULT"
        (ch,) = delivered(client)
        assert ch.get_template().get_full_name() == "CdhCore.cmdDisp.CommandErrors" and ch.get_val() == 9
        assert json.loads(fake.sent[0]["identifier"])["token"] == "t0k3n-S3ntinel"
        assert SECRET not in caplog.text and "t0k3n-S3ntinel" not in caplog.text
        assert "clear text" in caplog.text  # remote http host

    def test_connect_authenticates_subscribes_dictionary_packets_and_disconnect_closes(self, client, monkeypatch):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        fake_api = MagicMock()
        fake_api.call.return_value = [
            "CDHCORE.CMDDISP.COMMANDSDISPATCHED",
            "CDHCORE.CMDDISP.COMMANDERRORS",
            "FPRIME_CHANNELS",
            "FPRIME_UNKNOWN",
            "SOMETHING_ELSE",
        ]
        fake_api.cable_url.return_value = "ws://cosmos:2900/openc3-api/cable?scope=DEFAULT"
        fake_stream = MagicMock()
        with (
            patch.object(transport, "CosmosApi", return_value=fake_api) as api_cls,
            patch.object(transport, "StreamingSocket", return_value=fake_stream) as stream_cls,
        ):
            client.connect()
        assert api_cls.call_args.args == ("http://cosmos:2900", "DEFAULT", "pw")
        fake_api.authenticate.assert_called_once_with()
        fake_api.call.assert_called_once_with("get_all_tlm_names", "FPRIME")
        args = stream_cls.call_args.args
        assert args[0] == "ws://cosmos:2900/openc3-api/cable?scope=DEFAULT"
        assert args[1] is fake_api.authenticate and args[2] == "DEFAULT"
        assert stream_cls.call_args.kwargs == {"token": fake_api.token}
        assert args[3] == [
            "DECOM__TLM__FPRIME__CDHCORE.CMDDISP.COMMANDSDISPATCHED__CONVERTED",
            "DECOM__TLM__FPRIME__CDHCORE.CMDDISP.COMMANDERRORS__CONVERTED",
        ]
        fake_stream.start.assert_called_once_with()
        client.disconnect()
        fake_stream.stop.assert_called_once_with()
        fake_api.session.close.assert_called_once_with()
        assert client.stream is None and client.api is None

    def test_cfs_layout_subscribes_to_every_target_packet(self, dictionaries, monkeypatch):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        client = CosmosTransport(cosmos_url="http://c", cosmos_target="FPRIME_APP", cosmos_layout="cfs")
        client.set_pipeline_references(dictionaries, MagicMock(), None)
        fake_api = MagicMock()
        fake_api.call.return_value = ["HEALTH", "CFSCORE.CMDDISP.COMMANDSDISPATCHED"]
        with (
            patch.object(transport, "CosmosApi", return_value=fake_api),
            patch.object(transport, "StreamingSocket") as stream_cls,
        ):
            client.connect()
        assert stream_cls.call_args.args[3] == [
            "DECOM__TLM__FPRIME_APP__HEALTH__CONVERTED",
            "DECOM__TLM__FPRIME_APP__CFSCORE.CMDDISP.COMMANDSDISPATCHED__CONVERTED",
        ]

    def test_target_without_packets_fails_to_connect(self, client, monkeypatch):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        fake_api = MagicMock()
        fake_api.call.return_value = []
        with (
            patch.object(transport, "CosmosApi", return_value=fake_api),
            patch.object(transport, "StreamingSocket") as stream_cls,
            pytest.raises(CosmosTransportError, match="no telemetry packets"),
        ):
            client.connect()
        stream_cls.assert_not_called()

    def test_no_matching_packet_falls_back_to_all_with_a_warning(self, client, monkeypatch, caplog):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        fake_api = MagicMock()
        fake_api.call.return_value = ["OTHER_A", "OTHER_B"]
        with (
            patch.object(transport, "CosmosApi", return_value=fake_api),
            patch.object(transport, "StreamingSocket") as stream_cls,
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            client.connect()
        assert stream_cls.call_args.args[3] == [
            "DECOM__TLM__FPRIME__OTHER_A__CONVERTED",
            "DECOM__TLM__FPRIME__OTHER_B__CONVERTED",
        ]
        assert "matches a dictionary channel" in caplog.text

    def test_plain_http_to_a_remote_host_warns(self, dictionaries, monkeypatch, caplog):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        fake_api = MagicMock()
        fake_api.call.return_value = ["X"]
        for url, expected in [
            ("http://cosmos.example:2900", True),
            ("http://localhost:2900", False),
            ("https://x", False),
        ]:
            caplog.clear()
            client = CosmosTransport(cosmos_url=url, cosmos_layout="cfs")
            client.set_pipeline_references(dictionaries, None, None)
            with (
                patch.object(transport, "CosmosApi", return_value=fake_api),
                patch.object(transport, "StreamingSocket"),
                caplog.at_level(logging.WARNING, logger="cosmos_transport"),
            ):
                client.connect()
            assert ("clear text" in caplog.text) is expected, url

    def test_connect_without_password_fails_clearly(self, client, monkeypatch):
        monkeypatch.delenv(PASSWORD_ENV, raising=False)
        with pytest.raises(CosmosTransportError, match=PASSWORD_ENV):
            client.connect()


def test_reference_dictionary_has_the_names_the_tests_use(dictionaries):
    assert {"CdhCore.cmdDisp.CMD_NO_OP_STRING", "CdhCore.cmdDisp.CMD_TEST_CMD_1"} <= set(dictionaries.command_name)
    assert "CdhCore.version.CustomVersion10" in dictionaries.channel_name
