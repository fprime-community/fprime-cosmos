"""Tests for the ``cosmos`` fprime-gds data-transport plugin (fake JSON-RPC + fake ActionCable stream)"""

from __future__ import annotations

import json
import logging
import math
import sys
import threading
import time
import types
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
    SubscriptionRejectedError,
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
    "FPRIME_TIME_CONTEXT": 3,
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
            ({"cosmos_scope": ""}, "must not be empty"),
        ],
    )
    def test_check_arguments_rejects_bad_input(self, kwargs, message):
        with pytest.raises(ValueError, match=message) as info:
            CosmosTransport.check_arguments(**kwargs)
        assert "pw" not in str(info.value)

    @pytest.mark.parametrize(
        "url",
        [
            f"ftp://user:{SECRET}@host",
            f"http://user:{SECRET}@host",
            f"user:{SECRET}@host",
            f"http://admin:pa/{SECRET}@host:2900",  # '/' in the password ends the RFC 3986 authority before the '@'
            f"http://admin:pa?{SECRET}@host",
            f"https://admin:{SECRET}#x@host",
        ],
    )
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
        assert redact_url("http://u:p@host:1/x") == "http://***@host:1/x"
        assert redact_url("u:p@host") == "***@host"
        assert redact_url("http://u:pa/ss?w#d@host:1/x") == "http://***@host:1/x"
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
                {"FILENAME": "/tmp/x", "IGNOREERRORS": "FALSE"},
            ),
        ]
        assert type(calls[0][3]["ARG2"]) is float

    def test_command_echo_is_swallowed_but_other_binary_packets_fail_the_caller(self, client, dictionaries):
        client.api = MagicMock()
        client.data_callback(CmdData((), dictionaries.command_name["CdhCore.cmdDisp.CMD_NO_OP"]))
        client.data_callback(b"\x01\x02")  # the encoder's echo of that command: accepted silently
        with pytest.raises(CosmosTransportError, match="not carried by the COSMOS transport"):
            client.data_callback(b"\x03")  # a file-uplink packet: the uplinker must not wait for a handshake
        assert client._pending_commands == 0

    def test_struct_and_array_arguments_are_flattened_like_the_generator(self):
        from fprime_gds.common.models.serialize.array_type import ArrayType
        from fprime_gds.common.models.serialize.numerical_types import U16Type
        from fprime_gds.common.models.serialize.serializable_type import SerializableType

        enum = EnumType.construct_type("GdsTransportTestMode", {"OFF": 0, "ON": 1})
        inner = SerializableType.construct_type(
            "GdsTransportTestInner", [("flag", BoolType, "", ""), ("mode", enum, "", "")]
        )
        numbers = ArrayType.construct_type("GdsTransportTestNums", U16Type, 2, "")
        words = ArrayType.construct_type(
            "GdsTransportTestWords", StringType.construct_type("GdsTransportTestS8", 8), 2, ""
        )
        outer = SerializableType.construct_type(
            "GdsTransportTestOuter",
            [
                ("inner", inner, "", ""),
                ("nums", numbers, "", ""),
                ("words", words, "", ""),
                ("label", StringType.construct_type("GdsTransportTestS9", 9), "", ""),
            ],
        )
        value = {"inner": {"flag": True, "mode": "ON"}, "nums": [1, 2], "words": ["a", "b"], "label": "x"}
        assert transport.command_parameters("arg", outer, value) == {
            "ARG.INNER.FLAG": "TRUE",
            "ARG.INNER.MODE": "ON",
            "ARG.NUMS": [1, 2],
            "ARG.WORDS[0]": "a",
            "ARG.WORDS[1]": "b",
            "ARG.LABEL": "x",
        }
        lines = emit.emit_command(
            MagicMock(
                get_full_name=lambda: "X.Y",
                get_description=lambda: "",
                get_op_code=lambda: 1,
                get_args=lambda: [("arg", "", outer)],
            ),
            FprimeDictionary(REFERENCE_DICTIONARY).layout,
        )
        emitted = {line.split()[1].upper() for line in lines if "_PARAMETER " in line}
        assert set(transport.command_parameters("arg", outer, value)) <= emitted
        assert emitted - set(transport.command_parameters("arg", outer, value)) == {
            "FPRIME_DESCRIPTOR", "FPRIME_OPCODE", "ARG.WORDS[0]_LENGTH", "ARG.WORDS[1]_LENGTH", "ARG.LABEL_LENGTH"
        }  # fmt: skip

    def test_struct_argument_goes_through_issue_command_flattened(self, client):
        from fprime_gds.common.models.serialize.serializable_type import SerializableType
        from fprime_gds.common.templates.cmd_template import CmdTemplate

        pair = SerializableType.construct_type(
            "GdsTransportTestPair",
            [("count", U8Type, "", ""), ("name", StringType.construct_type("GdsTransportTestS6", 6), "", "")],
        )
        template = CmdTemplate(0x77, "SET_PAIR", "Comp", [("pair", "", pair)])
        client.api = MagicMock()
        client.data_callback(CmdData(('{"count": 2, "name": "ab"}',), template))
        client.api.call.assert_called_once_with(
            "cmd_no_hazardous_check", "FPRIME", "Comp.SET_PAIR", {"PAIR.COUNT": 2, "PAIR.NAME": "ab"}
        )

    def test_every_dictionary_command_matches_the_generated_parameters(self, client, dictionaries):
        layout = FprimeDictionary(REFERENCE_DICTIONARY).layout
        for template in dictionaries.command_name.values():
            emitted = {line.split()[1].upper() for line in emit.emit_command(template, layout) if "_PARAMETER " in line}
            emitted -= {"FPRIME_DESCRIPTOR", "FPRIME_OPCODE"}
            emitted = {name for name in emitted if not name.endswith("_LENGTH")}
            sent = {}
            for arg_name, _desc, arg_type in template.get_args():
                sent.update(
                    transport.command_parameters(arg_name, arg_type, "1" if issubclass(arg_type, StringType) else 0)
                )
            assert set(sent) == emitted, template.get_full_name()

    def test_non_command_data_goes_to_the_base_transport(self, client):
        client._pending_commands = 0
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
        with pytest.raises(CosmosTransportError, match="not carried by the COSMOS transport"):
            client.data_callback(b"\x03")  # no echo follows a failed command: this packet must not be swallowed as one

    def test_command_after_the_stream_gave_up_fails_loudly(self, client, dictionaries):
        client.api = MagicMock()
        client.stream = MagicMock(failure=CosmosTransportError("rejected 3 times"))
        with pytest.raises(CosmosTransportError, match="stream gave up: rejected 3 times"):
            client.data_callback(CmdData((), dictionaries.command_name["CdhCore.cmdDisp.CMD_NO_OP"]))
        client.api.call.assert_not_called()

    def test_connect_without_pipeline_references_fails_clearly(self, monkeypatch):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        client = CosmosTransport(cosmos_url="http://cosmos:2900")
        with (
            patch.object(transport, "CosmosApi") as api_cls,
            pytest.raises(CosmosTransportError, match="set_pipeline_references"),
        ):
            client.connect()
        api_cls.return_value.authenticate.assert_not_called()
        assert client.api is None

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
        assert naming.dedicated_item(template) == "COMMANDSDISPATCHED"  # per-channel packets carry the leaf name
        assert naming.channel_key(template.get_full_name()) == "CDHCORE.CMDDISP.COMMANDSDISPATCHED"
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
        assert ch.get_time().timeBase.val == "TB_WORKSTATION_TIME" and ch.get_time().timeContext == 3

    def test_channel_id_wins_over_packet_name(self, client):
        packet = channel_packet("SOME.RENAMED.PACKET", {"FPRIME_CHANNEL_ID": 16777217, "COMMANDERRORS": 2})
        client._on_packet(packet)
        (ch,) = delivered(client)
        assert ch.get_template().get_full_name() == "CdhCore.cmdDisp.CommandErrors" and ch.get_val() == 2

    def test_unknown_channel_id_is_reported_not_guessed(self, client, caplog):
        with caplog.at_level(logging.WARNING, logger="cosmos_transport"):
            for _ in range(2):
                client._on_packet(
                    channel_packet("CdhCore.cmdDisp.CommandErrors", {"FPRIME_CHANNEL_ID": 99, "COMMANDERRORS": 1})
                )
        assert delivered(client) == [] and caplog.text.count("unknown F Prime channel id 99") == 1

    def test_index_channels_reports_collisions_and_unusable_names(self, client, dictionaries, caplog):
        template = dictionaries.channel_name["CdhCore.cmdDisp.CommandErrors"]
        client.dictionaries = MagicMock(channel_name={"Comp.chan": template, "COMP.Chan": template})
        with caplog.at_level(logging.WARNING, logger="cosmos_transport"):
            client._index_channels()
        assert "collide on COSMOS name COMP.CHAN" in caplog.text
        client.dictionaries = MagicMock(channel_name={"Comp.chan <%= 1 %>": template})
        with pytest.raises(CosmosTransportError, match="Dictionary name not usable in COSMOS"):
            client._index_channels()

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

    def test_one_unconvertible_channel_does_not_drop_the_rest_of_a_packet_set_packet(self, dictionaries, caplog):
        client = CosmosTransport(cosmos_url="http://c", cosmos_target="FPRIME_APP", cosmos_layout="cfs")
        client.set_pipeline_references(dictionaries, MagicMock(), None)
        client._index_channels()
        packet = {
            "__time": STREAM_TIME,
            "__packet": "FPRIME_APP__HEALTH",
            "CDHCORE.CMDDISP.COMMANDSDISPATCHED": 1.5,  # not an integer: cannot be rebuilt
            "CDHCORE.CMDDISP.COMMANDERRORS": 4,
        }
        with caplog.at_level(logging.WARNING, logger="cosmos_transport"):
            client._on_packet(packet)
        assert [(ch.get_template().get_full_name(), ch.get_val()) for ch in delivered(client)] == [
            ("CdhCore.cmdDisp.CommandErrors", 4)
        ]
        assert "Cannot rebuild channel CdhCore.cmdDisp.CommandsDispatched" in caplog.text

    @pytest.mark.parametrize("tz", ["UTC", "America/Los_Angeles", "Asia/Kolkata"])
    def test_missing_fprime_time_falls_back_to_the_stream_time(self, client, monkeypatch, tz):
        monkeypatch.setenv("TZ", tz)
        time.tzset()
        try:
            time_obj = client._packet_time({"__time": 1_700_000_000_500_000_000})
        finally:
            monkeypatch.undo()
            time.tzset()
        assert (time_obj.seconds, time_obj.useconds) == (1_700_000_000, 500_000)
        assert time_obj.timeBase.val == "TB_WORKSTATION_TIME"
        garbage = client._packet_time({"__time": "soon", "FPRIME_TIME_BASE": "TB_NOPE", "FPRIME_TIME_SECONDS": "x"})
        assert garbage.seconds == 0

    def test_unknown_time_base_uses_the_receive_time_and_warns_once(self, client, caplog):
        items = {
            "FPRIME_TIME_BASE": "TB_CUSTOM",
            "FPRIME_TIME_CONTEXT": 1,
            "FPRIME_TIME_SECONDS": 7,
            "FPRIME_TIME_USECONDS": 8,
            "__time": 1_700_000_000_500_000_000,
        }
        with caplog.at_level(logging.WARNING, logger="cosmos_transport"):
            first, second = client._packet_time(items), client._packet_time(items)
        # flight seconds of an unknown base are not re-labelled as workstation time: the receive time is used instead
        assert (first.seconds, first.useconds, first.timeBase.val) == (1_700_000_000, 500_000, "TB_WORKSTATION_TIME")
        assert (second.seconds, second.useconds) == (1_700_000_000, 500_000)
        assert caplog.text.count("is not in the loaded dictionary") == 1

    def test_event_record_packet_is_decoded_by_the_gds_event_decoder(self, dictionaries):
        from fprime_gds.common.decoders.event_decoder import EventDecoder
        from fprime_gds.common.models.serialize.numerical_types import U32Type

        template = next(t for t in dictionaries.event_id.values() if not t.get_args())
        decoder = EventDecoder(dictionaries.event_id)
        decoder.send_to_all = MagicMock()
        client = CosmosTransport(cosmos_url="http://c", cosmos_target="FPRIME")
        client.set_pipeline_references(dictionaries, MagicMock(), decoder)
        client._index_channels()
        time_obj = TimeType(time_base=TimeType.TimeBase("TB_WORKSTATION_TIME"), seconds=1234, useconds=5678)
        record = U32Type(template.get_id()).serialize() + time_obj.serialize()
        packet = {"__time": STREAM_TIME, "__type": "PACKET", "__packet": "DECOM__TLM__FPRIME__FPRIME_EVENTS__CONVERTED"}
        packet.update(COSMOS_DERIVED)
        packet.update({"FPRIME_DESCRIPTOR": 2, "RECORD": {"json_class": "String", "raw": list(record)}})
        client._on_packet(packet)
        client._on_packet({**packet, "RECORD": list(record)})
        events = [call.args[0] for call in decoder.send_to_all.call_args_list]
        assert [e.template.get_full_name() for e in events] == [template.get_full_name()] * 2
        assert (events[0].get_time().seconds, events[0].get_time().useconds) == (1234, 5678)
        assert client.channel_decoder.send_to_all.call_count == 0  # an event packet never becomes a channel

    def test_event_arguments_and_time_base_survive_the_record(self, dictionaries):
        from fprime_gds.common.decoders.event_decoder import EventDecoder
        from fprime_gds.common.models.serialize.numerical_types import U32Type

        template = dictionaries.event_name["CdhCore.cmdDisp.OpCodeRegistered"]  # U32, I32, I32 arguments
        decoder = EventDecoder(dictionaries.event_id)
        decoder.send_to_all = MagicMock()
        client = CosmosTransport(cosmos_url="http://c", cosmos_target="FPRIME")
        client.set_pipeline_references(dictionaries, MagicMock(), decoder)
        client._index_channels()
        time_obj = TimeType(time_base=TimeType.TimeBase("TB_PROC_TIME"), seconds=1234, useconds=5678)
        record = U32Type(template.get_id()).serialize() + time_obj.serialize()
        for value, (_name, _desc, arg_type) in zip((0x500, 3, -1), template.get_args(), strict=True):
            record += arg_type(value).serialize()
        packet = {"__time": STREAM_TIME, "__type": "PACKET", "__packet": "DECOM__TLM__FPRIME__FPRIME_EVENTS__CONVERTED"}
        packet.update(COSMOS_DERIVED)
        packet.update({"FPRIME_DESCRIPTOR": 2, "RECORD": {"json_class": "String", "raw": list(record)}})
        client._on_packet(packet)
        (event,) = [call.args[0] for call in decoder.send_to_all.call_args_list]
        assert event.template is template
        assert [arg.val for arg in event.get_args()] == [0x500, 3, -1]
        assert event.get_time().timeBase.val == "TB_PROC_TIME"
        assert (event.get_time().seconds, event.get_time().useconds) == (1234, 5678)

    def test_undecodable_event_records_warn_once(self, dictionaries, caplog):
        from fprime_gds.common.decoders.event_decoder import EventDecoder

        decoder = EventDecoder(dictionaries.event_id)
        decoder.send_to_all = MagicMock()
        client = CosmosTransport(cosmos_url="http://c", cosmos_target="FPRIME")
        client.set_pipeline_references(dictionaries, MagicMock(), decoder)
        client._index_channels()
        packet = {"__packet": "DECOM__TLM__FPRIME__FPRIME_EVENTS__CONVERTED", "FPRIME_DESCRIPTOR": 2}
        with caplog.at_level(logging.WARNING, logger="cosmos_transport"):
            for _ in range(5):
                client._on_packet({**packet, "RECORD": [0xFF, 0xFF, 0xFF, 0xFF, 1, 2, 3]})
            client._on_packet({**packet, "RECORD": "\x00\x00\x00\x01"})  # plain-text BLOCK form is accepted
        assert caplog.text.count("Cannot decode F Prime event record") <= 2
        assert decoder.send_to_all.call_count == 0

    def test_event_packet_error_paths_never_reach_the_channel_path(self, dictionaries, caplog):
        decoder = MagicMock()
        decoder.data_callback.side_effect = RuntimeError("garbled")
        client = CosmosTransport(cosmos_url="http://c", cosmos_target="FPRIME")
        client.set_pipeline_references(dictionaries, MagicMock(), decoder)
        client._index_channels()
        channel = next(iter(dictionaries.channel_id))
        packet = {
            "__packet": "DECOM__TLM__FPRIME__FPRIME_EVENTS__CONVERTED",
            "FPRIME_DESCRIPTOR": 2,
            "FPRIME_CHANNEL_ID": channel,
        }
        with caplog.at_level(logging.WARNING, logger="cosmos_transport"):
            for record in ([1, 2, 3], [1, 2, 3], 42, None, {"json_class": "String", "raw": 7}):
                client._on_packet({**packet, "RECORD": record} if record is not None else packet)
        assert decoder.data_callback.call_count == 2  # garbled records are handed over, the failure is survived
        assert caplog.text.count("Cannot decode F Prime event record") == 1
        assert caplog.text.count("no usable RECORD block") == 1
        assert client.channel_decoder.send_to_all.call_count == 0  # the channel id item on an event packet is ignored

    def test_dictionary_packet_named_like_a_plugin_packet_is_refused(self, dictionaries):
        from unittest.mock import PropertyMock

        client = CosmosTransport(cosmos_url="http://c", cosmos_target="FPRIME")
        client.set_pipeline_references(dictionaries, MagicMock(), MagicMock())
        with (
            patch.object(
                type(dictionaries), "packet_set_name", new_callable=PropertyMock, return_value={"Fprime_Events": 1}
            ),
            pytest.raises(CosmosTransportError, match="collides"),
        ):
            client._index_channels()

    def test_events_packet_is_subscribed_but_not_a_channel(self, client, monkeypatch):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        fake_api = MagicMock()
        fake_api.call.return_value = ["CDHCORE.CMDDISP.COMMANDERRORS", "FPRIME_EVENTS", "FPRIME_UNKNOWN"]
        with (
            patch.object(transport, "CosmosApi", return_value=fake_api),
            patch.object(transport, "StreamingSocket") as s,
        ):
            client.connect()
        assert "DECOM__TLM__FPRIME__FPRIME_EVENTS__CONVERTED" in s.call_args.args[3]
        assert "DECOM__TLM__FPRIME__FPRIME_UNKNOWN__CONVERTED" not in s.call_args.args[3]

    def test_null_value_is_not_delivered(self, client, caplog):
        with caplog.at_level(logging.WARNING, logger="cosmos_transport"):
            client._on_packet(
                channel_packet(
                    "CdhCore.version.ProjectVersion", {"FPRIME_CHANNEL_ID": 16789505, "PROJECTVERSION": None}
                )
            )
        assert delivered(client) == [] and "Cannot rebuild channel CdhCore.version.ProjectVersion" in caplog.text

    def test_binary_cosmos_string_is_bounded_before_decoding(self):
        short = StringType.construct_type("GdsTransportTestS4", 4)
        with pytest.raises(ValueError, match="greater than 4"):
            coerce_scalar({"json_class": "String", "raw": [65] * 5}, short)
        with pytest.raises(ValueError, match="instead of bytes"):
            coerce_scalar({"json_class": "String", "raw": 400_000_000}, short)  # bytes(n) would allocate n zero bytes
        assert coerce_scalar({"json_class": "String", "raw": [65, 66]}, short) == "AB"

    def test_binary_cosmos_string_is_decoded(self):
        assert (
            coerce_scalar({"json_class": "String", "raw": [118, 49, 255]}, StringType.construct_type("S", 40))
            == "v1\ufffd"
        )
        assert transport.stream_packet_name("DECOM__TLM__TGT__A__B__CONVERTED") == "A__B"
        assert transport.stream_packet_name("FPRIME__HEALTH") == "HEALTH"

    def test_cfs_per_channel_packet_with_leaf_item_and_dotted_fallback(self, dictionaries):
        client = CosmosTransport(cosmos_url="http://c", cosmos_target="FPRIME_APP", cosmos_layout="cfs")
        client.set_pipeline_references(dictionaries, MagicMock(), None)
        client._index_channels()
        channel_id = dictionaries.channel_name["CdhCore.cmdDisp.CommandErrors"].get_id()
        base = {"__time": STREAM_TIME, "__type": "PACKET"}
        leaf = {**base, "__packet": "DECOM__TLM__FPRIME_APP__CDHCORE.CMDDISP.COMMANDERRORS__CONVERTED"}
        leaf.update(FPRIME_TIME)
        leaf.update({"FPRIME_CHANNEL_ID": channel_id, "COMMANDERRORS": 4})
        client._on_packet(leaf)
        dotted = {**base, "__packet": "DECOM__TLM__FPRIME_APP__CDHCORE.CMDDISP.COMMANDERRORS__CONVERTED"}
        dotted.update(FPRIME_TIME)
        dotted.update({"FPRIME_CHANNEL_ID": channel_id, "CDHCORE.CMDDISP.COMMANDERRORS": 5})
        client._on_packet(dotted)
        assert [(ch.template.get_full_name(), ch.get_val()) for ch in delivered(client)] == [
            ("CdhCore.cmdDisp.CommandErrors", 4),
            ("CdhCore.cmdDisp.CommandErrors", 5),
        ]

    def test_scheme_less_connection_uri_is_parsed_like_get_connection_uri(self):
        assert CosmosTransport._parse_uri("host:2900") == "http://host:2900"
        assert CosmosTransport._parse_uri("cosmos://host:2900") == "http://host:2900"
        assert CosmosTransport._parse_uri("cosmos+https://host") == "https://host"
        assert CosmosTransport._parse_uri("https://host") == "https://host"
        assert (
            CosmosApi(CosmosTransport._parse_uri("host:2900"), "DEFAULT", "pw")
            .cable_url()
            .startswith("ws://host:2900/")
        )

    def test_mixed_case_enum_labels_match_cosmos_upper_cased_states(self):
        enum = EnumType.construct_type("GdsTransportTestColor", {"Red": 0, "StandBy": 1})
        assert coerce_scalar("RED", enum) == "Red"
        assert coerce_scalar("StandBy", enum) == "StandBy"
        assert coerce_scalar(1, enum) == "StandBy" and coerce_scalar(1.0, enum) == "StandBy"
        with pytest.raises(ValueError, match="not a GdsTransportTestColor label"):
            coerce_scalar("BLUE", enum)
        with pytest.raises(ValueError, match="not a GdsTransportTestColor value"):
            coerce_scalar(0.7, enum)  # a non-integral number must not be truncated onto a label

    def test_time_base_label_is_matched_case_insensitively(self, client, caplog):
        items = {
            "FPRIME_TIME_BASE": "tb_sc_time",
            "FPRIME_TIME_CONTEXT": 0,
            "FPRIME_TIME_SECONDS": 9,
            "FPRIME_TIME_USECONDS": 1,
        }
        with caplog.at_level(logging.WARNING, logger="cosmos_transport"):
            time_obj = client._packet_time(items)
        assert (time_obj.seconds, time_obj.timeBase.val) == (9, "TB_SC_TIME") and caplog.text == ""
        items["FPRIME_TIME_BASE"] = "TB_WORKSTATION_TIME"
        assert (client._packet_time(items).seconds, client._packet_time(items).timeBase.val) == (
            9,
            "TB_WORKSTATION_TIME",
        )

    def test_out_of_range_fprime_time_keeps_the_packet_with_the_receive_time(self, client, caplog):
        items = {
            "FPRIME_TIME_BASE": "TB_SC_TIME",
            "FPRIME_TIME_CONTEXT": 0,
            "FPRIME_TIME_SECONDS": 5,
            "FPRIME_TIME_USECONDS": 2_000_000,
        }
        with caplog.at_level(logging.WARNING, logger="cosmos_transport"):
            time_obj = client._packet_time({**items, "__time": 1_700_000_000_500_000_000})
        assert (time_obj.seconds, time_obj.useconds, time_obj.timeBase.val) == (
            1_700_000_000,
            500_000,
            "TB_WORKSTATION_TIME",
        )
        assert caplog.text == ""  # a known base with a bad value is not reported as an unknown base

    def test_non_finite_floats_arrive_in_cosmos_float_form(self):
        from fprime_gds.common.models.serialize.numerical_types import F64Type

        assert math.isnan(coerce_scalar({"json_class": "Float", "raw": "NaN"}, F64Type))
        assert coerce_scalar({"json_class": "Float", "raw": "-Infinity"}, F64Type) == -math.inf
        with pytest.raises(ValueError):
            coerce_scalar({"json_class": "Float", "raw": "NaN"}, U8Type)

    def test_coerce_scalar_types(self):
        enum = EnumType.construct_type("E", {"A": 0, "B": 1})
        assert coerce_scalar("TRUE", BoolType) is True and coerce_scalar("off", BoolType) is False
        assert coerce_scalar(0, BoolType) is False
        assert coerce_scalar("FALSE", BoolType) is False  # the generator's BOOL states are FALSE / TRUE
        assert coerce_scalar("1", BoolType) is True and coerce_scalar("ON", BoolType) is True
        assert coerce_scalar("0", BoolType) is False
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
        assert set(kwargs) == {"data", "timeout", "allow_redirects"} and kwargs["data"] == {"password": SECRET}
        assert kwargs["allow_redirects"] is False
        assert api.session.get.call_args.kwargs["allow_redirects"] is False
        assert SECRET not in api.cable_url() and SECRET not in api.url

    @pytest.mark.parametrize(
        "get, post, message, kind",
        [
            (requests.ConnectionError("refused"), None, "Cannot reach COSMOS", transport.CosmosUnreachableError),
            (FakeResponse(payload={"result": False}), None, "no password set", CosmosTransportError),
            (FakeResponse(status=503, payload={}), None, r"failed \(503\)", transport.CosmosUnreachableError),
            (
                FakeResponse(payload={"result": True}),
                requests.Timeout("slow"),
                "Cannot reach",
                transport.CosmosUnreachableError,
            ),
            (
                FakeResponse(payload={"result": True}),
                FakeResponse(status=401, text="nope"),
                r"failed \(401\)",
                CosmosTransportError,
            ),
            (
                FakeResponse(payload={"result": True}),
                FakeResponse(text=" \n"),
                "rejected the password",
                CosmosTransportError,
            ),
            (FakeResponse(status=302, text=""), None, r"failed \(302\)", CosmosTransportError),
            (
                FakeResponse(payload={"result": True}),
                FakeResponse(status=307, text=""),
                r"failed \(307\)",
                CosmosTransportError,
            ),
            (
                FakeResponse(status=200, text="<html>login</html>"),
                None,
                "did not answer as a COSMOS",
                CosmosTransportError,
            ),
        ],
    )
    def test_authenticate_failures_are_reported_without_the_password(self, get, post, message, kind):
        api = CosmosApi("http://cosmos:2900", "DEFAULT", SECRET)
        api.session = MagicMock()
        api.session.headers = {}
        api.session.get.side_effect = [get]
        api.session.post.side_effect = [post]
        with pytest.raises(CosmosTransportError, match=message) as info:
            api.authenticate()
        assert type(info.value) is kind  # unreachable vs. refused decides whether the stream charges its auth budget
        assert SECRET not in str(info.value)
        assert api.token in (None, "") and "Authorization" not in api.session.headers

    @pytest.mark.parametrize(
        "response, message",
        [
            (FakeResponse(status=200, payload={"jsonrpc": "2.0", "error": {"message": "nope"}, "id": 1}), "nope"),
            (FakeResponse(status=400, text="bad request"), "bad request"),
            (FakeResponse(status=500, payload={"error": {"message": "x", "data": "no such cmd"}}), "no such cmd"),
            (FakeResponse(status=500, payload={"error": {"message": "x", "data": None}}), "x"),
            (FakeResponse(status=200, payload=["unexpected"]), "unexpected JSON-RPC reply list"),
            (FakeResponse(status=200, payload={}), "not a JSON-RPC reply"),
            (FakeResponse(status=200, text="<html>login</html>"), "not a JSON-RPC reply"),
        ],
    )
    def test_call_raises_on_a_json_rpc_error_or_an_http_error_alone(self, response, message):
        api = CosmosApi("http://cosmos", "DEFAULT", "pw")
        api.session = MagicMock()
        api.session.post.return_value = response
        with pytest.raises(CosmosTransportError, match=message):
            api.call("cmd_no_hazardous_check", "FPRIME", "X", {})

    def test_call_reauthenticates_once_on_401(self):
        api = CosmosApi("http://cosmos", "DEFAULT", SECRET)
        api.session = MagicMock()
        api.session.headers = {}
        api.session.get.return_value = FakeResponse(payload={"result": True})
        api.session.post.side_effect = [
            FakeResponse(status=401, text="expired"),
            FakeResponse(text="tok-fresh-42"),
            FakeResponse(payload={"jsonrpc": "2.0", "result": 1, "id": 2}),
        ]
        assert api.call("get_cmd_cnt", "FPRIME", "X") == 1
        assert api.session.headers["Authorization"] == "tok-fresh-42"
        assert [call.args[0] for call in api.session.post.call_args_list] == [
            "http://cosmos/openc3-api/api",
            "http://cosmos/openc3-api/auth/verify",
            "http://cosmos/openc3-api/api",
        ]
        api.session.post.reset_mock()
        api.session.post.side_effect = [
            FakeResponse(status=401, text="expired"),
            FakeResponse(text="tok-again"),
            FakeResponse(status=401, text="still refused"),
            FakeResponse(text="tok-never-asked-for"),
            FakeResponse(status=401, text="still refused"),
        ]
        with pytest.raises(CosmosTransportError, match=r"get_cmd_cnt failed \(401\)"):
            api.call("get_cmd_cnt", "FPRIME", "X")
        assert [call.args[0].rsplit("/", 1)[-1] for call in api.session.post.call_args_list] == ["api", "verify", "api"]
        api.session.post.side_effect = [FakeResponse(status=401, text="expired"), FakeResponse(status=401, text="no")]
        with pytest.raises(CosmosTransportError, match=r"/x failed \(401\)"):  # the re-authentication itself is refused
            api.call("get_cmd_cnt", "FPRIME", "X")

    def test_call_does_not_reauthenticate_on_403_or_without_a_password(self):
        api = CosmosApi("http://cosmos", "DEFAULT", SECRET)
        api.session = MagicMock()
        api.session.post.return_value = FakeResponse(status=403, text="forbidden")
        with pytest.raises(CosmosTransportError, match=r"failed \(403\)"):
            api.call("cmd_no_hazardous_check", "FPRIME", "X", {})
        assert api.session.post.call_count == 1 and not api.session.get.called
        api = CosmosApi("http://cosmos", "DEFAULT", None)
        api.session = MagicMock()
        api.session.post.return_value = FakeResponse(status=401, text="expired")
        with pytest.raises(CosmosTransportError, match=r"failed \(401\)"):
            api.call("get_cmd_cnt", "FPRIME", "X")
        assert api.session.post.call_count == 1 and not api.session.get.called

    def test_call_unreachable_is_classified(self):
        api = CosmosApi("http://cosmos", "DEFAULT", SECRET)
        api.session = MagicMock()
        api.session.post.side_effect = requests.ConnectionError("refused")
        with pytest.raises(transport.CosmosUnreachableError, match="Cannot reach COSMOS"):
            api.call("get_cmd_cnt", "FPRIME", "X")

    def test_call_does_not_follow_redirects(self):
        api = CosmosApi("http://cosmos", "DEFAULT", "pw")
        api.session = MagicMock()
        api.session.post.return_value = FakeResponse(status=307, text="elsewhere")
        with pytest.raises(CosmosTransportError, match=r"failed \(307\)"):
            api.call("cmd_no_hazardous_check", "FPRIME", "X", {})
        assert api.session.post.call_args.kwargs["allow_redirects"] is False

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

    def settimeout(self, _seconds):
        pass

    def getstatus(self):
        return 101


@pytest.fixture
def stream_factory():
    """Builds StreamingSockets and stops them at teardown even when an assertion fails mid-test"""
    streams = []

    def build(*args, **kwargs):
        stream = StreamingSocket(*args, **kwargs)
        streams.append(stream)
        return stream

    yield build
    for stream in streams:
        stream.stop()


def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        threading.Event().wait(0.01)
    return predicate()


class TestStreamingSocket:
    def test_subscribe_add_and_deliver(self, stream_factory):
        packet = {"__packet": "FPRIME__X", "A": 1}
        fake = FakeSocket([json.dumps({"type": "ping"}), json.dumps({"identifier": "i", "message": [packet]})])
        delivered_packets = []
        got = threading.Event()

        def callback(p):
            delivered_packets.append(p)
            got.set()

        stream = stream_factory(
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
        assert create.call_args.kwargs["timeout"] == transport.CONNECT_TIMEOUT_SECONDS
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

    def test_callback_error_drops_the_packet_once_per_name_but_keeps_the_subscription(self, stream_factory, caplog):
        frames = [
            json.dumps(
                {"identifier": "i", "message": [{"__packet": "P1", "bad": True}, {"__packet": "P1", "bad": True}]}
            ),
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

        stream = stream_factory("ws://c", lambda: "tok", "DEFAULT", [], callback)
        with patch("websocket.create_connection", return_value=fake) as create, caplog.at_level(logging.WARNING):
            stream.start()
            assert got.wait(5)
            stream.stop()
        assert seen == ["P1", "P1", "P2"] and create.call_count == 1
        assert caplog.text.count("Dropped COSMOS packet P1") == 1

    def test_first_subscription_failures_raise_from_start(self, stream_factory):
        for frames, message in [
            ([json.dumps({"type": "reject_subscription"})], "rejected the streaming subscription"),
            ([json.dumps({"type": "disconnect", "reason": "unauthorized"})], "closed the stream: unauthorized"),
            ([], "did not confirm"),
        ]:
            fake = FakeSocket([], reject=False)
            fake.send = lambda text, fake=fake, frames=frames: (
                fake.sent.append(json.loads(text)) or fake.frames.extend(frames)
            )
            stream = stream_factory("ws://c", lambda: "tok-a1b2", "DEFAULT", [], lambda p: None)
            with (
                patch("websocket.create_connection", return_value=fake),
                patch.object(transport, "SUBSCRIBE_TIMEOUT_SECONDS", 0.05),
                pytest.raises(CosmosTransportError, match=message) as info,
            ):
                stream.start()
            assert fake.closed.is_set() and stream._thread is None and "tok-a1b2" not in str(info.value)

    def test_stop_during_a_pending_handshake_closes_the_socket_and_fails_start(self, stream_factory):
        class BlockingSocket(FakeSocket):
            def recv(self):
                import websocket

                if self.closed.wait(0.02):
                    raise websocket.WebSocketConnectionClosedException()
                raise websocket.WebSocketTimeoutException()

        fake = BlockingSocket([])
        fake.send = lambda text: fake.sent.append(json.loads(text))  # never confirms
        stream = stream_factory("ws://c", lambda: "tok", "DEFAULT", [], lambda p: None)
        outcome = []

        def run():
            try:
                stream.start()
            except CosmosTransportError as error:
                outcome.append(str(error))

        with patch("websocket.create_connection", return_value=fake):
            thread = threading.Thread(target=run)
            thread.start()
            assert wait_for(lambda: len(fake.sent) == 1)
            stream.stop()
            thread.join(5)
        assert outcome == ["stopped during the streaming subscription"] and fake.closed.is_set()
        assert stream._thread is None

    def test_redirected_handshake_is_refused(self, stream_factory):
        fake = FakeSocket([])
        fake.getstatus = lambda: 302
        stream = stream_factory("ws://c", lambda: "tok-r3d", "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", return_value=fake) as create,
            pytest.raises(CosmosTransportError, match="302"),
        ):
            stream.start()
        assert create.call_args.kwargs["redirect_limit"] == 0 and fake.closed.is_set() and fake.sent == []

    def test_junk_frame_during_the_handshake_is_skipped(self, stream_factory):
        fake = FakeSocket([json.dumps({"identifier": "i", "message": [{"__packet": "P"}]})])
        original_send = fake.send
        fake.send = lambda text: (original_send(text), fake.frames.insert(0, "not json"))
        got = threading.Event()
        stream = stream_factory("ws://c", lambda: "tok", "DEFAULT", [], lambda p: got.set())
        with patch("websocket.create_connection", return_value=fake):
            stream.start()
            assert got.wait(5)
            stream.stop()
        assert len(fake.sent) == 2

    def test_mid_stream_disconnect_reconnects_with_backoff_and_a_fresh_token_after_rejection(
        self, stream_factory, caplog
    ):
        sockets = []

        def create(*_args, **_kwargs):
            index = len(sockets)
            frames = [json.dumps({"type": "disconnect", "reason": "server_restart"})] if index == 0 else []
            sockets.append(FakeSocket(frames, reject=(index == 1)))
            return sockets[-1]

        tokens = iter(["tok-old-9f3", "tok-new-7c1"])
        stream = stream_factory("ws://c", lambda: next(tokens), "DEFAULT", [], lambda p: None)
        delays = []
        original_wait = stream._stop.wait
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            patch.object(stream._stop, "wait", side_effect=lambda d: delays.append(d) or original_wait(d)),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            assert wait_for(lambda: len(sockets) >= 3 and len(sockets[2].sent) == 2)
            stream.stop()
        assert delays[:2] == [0.01, 0.02]  # exponential backoff between the two failed attempts
        assert (
            "closed the stream: server_restart" in caplog.text and "rejected the streaming subscription" in caplog.text
        )
        assert sockets[0].closed.is_set() and sockets[1].closed.is_set()
        used = [json.loads(s.sent[0]["identifier"])["token"] for s in sockets[:3]]
        assert used == ["tok-old-9f3", "tok-old-9f3", "tok-new-7c1"]  # token only refreshed after the rejection
        assert "tok-old-9f3" not in caplog.text and "tok-new-7c1" not in caplog.text

    def test_rejection_after_confirmation_triggers_a_reconnect(self, stream_factory, caplog):
        sockets = []

        def create(*_args, **_kwargs):
            frames = [json.dumps({"type": "reject_subscription"})] if not sockets else []
            sockets.append(FakeSocket(frames))
            return sockets[-1]

        stream = stream_factory("ws://c", lambda: "tok", "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            assert wait_for(lambda: len(sockets) >= 2 and len(sockets[1].sent) == 2)
            stream.stop()
        assert "after it was confirmed" in caplog.text and sockets[0].closed.is_set()

    def test_error_frame_then_rejection_after_confirmation_counts_toward_give_up(self, stream_factory, caplog):
        sockets = []

        def create(*_args, **_kwargs):
            sockets.append(
                FakeSocket(
                    [
                        json.dumps({"identifier": "i", "message": {"error": "NoMethodError: scope is required"}}),
                        json.dumps({"type": "reject_subscription"}),
                    ]
                )
            )
            return sockets[-1]

        stream = stream_factory("ws://c", lambda: "tok", "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            assert wait_for(lambda: stream.failure is not None)
        assert len(sockets) == transport.MAX_AUTH_FAILURES
        assert "scope is required" in caplog.text and "giving up on telemetry" in caplog.text

    def test_non_reconnectable_disconnects_count_as_rejections(self, stream_factory, caplog):
        sockets = []

        def create(*_args, **_kwargs):
            sockets.append(
                FakeSocket([json.dumps({"type": "disconnect", "reason": "unauthorized", "reconnect": False})])
            )
            return sockets[-1]

        stream = stream_factory("ws://c", lambda: "tok", "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            assert wait_for(lambda: stream.failure is not None)
        assert len(sockets) == transport.MAX_AUTH_FAILURES and "unauthorized" in caplog.text

    def test_failed_reauthentication_counts_toward_give_up(self, stream_factory, caplog):
        sockets = []
        tokens = iter(["tok-first"])

        def provide():
            try:
                return next(tokens)
            except StopIteration:
                raise CosmosTransportError("COSMOS rejected the password (401)") from None

        def create(*_args, **_kwargs):
            sockets.append(FakeSocket([json.dumps({"type": "reject_subscription"})]))
            return sockets[-1]

        stream = stream_factory("ws://c", provide, "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            assert wait_for(lambda: stream.failure is not None)
        assert len(sockets) == 1 and "re-authentication failed" in caplog.text and "giving up" in caplog.text

    def test_unreachable_cosmos_during_reauthentication_is_not_charged_to_the_budget(self, stream_factory, caplog):
        sockets = []
        calls = []

        def provide():
            calls.append(1)
            if len(calls) == 1:
                return "tok-first"
            if len(calls) <= 1 + transport.MAX_AUTH_FAILURES:
                raise transport.CosmosUnreachableError("Cannot reach COSMOS at http://c: refused")
            return "tok-again"

        def create(*_args, **_kwargs):
            frames = [json.dumps({"type": "reject_subscription"})] if len(sockets) == 0 else []
            sockets.append(FakeSocket(frames))
            return sockets[-1]

        stream = stream_factory("ws://c", provide, "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            assert wait_for(lambda: len(sockets) >= 2 and len(sockets[1].sent) == 2)
            stream.stop()
        assert stream.failure is None and len(calls) == 2 + transport.MAX_AUTH_FAILURES
        assert "Cannot reach COSMOS" in caplog.text and "giving up" not in caplog.text

    def test_non_reconnectable_disconnect_before_confirmation_counts_as_a_rejection(self, stream_factory, caplog):
        """First socket confirms then drops; later sockets reject the subscribe with a non-reconnectable disconnect"""
        sockets = []
        tokens = []

        def create(*_args, **_kwargs):
            fake = FakeSocket([json.dumps({"type": "disconnect", "reason": "restart"})] if not sockets else [])
            if sockets:
                fake.send = lambda text, fake=fake: (
                    fake.sent.append(json.loads(text))
                    or fake.frames.extend(
                        [
                            json.dumps({"type": "welcome"}),
                            json.dumps({"type": "disconnect", "reason": "unauthorized", "reconnect": False}),
                        ]
                    )
                )
            sockets.append(fake)
            return fake

        def provide():
            tokens.append(f"tok-{len(tokens)}")
            return tokens[-1]

        stream = stream_factory("ws://c", provide, "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            assert wait_for(lambda: stream.failure is not None)
        assert isinstance(stream.failure, SubscriptionRejectedError) and "unauthorized" in str(stream.failure)
        assert len(sockets) == 1 + transport.MAX_AUTH_FAILURES
        assert len(tokens) == transport.MAX_AUTH_FAILURES  # a fresh token before every retry, none after giving up
        assert "giving up on telemetry" in caplog.text

    @pytest.mark.parametrize(
        "marker, message", [([], "complete marker"), ({"__openc3_cable__": "resubscribe"}, "resubscribe")]
    )
    def test_end_of_stream_markers_trigger_a_reconnect(self, stream_factory, caplog, marker, message):
        sockets = []

        def create(*_args, **_kwargs):
            frames = [json.dumps({"identifier": "i", "message": marker})] if not sockets else []
            sockets.append(FakeSocket(frames))
            return sockets[-1]

        stream = stream_factory("ws://c", lambda: "tok", "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            assert wait_for(lambda: len(sockets) >= 2 and len(sockets[1].sent) == 2)
            stream.stop()
        assert sockets[0].closed.is_set() and message in caplog.text

    @pytest.mark.parametrize(
        "rescue, expect_failure",
        [({"type": "ping"}, True), ({"identifier": "i", "message": [{"__packet": "P"}]}, False)],
    )
    def test_only_a_delivered_packet_resets_the_give_up_counter(self, stream_factory, rescue, expect_failure):
        """Two rejections, a socket that only pings, then a rejection must give up; a delivered packet must reset"""
        sockets = []
        reject = json.dumps({"type": "reject_subscription"})

        def create(*_args, **_kwargs):
            index = len(sockets)
            frames = [json.dumps(rescue), json.dumps({"type": "disconnect", "reason": "r"})] if index == 2 else [reject]
            sockets.append(FakeSocket(frames if index < 5 else []))
            return sockets[-1]

        stream = stream_factory("ws://c", lambda: "tok", "DEFAULT", [], lambda p: None)
        delays = []
        original_wait = stream._stop.wait
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            patch.object(stream._stop, "wait", side_effect=lambda d: delays.append(d) or original_wait(d)),
        ):
            stream.start()
            if expect_failure:
                assert wait_for(lambda: stream.failure is not None)
                assert len(sockets) == 4  # confirmed, rejected, rejected, ping-only then the third rejection
                assert delays[:3] == [0.01, 0.02, 0.04]  # the ping-only socket did not reset the backoff either
            else:
                assert wait_for(lambda: len(sockets) >= 6)
                assert stream.failure is None
                assert delays[:4] == [0.01, 0.02, 0.01, 0.02]  # the delivered packet reset the backoff
            stream.stop()

    def test_pings_keep_the_stream_alive(self, stream_factory):
        class PingingSocket(FakeSocket):
            reads = 0

            def recv(self):
                import websocket

                if self.frames or self.closed.is_set():
                    return super().recv()
                self.reads += 1
                if self.reads % 2:
                    raise websocket.WebSocketTimeoutException()  # a read timeout between pings, as in production
                threading.Event().wait(0.02)
                return json.dumps({"type": "ping"})

        sockets = []

        def create(*_args, **_kwargs):
            sockets.append(PingingSocket([]))
            return sockets[-1]

        stream = stream_factory("ws://c", lambda: "tok", "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "STREAM_IDLE_TIMEOUT_SECONDS", 0.05),
        ):
            stream.start()
            threading.Event().wait(0.3)
            stream.stop()
        assert len(sockets) == 1 and sockets[0].reads > 10

    def test_handshake_socket_errors_are_transport_errors(self, stream_factory):
        stream = stream_factory("ws://c", lambda: "tok-s0ck", "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", side_effect=ConnectionRefusedError("refused")),
            pytest.raises(transport.CosmosUnreachableError, match="Cannot open the COSMOS stream") as info,
        ):
            stream.start()
        assert "tok-s0ck" not in str(info.value)

    def test_gives_up_after_repeated_rejections(self, stream_factory, caplog):
        sockets = []

        def create(*_args, **_kwargs):
            frames = [json.dumps({"type": "disconnect", "reason": "restart"})] if not sockets else []
            sockets.append(FakeSocket(frames, reject=bool(sockets)))
            return sockets[-1]

        stream = stream_factory("ws://c", lambda: "tok", "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            assert wait_for(lambda: stream.failure is not None)
            thread = stream._thread
            assert wait_for(lambda: not thread.is_alive())
        assert len(sockets) == 1 + transport.MAX_AUTH_FAILURES
        assert "giving up on telemetry" in caplog.text

    def test_silent_socket_is_reconnected(self, stream_factory, caplog):
        sockets = []

        def create(*_args, **_kwargs):
            sockets.append(FakeSocket([]))
            return sockets[-1]

        stream = stream_factory("ws://c", lambda: "tok", "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            patch.object(transport, "STREAM_IDLE_TIMEOUT_SECONDS", 0.05),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            assert wait_for(lambda: len(sockets) >= 2)
            stream.stop()
        assert "silent for" in caplog.text and sockets[0].closed.is_set()

    @pytest.mark.parametrize(
        "case",
        [
            "handshake-disconnect",
            "handshake-disconnect-reconnect-true",
            "no-confirm",
            "non-101",
            "closed",
            "idle",
            "resubscribe",
            "complete",
        ],
    )
    def test_transient_stream_errors_are_never_charged_to_the_give_up_budget(self, stream_factory, case):
        """More than MAX_AUTH_FAILURES transient errors in a row: capped backoff, no give-up, no new token"""
        rounds = transport.MAX_AUTH_FAILURES + 2

        def unconfirmed(*frames):  # a socket whose subscribe is answered by `frames` instead of a confirmation
            fake = FakeSocket([])
            fake.send = lambda text: (fake.sent.append(json.loads(text)), fake.frames.extend(frames))
            return fake

        def transient():
            if case == "handshake-disconnect":
                return unconfirmed(json.dumps({"type": "disconnect", "reason": "server_restart"}))
            if case == "handshake-disconnect-reconnect-true":
                return unconfirmed(json.dumps({"type": "disconnect", "reason": "server_restart", "reconnect": True}))
            if case == "no-confirm":
                return unconfirmed()
            fake = FakeSocket(
                {
                    "resubscribe": [json.dumps({"identifier": "i", "message": {"__openc3_cable__": "resubscribe"}})],
                    "complete": [json.dumps({"identifier": "i", "message": []})],
                }.get(case, [])
            )
            if case == "non-101":
                fake.getstatus = lambda: 503
            if case == "closed":
                fake.closed.set()
            return fake  # "idle": confirms, then stays silent

        sockets = []

        def create(*_args, **_kwargs):
            if not sockets:  # start() succeeds, then the stream is lost
                sockets.append(FakeSocket([json.dumps({"type": "disconnect", "reason": "restart"})]))
            else:
                sockets.append(transient() if len(sockets) <= rounds else FakeSocket([json.dumps({"type": "ping"})]))
            return sockets[-1]

        tokens = []
        stream = stream_factory("ws://c", lambda: tokens.append("tok") or "tok", "DEFAULT", [], lambda p: None)
        delays = []
        original_wait = stream._stop.wait
        with (
            patch("websocket.create_connection", side_effect=create),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.001),
            patch.object(transport, "RECONNECT_DELAY_MAX_SECONDS", 0.004),
            patch.object(transport, "STREAM_IDLE_TIMEOUT_SECONDS", 0.02),
            patch.object(transport, "SUBSCRIBE_TIMEOUT_SECONDS", 0.02),
            patch.object(stream._stop, "wait", side_effect=lambda d: delays.append(d) or original_wait(d)),
        ):
            stream.start()
            assert wait_for(lambda: len(sockets) > rounds + 1 and len(sockets[rounds + 1].sent) == 2)
            stream.stop()
        assert stream.failure is None and tokens == ["tok"]  # still trying, and the password was not re-sent
        assert delays[: rounds + 1] == [0.001, 0.002, 0.004, 0.004, 0.004, 0.004][: rounds + 1]  # capped backoff


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

    def test_connect_authenticates_subscribes_dictionary_packets_and_disconnect_closes(
        self, client, monkeypatch, caplog
    ):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        caplog.set_level(logging.INFO, logger="cosmos_transport")
        fake_api = MagicMock()
        fake_api.call.return_value = [
            "CDHCORE.CMDDISP.COMMANDSDISPATCHED",
            "CDHCORE.CMDDISP.COMMANDERRORS",
            "FPRIME_CHANNELS",
            "FPRIME_UNKNOWN",
            "SOMETHING_ELSE",
            "CDH",  # a packet-set packet of the dictionary
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
            "DECOM__TLM__FPRIME__CDH__CONVERTED",
        ]
        assert "1 COSMOS packet(s) of target FPRIME are not in the dictionary" in caplog.text
        assert "SOMETHING_ELSE" in caplog.text and ", ..." not in caplog.text
        fake_stream.start.assert_called_once_with()
        client.disconnect()
        fake_stream.stop.assert_called_once_with()
        fake_api.session.close.assert_called_once_with()
        assert client.stream is None and client.api is None

    def test_long_unmatched_packet_lists_are_marked_as_truncated(self, client, monkeypatch, caplog):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        fake_api = MagicMock()
        fake_api.call.return_value = ["CDHCORE.CMDDISP.COMMANDERRORS"] + [f"EXTRA_{i:02d}" for i in range(12)]
        with (
            patch.object(transport, "CosmosApi", return_value=fake_api),
            patch.object(transport, "StreamingSocket"),
            caplog.at_level(logging.INFO, logger="cosmos_transport"),
        ):
            client.connect()
        assert "12 COSMOS packet(s)" in caplog.text and "EXTRA_09, ..." in caplog.text and "EXTRA_10" not in caplog.text

    def test_cfs_layout_subscribes_to_every_target_packet(self, dictionaries, monkeypatch):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        client = CosmosTransport(cosmos_url="http://c", cosmos_target="FPRIME_APP", cosmos_layout="cfs")
        client.set_pipeline_references(dictionaries, MagicMock(), None)
        fake_api = MagicMock()
        fake_api.call.return_value = [
            "HEALTH",
            "CFSCORE.CMDDISP.COMMANDSDISPATCHED",
            "FPRIME_CHANNELS",
            "FPRIME_UNKNOWN",
        ]
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
        assert client.api is None and client.stream is None

    def test_no_matching_packet_fails_to_connect(self, client, monkeypatch):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        fake_api = MagicMock()
        fake_api.call.return_value = ["OTHER_A", "FPRIME_CHANNELS", "FPRIME_EVENTS", "FPRIME_UNKNOWN"]
        with (
            patch.object(transport, "CosmosApi", return_value=fake_api),
            patch.object(transport, "StreamingSocket") as stream_cls,
            pytest.raises(CosmosTransportError, match="matches a dictionary channel"),
        ):
            client.connect()
        stream_cls.assert_not_called()
        assert client.api is None and client.stream is None
        fake_api.session.close.assert_called_once_with()

    def test_failed_stream_start_fails_connect_and_cleans_up(self, client, monkeypatch):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        fake_api = MagicMock()
        fake_api.call.return_value = ["CDHCORE.CMDDISP.COMMANDERRORS"]
        fake_stream = MagicMock()
        fake_stream.start.side_effect = CosmosTransportError("did not confirm")
        with (
            patch.object(transport, "CosmosApi", return_value=fake_api),
            patch.object(transport, "StreamingSocket", return_value=fake_stream),
            pytest.raises(CosmosTransportError, match="did not confirm"),
        ):
            client.connect()
        assert client.api is None and client.stream is None
        fake_stream.stop.assert_called_once_with()

    def test_plain_http_to_a_remote_host_warns(self, dictionaries, monkeypatch, caplog):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        fake_api = MagicMock()
        fake_api.call.return_value = ["X"]
        for url, expected in [
            ("http://cosmos.example:2900", True),
            ("http://localhost:2900", False),
            ("http://127.0.0.1:2900", False),
            ("http://[::1]:2900", False),
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

    @pytest.mark.parametrize(
        "module, message",
        [(None, "websocket-client is required"), (types.ModuleType("websocket"), "not websocket-client")],
    )
    def test_connect_fails_clearly_without_websocket_client(self, client, monkeypatch, module, message):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        with (
            patch.dict(sys.modules, {"websocket": module}),
            patch.object(transport, "CosmosApi") as api_cls,
            pytest.raises(CosmosTransportError, match=message),
        ):
            client.connect()
        api_cls.return_value.authenticate.assert_not_called()  # no password sent for a connect that cannot work
        assert client.api is None and client.stream is None

    def test_connect_without_password_fails_clearly(self, client, monkeypatch):
        monkeypatch.delenv(PASSWORD_ENV, raising=False)
        with pytest.raises(CosmosTransportError, match=PASSWORD_ENV):
            client.connect()
        assert client.api is None and client.stream is None


def test_reference_dictionary_has_the_names_the_tests_use(dictionaries):
    assert {"CdhCore.cmdDisp.CMD_NO_OP_STRING", "CdhCore.cmdDisp.CMD_TEST_CMD_1"} <= set(dictionaries.command_name)
    assert "CdhCore.version.CustomVersion10" in dictionaries.channel_name
