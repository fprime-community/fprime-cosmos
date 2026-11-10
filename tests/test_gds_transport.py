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
from fprime_gds.common.loaders.cmd_json_loader import CmdJsonLoader
from fprime_gds.common.models.dictionaries import Dictionaries
from fprime_gds.common.models.serialize.bool_type import BoolType
from fprime_gds.common.models.serialize.enum_type import EnumType
from fprime_gds.common.models.serialize.numerical_types import F32Type, I32Type, U8Type
from fprime_gds.common.models.serialize.string_type import StringType

from fprime_cosmos.gds import transport
from fprime_cosmos.gds.transport import (
    PASSWORD_ENV,
    CosmosApi,
    CosmosClient,
    CosmosNaming,
    CosmosTransportError,
    StreamingSocket,
    build_value_object,
    command_string,
    format_command_value,
    redact_url,
    split_stream_packet,
)
from tests.conftest import REFERENCE_DICTIONARY


@pytest.fixture
def dictionaries():
    dicts = Dictionaries()
    dicts.load_dictionaries(str(REFERENCE_DICTIONARY), None, None)
    return dicts


@pytest.fixture
def client(dictionaries):
    client = CosmosClient(cosmos_url="http://cosmos:2900", cosmos_target="FPRIME")
    client.set_pipeline_references(dictionaries, MagicMock(), MagicMock())
    return client


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
        assert [ep.value for ep in matches] == ["fprime_cosmos.gds.transport:CosmosClient"]
        assert matches[0].load() is CosmosClient

    def test_plugin_system_discovers_cosmos_when_the_category_exists(self):
        from fprime_gds.plugin.system import Plugins

        try:
            plugins = Plugins(["data_transport"]).get_plugins("data_transport")
        except Exception:  # pragma: no cover - older fprime-gds
            pytest.skip("installed fprime-gds predates the data_transport category")
        names = {plugin.plugin_class.get_name(): plugin.plugin_class for plugin in plugins}
        assert names["cosmos"] is CosmosClient

    def test_name_arguments_and_uri(self):
        assert CosmosClient.get_name() == "cosmos"
        assert CosmosClient.register_data_transport_plugin() is CosmosClient
        dests = {spec["dest"] for spec in CosmosClient.get_arguments().values()}
        assert dests == {"cosmos_url", "cosmos_scope", "cosmos_target", "cosmos_layout"}
        assert "password" not in " ".join(dests)
        assert CosmosClient.get_connection_uri(cosmos_url="http://host:2900") == "cosmos://host:2900"
        assert CosmosClient.get_connection_uri(cosmos_url="host:2900") == "cosmos://host:2900"
        assert CosmosClient.get_connection_uri(cosmos_url="https://host") == "cosmos+https://host"
        assert CosmosClient._parse_uri("cosmos+https://host") == "https://host"
        assert CosmosClient._parse_uri("cosmos://host:2900") == "http://host:2900"

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
            CosmosClient.check_arguments(**kwargs)
        assert "pw" not in str(info.value)

    def test_redact_url(self):
        assert redact_url("http://u:p@host:1/x") == "http://host:1/x"
        assert redact_url("u:p@host") == "host"
        assert redact_url("http://host") == "http://host"


class TestCommandFormatting:
    def test_values(self):
        assert format_command_value(True) == "1"
        assert format_command_value(3) == "3"
        assert format_command_value(1.5) == "1.5"
        assert format_command_value("it's") == "'it\\'s'"
        assert format_command_value([1, "a"]) == "[1, 'a']"
        assert format_command_value({"x": 1}) == "{'x': 1}"

    def test_command_string(self):
        assert command_string("FPRIME", "CdhCore.cmdDisp.CMD_NO_OP", []) == "FPRIME CdhCore.cmdDisp.CMD_NO_OP"
        assert (
            command_string("FPRIME", "CdhCore.cmdDisp.CMD_NO_OP_STRING", [("arg1", "hi there")])
            == "FPRIME CdhCore.cmdDisp.CMD_NO_OP_STRING with arg1 'hi there'"
        )

    def test_issue_command_uses_the_json_rpc_cmd_api(self, client, dictionaries):
        template = dictionaries.command_name["CdhCore.cmdDisp.CMD_NO_OP_STRING"]
        cmd = CmdData(("hello",), template)
        client.api = MagicMock()
        client.data_callback(cmd)
        client.api.call.assert_called_once_with(
            "cmd_no_hazardous_check", "FPRIME CdhCore.cmdDisp.CMD_NO_OP_STRING with arg1 'hello'"
        )
        assert client._pending_cmd is True
        client.send(b"\x00\x01")  # encoded bytes from the pipeline: swallowed, COSMOS already has the command
        assert client._pending_cmd is False
        assert client.recv() == b""

    def test_issue_command_logs_cosmos_rejection(self, client, dictionaries, caplog):
        template = dictionaries.command_name["CdhCore.cmdDisp.CMD_NO_OP"]
        client.api = MagicMock()
        client.api.call.side_effect = CosmosTransportError("boom")
        with caplog.at_level(logging.ERROR, logger="cosmos_transport"):
            client.data_callback(CmdData((), template))
        assert "Command failed" in caplog.text and "boom" in caplog.text


class TestNaming:
    def test_fprime_layout(self):
        naming = CosmosNaming("fprime", "FPRIME")
        assert naming.channel_packet("CdhCore.cmdDisp.CommandsDispatched") == "CdhCore.cmdDisp.CommandsDispatched"
        assert naming.channel_keys("CdhCore.cmdDisp.CommandsDispatched") == (
            "COMMANDSDISPATCHED",
            "CDHCORE.CMDDISP.COMMANDSDISPATCHED",
        )

    def test_cfs_layout(self):
        naming = CosmosNaming("cfs", "FPRIME_APP")
        assert naming.channel_packet("CfsCore.cmdDisp.CommandsDispatched") is None
        assert naming.channel_keys("CfsCore.cmdDisp.CommandsDispatched") == ("CFSCORE.CMDDISP.COMMANDSDISPATCHED",)
        with pytest.raises(ValueError, match="Unknown COSMOS layout"):
            CosmosNaming("yamcs", "X")


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
            "__time": 1_700_000_000_000_000_000,
            "__type": "DECOM",
            "DECOM__TLM__FPRIME__CdhCore.cmdDisp.CommandsDispatched__COMMANDSDISPATCHED__CONVERTED": 7,
            "DECOM__TLM__FPRIME__CdhCore.cmdDisp.CommandsDispatched__FPRIME_TIME_SECONDS__CONVERTED": 10,
        }
        name, items = split_stream_packet(packet)
        assert name == "CdhCore.cmdDisp.CommandsDispatched"
        assert items == {"__time": 1_700_000_000_000_000_000, "COMMANDSDISPATCHED": 7, "FPRIME_TIME_SECONDS": 10}

    def test_split_stream_packet_handles_plain_item_keys(self):
        name, items = split_stream_packet({"__packet": "FPRIME__HEALTH", "CfsCore.cmdDisp.CommandsDispatched": 1})
        assert name == "HEALTH"
        assert items == {"CFSCORE.CMDDISP.COMMANDSDISPATCHED": 1}

    def test_channel_packet_becomes_ch_data_with_fprime_time(self, client):
        client._index_channels()
        client._on_packet(
            {
                "__time": 1_700_000_000_000_000_000,
                "__packet": "FPRIME__CdhCore.cmdDisp.CommandsDispatched",
                "FPRIME_CHANNEL_ID": 1280,
                "FPRIME_TIME_BASE": "TB_WORKSTATION_TIME",
                "FPRIME_TIME_CONTEXT": 0,
                "FPRIME_TIME_SECONDS": 1234,
                "FPRIME_TIME_USECONDS": 5678,
                "COMMANDSDISPATCHED": 42,
            }
        )
        client.channel_decoder.send_to_all.assert_called_once()
        ch = client.channel_decoder.send_to_all.call_args.args[0]
        assert isinstance(ch, ChData)
        assert ch.get_template().get_full_name() == "CdhCore.cmdDisp.CommandsDispatched"
        assert ch.get_val() == 42
        assert (ch.get_time().seconds, ch.get_time().useconds) == (1234, 5678)
        assert ch.get_time().timeBase.val == "TB_WORKSTATION_TIME"

    def test_packetized_layout_maps_items_by_full_name(self, dictionaries):
        client = CosmosClient(cosmos_url="http://c", cosmos_target="FPRIME_APP", cosmos_layout="cfs")
        client.set_pipeline_references(dictionaries, MagicMock(), None)
        client._index_channels()
        client._on_packet(
            {
                "__time": 1_700_000_000_000_000_000,
                "__packet": "FPRIME_APP__HEALTH",
                "FPRIME_TIME_BASE": 2,
                "FPRIME_TIME_SECONDS": 1,
                "FPRIME_TIME_USECONDS": 2,
                "CDHCORE.CMDDISP.COMMANDSDISPATCHED": 3,
                "CDHCORE.CMDDISP.COMMANDERRORS": 4,
                "UNRELATED.ITEM": 5,
            }
        )
        received = [call.args[0] for call in client.channel_decoder.send_to_all.call_args_list]
        names = sorted(ch.get_template().get_full_name() for ch in received)
        assert names == ["CdhCore.cmdDisp.CommandErrors", "CdhCore.cmdDisp.CommandsDispatched"]

    def test_missing_fprime_time_falls_back_to_the_stream_time(self, client):
        time_obj = CosmosClient._packet_time({"__time": 1_700_000_000_500_000_000})
        assert time_obj.seconds == 1_700_000_000 and time_obj.useconds == 500_000

    def test_build_value_object_types(self):
        assert build_value_object("TRUE", BoolType()).val is True
        assert build_value_object(0, BoolType()).val is False
        assert build_value_object("7", U8Type()).val == 7
        assert build_value_object(-3, I32Type()).val == -3
        assert build_value_object("1.5", F32Type()).val == 1.5
        assert build_value_object(3, StringType.construct_type("S", 40)()).val == "3"
        enum = EnumType.construct_type("E", {"A": 0, "B": 1})
        assert build_value_object("B", enum()).val == "B"
        assert build_value_object(1, enum()).val == "B"


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
            api.call("cmd", "FPRIME NOPE")
        api.session.post.side_effect = requests.ConnectionError("refused")
        with pytest.raises(CosmosTransportError, match="Cannot reach COSMOS"):
            api.call("cmd", "FPRIME NOPE")


class FakeSocket:
    """Scripted ActionCable server: confirms the subscription then plays frames"""

    def __init__(self, frames):
        self.sent = []
        self.frames = list(frames)
        self.closed = threading.Event()

    def send(self, text):
        self.sent.append(json.loads(text))
        if self.sent[-1]["command"] == "subscribe":
            self.frames.insert(0, json.dumps({"type": "confirm_subscription"}))
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
        delivered = []
        got = threading.Event()

        def callback(p):
            delivered.append(p)
            got.set()

        stream = StreamingSocket(
            "ws://c/openc3-api/cable?scope=DEFAULT", "tok", "DEFAULT", ["DECOM__TLM__FPRIME__X__CONVERTED"], callback
        )
        with patch("websocket.create_connection", return_value=fake) as create:
            stream.start()
            assert got.wait(5)
            stream.stop()
        assert delivered == [packet]
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

    def test_rejected_subscription_is_reported_and_retried(self, caplog):
        rejected = FakeSocket([])
        rejected.frames = []
        rejected.send = lambda text: rejected.frames.append(json.dumps({"type": "reject_subscription"}))
        stream = StreamingSocket("ws://c", "tok", "DEFAULT", [], lambda p: None)
        with (
            patch("websocket.create_connection", return_value=rejected),
            patch.object(transport, "RECONNECT_DELAY_SECONDS", 0.01),
            caplog.at_level(logging.WARNING, logger="cosmos_transport"),
        ):
            stream.start()
            threading.Event().wait(0.3)
            stream.stop()
        assert "rejected the streaming subscription" in caplog.text
        assert "tok" not in caplog.text


class TestConnectLifecycle:
    def test_connect_authenticates_subscribes_all_target_packets_and_disconnect_closes(self, client, monkeypatch):
        monkeypatch.setenv(PASSWORD_ENV, "pw")
        fake_api = MagicMock()
        fake_api.token = "tok"
        fake_api.call.return_value = ["CdhCore.cmdDisp.CommandsDispatched", "FPRIME_CHANNELS"]
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
        assert stream_cls.call_args.args[:4] == (
            "ws://cosmos:2900/openc3-api/cable?scope=DEFAULT",
            "tok",
            "DEFAULT",
            [
                "DECOM__TLM__FPRIME__CdhCore.cmdDisp.CommandsDispatched__CONVERTED",
                "DECOM__TLM__FPRIME__FPRIME_CHANNELS__CONVERTED",
            ],
        )
        fake_stream.start.assert_called_once_with()
        client.disconnect()
        fake_stream.stop.assert_called_once_with()
        fake_api.session.close.assert_called_once_with()
        assert client.stream is None and client.api is None

    def test_connect_without_password_fails_clearly(self, client, monkeypatch):
        monkeypatch.delenv(PASSWORD_ENV, raising=False)
        with pytest.raises(CosmosTransportError, match=PASSWORD_ENV):
            client.connect()


def test_cmd_json_loader_smoke(dictionaries):
    """The reference dictionary used by the fixtures carries the commands the tests rely on"""
    assert isinstance(CmdJsonLoader, type)
    assert "CdhCore.cmdDisp.CMD_NO_OP_STRING" in dictionaries.command_name
