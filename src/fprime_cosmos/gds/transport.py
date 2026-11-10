"""fprime-gds ``data-transport`` plugin that talks to OpenC3 COSMOS instead of an F Prime comm layer

Architecture: deployment -> COSMOS (its own interface / the fprime-cosmos bridge) -> this transport -> GDS pipeline.
Commands are issued through the COSMOS JSON-RPC API (``cmd``); telemetry is received through the cmd-tlm-api streaming
websocket (ActionCable ``StreamingChannel``, DECOM mode) subscribed to every packet of the F Prime target and mapped
back onto F Prime channels through a ``CosmosNaming`` layout. Events are delivered once the generator models them.

The COSMOS password is read from ``OPENC3_API_PASSWORD`` only; it never appears on a command line or in a URI.
"""

from __future__ import annotations

import contextlib
import datetime
import json
import logging
import os
import threading
import time
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import requests
from fprime_gds.common.data_types.ch_data import ChData
from fprime_gds.common.data_types.cmd_data import CmdData
from fprime_gds.common.models.serialize.bool_type import BoolType
from fprime_gds.common.models.serialize.enum_type import EnumType
from fprime_gds.common.models.serialize.numerical_types import FloatType, IntegerType
from fprime_gds.common.models.serialize.string_type import StringType
from fprime_gds.common.models.serialize.time_type import TimeType
from fprime_gds.common.transport import TransportClient
from fprime_gds.plugin.definitions import gds_plugin_implementation

LOGGER = logging.getLogger("cosmos_transport")

COSMOS_URI_SCHEME = "cosmos://"
COSMOS_SECURE_URI_SCHEME = "cosmos+https://"
DEFAULT_URL = "http://localhost:2900"
DEFAULT_SCOPE = "DEFAULT"
DEFAULT_TARGET = "FPRIME"
PASSWORD_ENV = "OPENC3_API_PASSWORD"
LAYOUTS = ("fprime", "cfs")

API_PATH = "/openc3-api/api"
AUTH_PATH = "/openc3-api/auth"
CABLE_PATH = "/openc3-api/cable"
CABLE_PROTOCOLS = "actioncable-v1-json, actioncable-unsupported"
STREAMING_CHANNEL = "StreamingChannel"
TIME_ITEMS = ("FPRIME_TIME_BASE", "FPRIME_TIME_CONTEXT", "FPRIME_TIME_SECONDS", "FPRIME_TIME_USECONDS")
CHANNEL_ID_ITEM = "FPRIME_CHANNEL_ID"
PACKET_ID_ITEM = "FPRIME_PACKET_ID"
DESCRIPTOR_ITEM = "FPRIME_DESCRIPTOR"
OPCODE_ITEM = "FPRIME_OPCODE"
NON_VALUE_ITEMS = set(TIME_ITEMS) | {CHANNEL_ID_ITEM, PACKET_ID_ITEM, DESCRIPTOR_ITEM, OPCODE_ITEM, "FPRIME_TIME"}
RECONNECT_DELAY_SECONDS = 2.0
TIME_BASE_WORKSTATION = "TB_WORKSTATION_TIME"


class CosmosTransportError(RuntimeError):
    """Raised when COSMOS rejects a request or the stream cannot be established"""


def redact_url(url: str) -> str:
    """Drop user:password from a URL before it is logged or placed in an exception message"""
    if "://" not in url:
        return url.rsplit("@", 1)[-1]
    parts = urlsplit(url)
    if "@" not in parts.netloc:
        return url
    return urlunsplit(parts._replace(netloc=parts.netloc.rsplit("@", 1)[-1]))


class CosmosNaming:
    """Maps F Prime dictionary names onto COSMOS target/packet/item names for one plugin layout

    ``fprime`` (fprime-cosmos generator): command ``<T> Full.Name`` with parameters named after the arguments; one
    telemetry packet per channel named ``Full.Channel.Name`` whose value item is the channel leaf name.
    ``cfs`` (one-app-one-target cFS generator): same command form; channel items are the upper-cased dotted full name
    (``CFSCORE.CMDDISP.COMMANDSDISPATCHED``) inside packetized (``HEALTH``) or per-channel packets.
    COSMOS upper-cases item and parameter names, so lookups are case-insensitive on the item side.
    """

    def __init__(self, layout: str, target: str):
        if layout not in LAYOUTS:
            raise ValueError(f"Unknown COSMOS layout {layout!r}; expected one of {LAYOUTS}")
        self.layout = layout
        self.target = target

    def command_name(self, full_name: str) -> str:
        return full_name

    def command_parameter(self, arg_name: str) -> str:
        return arg_name

    def channel_keys(self, full_name: str) -> tuple[str, ...]:
        """Item-name keys (upper-cased) under which a channel's value may appear in a streamed packet"""
        upper = full_name.upper()
        if self.layout == "fprime":
            return (full_name.rsplit(".", 1)[-1].upper(), upper)
        return (upper,)

    def channel_packet(self, full_name: str) -> str | None:
        """Packet carrying the channel when the layout dedicates one packet per channel (``None`` = any packet)"""
        return full_name if self.layout == "fprime" else None


class CosmosApi:
    """JSON-RPC + auth client for the COSMOS cmd-tlm-api (the pieces the transport needs)"""

    def __init__(self, url: str, scope: str, password: str | None, timeout: float = 30.0):
        self.url = url.rstrip("/")
        self.scope = scope
        self.password = password
        self.timeout = timeout
        self.session = requests.Session()
        self.token: str | None = None
        self._id = 0

    def authenticate(self) -> str:
        if not self.password:
            raise CosmosTransportError(f"A COSMOS password is required: set {PASSWORD_ENV}")
        try:
            exists = self._check(self.session.get(f"{self.url}{AUTH_PATH}/token-exists", timeout=self.timeout))
            if not exists.json().get("result"):
                raise CosmosTransportError(f"COSMOS at {redact_url(self.url)} has no password set yet")
            response = self._check(
                self.session.post(
                    f"{self.url}{AUTH_PATH}/verify", data={"password": self.password}, timeout=self.timeout
                )
            )
        except requests.RequestException as error:
            raise CosmosTransportError(f"Cannot reach COSMOS at {redact_url(self.url)}: {error}") from error
        self.token = response.text.strip()
        if not self.token:
            raise CosmosTransportError(f"COSMOS at {redact_url(self.url)} rejected the password")
        self.session.headers["Authorization"] = self.token
        return self.token

    def call(self, method: str, *params: Any, **keyword_params: Any) -> Any:
        """One JSON-RPC call against /openc3-api/api; returns the result or raises CosmosTransportError"""
        self._id += 1
        body = {"jsonrpc": "2.0", "method": method, "params": list(params), "id": self._id}
        keyword_params.setdefault("scope", self.scope)
        body["keyword_params"] = keyword_params
        try:
            response = self.session.post(
                f"{self.url}{API_PATH}",
                data=json.dumps(body),
                headers={"Content-Type": "application/json-rpc"},
                timeout=self.timeout,
            )
        except requests.RequestException as error:
            raise CosmosTransportError(f"Cannot reach COSMOS at {redact_url(self.url)}: {error}") from error
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if response.status_code >= 400 or "error" in payload:
            error = payload.get("error") or {}
            message = error.get("data", {}).get("message") if isinstance(error, dict) else None
            message = message or (error.get("message") if isinstance(error, dict) else None) or response.text
            raise CosmosTransportError(f"COSMOS {method} failed ({response.status_code}): {message}")
        return payload.get("result")

    @staticmethod
    def _check(response: requests.Response) -> requests.Response:
        if response.status_code >= 400:
            raise CosmosTransportError(
                f"{response.request.method} {redact_url(response.url)} failed ({response.status_code})"
            )
        return response

    def cable_url(self) -> str:
        scheme = "wss" if self.url.startswith("https://") else "ws"
        return f"{scheme}://{self.url.split('://', 1)[1]}{CABLE_PATH}?scope={self.scope}"


def format_command_value(value: Any) -> str:
    """Render one argument value the way the COSMOS ``cmd("TGT CMD with A 1, B 'x'")`` string expects it"""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(format_command_value(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{k!r}: {format_command_value(v)}" for k, v in value.items()) + "}"
    text = str(value)
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def command_string(target: str, name: str, parameters: Iterable[tuple[str, Any]]) -> str:
    """``TARGET NAME with A 1, B 'x'`` as consumed by the COSMOS ``cmd`` API"""
    rendered = ", ".join(f"{param} {format_command_value(value)}" for param, value in parameters)
    return f"{target} {name}" + (f" with {rendered}" if rendered else "")


class StreamingSocket:
    """ActionCable client for the cmd-tlm-api StreamingChannel (DECOM packets) on a background thread"""

    def __init__(self, cable_url: str, token: str, scope: str, packets: list[str], callback):
        self.cable_url = cable_url
        self.identifier = json.dumps({"channel": STREAMING_CHANNEL, "token": token})
        self.token = token
        self.scope = scope
        self.packets = packets
        self.callback = callback
        self._socket = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="cosmos-stream", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        socket, self._socket = self._socket, None
        if socket is not None:
            with contextlib.suppress(Exception):
                socket.close()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=5)

    def _connect(self):
        import websocket  # websocket-client; imported lazily so the plugin loads without it until selected

        socket = websocket.create_connection(
            self.cable_url, header=[f"Sec-WebSocket-Protocol: {CABLE_PROTOCOLS}"], timeout=1.0, suppress_origin=True
        )
        socket.send(json.dumps({"command": "subscribe", "identifier": self.identifier}))
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            frame = self._read_frame(socket)
            if frame is None:
                continue
            kind = frame.get("type")
            if kind == "confirm_subscription":
                break
            if kind == "reject_subscription":
                raise CosmosTransportError("COSMOS rejected the streaming subscription (authentication or scope?)")
            if kind == "disconnect":
                raise CosmosTransportError(f"COSMOS closed the stream: {frame.get('reason')}")
        else:
            raise CosmosTransportError("COSMOS did not confirm the streaming subscription")
        data = {"action": "add", "packets": self.packets, "scope": self.scope, "token": self.token}
        socket.send(json.dumps({"command": "message", "identifier": self.identifier, "data": json.dumps(data)}))
        return socket

    @staticmethod
    def _read_frame(socket) -> dict | None:
        import websocket

        try:
            raw = socket.recv()
        except websocket.WebSocketTimeoutException:
            return None
        if not raw:
            return None
        try:
            frame = json.loads(raw)
        except ValueError:
            return None
        return frame if isinstance(frame, dict) else None

    def _run(self):
        while not self._stop.is_set():
            try:
                self._socket = self._connect()
                LOGGER.info("COSMOS stream subscribed to %d packet(s)", len(self.packets))
                while not self._stop.is_set():
                    frame = self._read_frame(self._socket)
                    if frame is None:
                        continue
                    if frame.get("type") in ("ping", "welcome", "confirm_subscription"):
                        continue
                    message = frame.get("message")
                    if isinstance(message, list):
                        for packet in message:
                            self.callback(packet)
            except Exception as exc:
                if self._stop.is_set():
                    return
                LOGGER.warning("COSMOS stream error (%s); reconnecting in %ss", exc, RECONNECT_DELAY_SECONDS)
                self._stop.wait(RECONNECT_DELAY_SECONDS)


class CosmosClient(TransportClient):
    """``cosmos`` data transport: GDS pipeline <-> COSMOS cmd-tlm-api"""

    def __init__(
        self, cosmos_url=None, cosmos_scope=DEFAULT_SCOPE, cosmos_target=DEFAULT_TARGET, cosmos_layout="fprime"
    ):
        super().__init__()
        self.default_uri = self.get_connection_uri(cosmos_url=cosmos_url or DEFAULT_URL)
        self.scope = cosmos_scope or DEFAULT_SCOPE
        self.target = cosmos_target or DEFAULT_TARGET
        self.naming = CosmosNaming(cosmos_layout or "fprime", self.target)
        self.api: CosmosApi | None = None
        self.stream: StreamingSocket | None = None
        self.dictionaries = None
        self.channel_decoder = None
        self.event_decoder = None
        self._pending_cmd = False
        self._channel_index: dict[str, Any] = {}
        self._packet_channels: dict[str, Any] = {}

    # ---- plugin contract -------------------------------------------------------------------------------------------
    @classmethod
    def get_name(cls):
        return "cosmos"

    @classmethod
    def get_arguments(cls):
        return {
            ("--cosmos-url",): {
                "dest": "cosmos_url",
                "type": str,
                "default": DEFAULT_URL,
                "help": f"COSMOS cmd-tlm-api URL http[s]://host[:port]; password from {PASSWORD_ENV}",
            },
            ("--cosmos-scope",): {
                "dest": "cosmos_scope",
                "type": str,
                "default": DEFAULT_SCOPE,
                "help": "COSMOS scope",
            },
            ("--cosmos-target",): {
                "dest": "cosmos_target",
                "type": str,
                "default": DEFAULT_TARGET,
                "help": "COSMOS target name of the F Prime deployment",
            },
            ("--cosmos-layout",): {
                "dest": "cosmos_layout",
                "type": str,
                "choices": list(LAYOUTS),
                "default": "fprime",
                "help": "COSMOS plugin naming layout: fprime (fprime-cosmos generator) or cfs (one-app-one-target)",
            },
        }

    @classmethod
    def check_arguments(
        cls,
        cosmos_url=DEFAULT_URL,
        cosmos_scope=DEFAULT_SCOPE,
        cosmos_target=DEFAULT_TARGET,
        cosmos_layout="fprime",
        **_,
    ):
        if "://" in cosmos_url and not cosmos_url.startswith(("http://", "https://")):
            raise ValueError(f"--cosmos-url must be an http:// or https:// URL, got: {redact_url(cosmos_url)}")
        if "@" in urlsplit(cosmos_url if "://" in cosmos_url else "http://" + cosmos_url).netloc:
            raise ValueError(f"--cosmos-url must not carry credentials; set {PASSWORD_ENV} instead")
        if cosmos_layout not in LAYOUTS:
            raise ValueError(f"--cosmos-layout must be one of {LAYOUTS}")
        if not cosmos_target or not cosmos_scope:
            raise ValueError("--cosmos-target and --cosmos-scope must not be empty")

    @classmethod
    def get_connection_uri(cls, client=True, cosmos_url=DEFAULT_URL, **_):
        cls.check_arguments(cosmos_url=cosmos_url)
        if "://" not in cosmos_url:
            cosmos_url = "http://" + cosmos_url
        if cosmos_url.startswith("https://"):
            return COSMOS_SECURE_URI_SCHEME + cosmos_url[len("https://") :]
        return COSMOS_URI_SCHEME + cosmos_url[len("http://") :]

    @classmethod
    @gds_plugin_implementation
    def register_data_transport_plugin(cls):
        return cls

    # ---- TransportClient ------------------------------------------------------------------------------------------
    @staticmethod
    def _parse_uri(connection_uri: str) -> str:
        if connection_uri.startswith(COSMOS_SECURE_URI_SCHEME):
            return "https://" + connection_uri[len(COSMOS_SECURE_URI_SCHEME) :]
        if connection_uri.startswith(COSMOS_URI_SCHEME):
            return "http://" + connection_uri[len(COSMOS_URI_SCHEME) :]
        return connection_uri

    def set_pipeline_references(self, dictionaries, channel_decoder, event_decoder):
        """Called by the pipeline (fprime-gds data_transport contract) so decoded ChData/EventData can be delivered"""
        self.dictionaries = dictionaries
        self.channel_decoder = channel_decoder
        self.event_decoder = event_decoder

    def connect(self, connection_uri=None, incoming_routing=None, outgoing_routing=None):
        url = self._parse_uri(self.default_uri if connection_uri is None else connection_uri)
        self.api = CosmosApi(url, self.scope, os.environ.get(PASSWORD_ENV))
        self.api.authenticate()
        self._index_channels()
        packets = self._subscription_packets()
        LOGGER.info(
            "Connected to COSMOS %s scope=%s target=%s layout=%s; %d packet(s)",
            redact_url(url),
            self.scope,
            self.target,
            self.naming.layout,
            len(packets),
        )
        self.stream = StreamingSocket(self.api.cable_url(), self.api.token, self.scope, packets, self._on_packet)
        self.stream.start()

    def disconnect(self):
        if self.stream is not None:
            self.stream.stop()
            self.stream = None
        if self.api is not None:
            self.api.session.close()
            self.api = None
        self._pending_cmd = False

    def send(self, data):
        if self._pending_cmd:
            self._pending_cmd = False
            return
        LOGGER.debug("Binary packet not routed (COSMOS uses structured APIs)")

    def recv(self, timeout=None):
        return b""

    def data_callback(self, data, sender=None):
        if isinstance(data, CmdData):
            self._issue_command(data)
            self._pending_cmd = True
            return
        super().data_callback(data, sender)

    # ---- commands -------------------------------------------------------------------------------------------------
    def _issue_command(self, cmd_data: CmdData):
        template = cmd_data.get_template()
        parameters = [
            (self.naming.command_parameter(spec[0]), python_value(arg))
            for spec, arg in zip(template.get_args(), cmd_data.get_args(), strict=False)
        ]
        text = command_string(self.target, self.naming.command_name(template.get_full_name()), parameters)
        try:
            self.api.call("cmd_no_hazardous_check", text)
            LOGGER.info("Command issued: %s", text)
        except CosmosTransportError as exc:
            LOGGER.error("Command failed: %s: %s", text, exc)

    # ---- telemetry ------------------------------------------------------------------------------------------------
    def _index_channels(self):
        self._channel_index = {}
        self._packet_channels = {}
        for full_name, template in getattr(self.dictionaries, "channel_name", {}).items():
            for key in self.naming.channel_keys(full_name):
                self._channel_index.setdefault(key, template)
            packet = self.naming.channel_packet(full_name)
            if packet is not None:
                self._packet_channels[packet.upper()] = template

    def _subscription_packets(self) -> list[str]:
        names = self.api.call("get_all_tlm_names", self.target)
        return [f"DECOM__TLM__{self.target}__{name}__CONVERTED" for name in names]

    def _on_packet(self, packet: dict):
        packet_name, items = split_stream_packet(packet)
        ch_time = self._packet_time(items)
        dedicated = self._packet_channels.get(packet_name.upper()) if packet_name else None
        for item_name, value in items.items():
            if item_name.startswith("__") or item_name in NON_VALUE_ITEMS or value is None:
                continue
            template = self._channel_index.get(item_name) if dedicated is None else dedicated
            if template is None:
                continue
            try:
                val_obj = build_value_object(value, template.get_type_obj())
            except Exception as exc:
                LOGGER.debug("Cannot convert %s=%r: %s", item_name, value, exc)
                continue
            if self.channel_decoder is not None:
                self.channel_decoder.send_to_all(ChData(val_obj, ch_time, template))
            if dedicated is not None:
                break

    @staticmethod
    def _packet_time(items: dict[str, Any]) -> TimeType:
        """F Prime time from the FPRIME_TIME_* items, else the COSMOS receive time (workstation time base)"""
        try:
            base = items["FPRIME_TIME_BASE"]
            base = TimeType.TimeBase(str(base)) if isinstance(base, str) else int(base)
            return TimeType(
                time_base=base,
                time_context=int(items.get("FPRIME_TIME_CONTEXT", 0)),
                seconds=int(items["FPRIME_TIME_SECONDS"]),
                useconds=int(items["FPRIME_TIME_USECONDS"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            LOGGER.debug("No F Prime time in packet (%s); using the receive time", exc)
        time_obj = TimeType()
        received = items.get("__time")
        if received is not None:
            stamp = datetime.datetime.fromtimestamp(int(received) / 1e9, tz=datetime.timezone.utc).replace(tzinfo=None)
            time_obj.set_datetime(stamp, TimeType.TimeBase(TIME_BASE_WORKSTATION))
        return time_obj


def split_stream_packet(packet: dict) -> tuple[str | None, dict[str, Any]]:
    """Split an ActionCable DECOM packet into (packet name, {ITEM: value}).

    COSMOS 7 sends ``__packet`` = ``DECOM__TLM__<TARGET>__<PACKET>__CONVERTED`` (older builds: plain packet name)
    plus the items keyed by plain item name; ``__time``/``__type`` metadata is kept only for ``__time``.
    """
    packet_name = packet.get("__packet") or packet.get("packet")
    if isinstance(packet_name, str) and "__" in packet_name:
        parts = packet_name.split("__")
        packet_name = parts[3] if len(parts) >= 5 else parts[-1]
    items: dict[str, Any] = {}
    for key, value in packet.items():
        if key.startswith("__"):
            if key == "__time":
                items[key] = value
            continue
        parts = key.split("__")
        if len(parts) >= 5:
            packet_name = packet_name or parts[3]
            key = parts[4]
        items[key.upper()] = value
    return packet_name, items


def python_value(arg) -> Any:
    """Plain Python value of a decoded F Prime command argument (enum label, bool, number, string, struct, array)"""
    if isinstance(arg, (StringType, EnumType, BoolType, IntegerType, FloatType)):
        return arg.val
    val = getattr(arg, "val", arg)
    if isinstance(val, dict):
        return {k: python_value(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [python_value(v) for v in val]
    return val


def build_value_object(value: Any, type_obj):
    """Fill an F Prime value object from a COSMOS CONVERTED value (``type_obj`` may be the dictionary's type class)"""
    if isinstance(type_obj, type):
        type_obj = type_obj()
    if isinstance(type_obj, BoolType):
        if isinstance(value, str):
            value = value.strip().upper() in ("TRUE", "1", "ON")
        type_obj.val = bool(value)
    elif isinstance(type_obj, EnumType):
        if isinstance(value, str):
            type_obj.val = value
        else:
            type_obj.val = type_obj.keys()[type_obj.values().index(int(value))]
    elif isinstance(type_obj, IntegerType):
        type_obj.val = int(value)
    elif isinstance(type_obj, FloatType):
        type_obj.val = float(value)
    elif isinstance(type_obj, StringType):
        type_obj.val = str(value)
    else:
        type_obj.val = value
    return type_obj
