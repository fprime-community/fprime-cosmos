"""fprime-gds ``data-transport`` plugin that talks to OpenC3 COSMOS instead of an F Prime comm layer

Architecture: deployment -> COSMOS (its own interface / the fprime-cosmos bridge) -> this transport -> GDS pipeline.
Commands are issued through the COSMOS JSON-RPC API with ``cmd_no_hazardous_check`` (structured parameters; the
hazardous-command confirmation step is bypassed because nobody is there to confirm it). Telemetry is received through
the cmd-tlm-api streaming websocket (ActionCable ``StreamingChannel``, DECOM CONVERTED values) and rebuilt into GDS
channel data: a packet carrying the generator's ``FPRIME_CHANNEL_ID`` item is resolved by channel id, any other packet
by the channel names its items spell (``CosmosNaming``), and the value is reassembled from the items exactly as
``fprime_cosmos.items.flatten`` split it (primitives, enums, strings, arrays and structs). Events, files and data
products are not delivered: the generator does not model them yet and this client maps telemetry channels only.

The COSMOS password is read from ``OPENC3_API_PASSWORD`` only; it never appears on a command line, in a URI, in a log
line or in an exception message.
"""

from __future__ import annotations

import contextlib
import datetime
import json
import logging
import os
import threading
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import requests
from fprime_gds.common.data_types.ch_data import ChData
from fprime_gds.common.data_types.cmd_data import CmdData
from fprime_gds.common.models.serialize.array_type import ArrayType
from fprime_gds.common.models.serialize.bool_type import BoolType
from fprime_gds.common.models.serialize.enum_type import EnumType
from fprime_gds.common.models.serialize.numerical_types import FloatType, IntegerType
from fprime_gds.common.models.serialize.serializable_type import SerializableType
from fprime_gds.common.models.serialize.string_type import StringType
from fprime_gds.common.models.serialize.time_type import TimeType
from fprime_gds.common.transport import TransportationException, TransportClient
from fprime_gds.plugin.definitions import gds_plugin_implementation

from fprime_cosmos.emit import (
    CHANNEL_ID_ITEM,
    CHANNELIZED_PACKET,
    DESCRIPTOR_ITEM,
    OPCODE_ITEM,
    PACKET_ID_ITEM,
    TIME_ITEM,
    TIME_ITEMS,
    UNKNOWN_PACKET,
    identifier,
)

LOGGER = logging.getLogger("cosmos_transport")

COSMOS_URI_SCHEME = "cosmos://"
COSMOS_SECURE_URI_SCHEME = "cosmos+https://"
DEFAULT_URL = "http://localhost:2900"
DEFAULT_SCOPE = "DEFAULT"
DEFAULT_TARGET = "FPRIME"
PASSWORD_ENV = "OPENC3_API_PASSWORD"
LAYOUTS = ("fprime", "cfs")
LOOPBACK_HOSTS = ("localhost", "127.0.0.1", "::1", "[::1]")

API_PATH = "/openc3-api/api"
AUTH_PATH = "/openc3-api/auth"
CABLE_PATH = "/openc3-api/cable"
CABLE_PROTOCOLS = "actioncable-v1-json, actioncable-unsupported"
STREAMING_CHANNEL = "StreamingChannel"
COMMAND_METHOD = "cmd_no_hazardous_check"
#: Items COSMOS derives on every packet (they never carry an F Prime channel value)
COSMOS_DERIVED_ITEMS = frozenset(
    {"RECEIVED_TIMESECONDS", "RECEIVED_TIMEFORMATTED", "RECEIVED_COUNT", "PACKET_TIMESECONDS", "PACKET_TIMEFORMATTED"}
)
NON_VALUE_ITEMS = (
    frozenset(TIME_ITEMS)
    | {CHANNEL_ID_ITEM, PACKET_ID_ITEM, DESCRIPTOR_ITEM, OPCODE_ITEM, TIME_ITEM}
    | COSMOS_DERIVED_ITEMS
)
STRING_LENGTH_SUFFIX = "_LENGTH"
RECONNECT_DELAY_SECONDS = 2.0
SOCKET_READ_TIMEOUT_SECONDS = 1.0
SUBSCRIBE_TIMEOUT_SECONDS = 10.0
TIME_BASE_WORKSTATION = "TB_WORKSTATION_TIME"
BOOL_TRUE_WORDS = ("TRUE", "1", "ON")
BOOL_FALSE_WORDS = ("FALSE", "0", "OFF")

if not hasattr(TransportClient, "register_data_transport_plugin"):
    LOGGER.warning(
        "The installed fprime-gds has no data-transport plugin category (nasa/fprime-gds#386); "
        "--data-transport-selection cosmos is unavailable"
    )


class CosmosTransportError(TransportationException):
    """Raised when COSMOS rejects a request or the stream cannot be established"""


class SubscriptionRejectedError(CosmosTransportError):
    """COSMOS refused the StreamingChannel subscription (token expired, wrong scope, ...)"""


def redact_url(url: str) -> str:
    """Drop user:password from a URL before it is logged or placed in an exception message"""
    if "://" not in url:
        return url.rsplit("@", 1)[-1]
    parts = urlsplit(url)
    if "@" not in parts.netloc:
        return url
    return urlunsplit(parts._replace(netloc=parts.netloc.rsplit("@", 1)[-1]))


class CosmosNaming:
    """Maps F Prime dictionary names onto the COSMOS packet/item/parameter names of one plugin layout

    ``fprime`` (this package's generator, ``fprime_cosmos.emit``): one telemetry packet per channel named after the
    channel's full name whose value items start at the channel leaf name (``CommandsDispatched``); packet-set packets
    and the cFS ``cfs`` layout (one-app-one-target generator) spell the full dotted name instead
    (``CfsCore.cmdDisp.CommandsDispatched`` -> ``CFSCORE.CMDDISP.COMMANDSDISPATCHED``, inside ``HEALTH`` or a
    per-channel packet). Commands are named after the dictionary full name with parameters named after the arguments
    in both layouts. COSMOS upper-cases packet, item and parameter names, so every name here is compared upper-cased.
    The ``cfs`` command form is taken from the one-app-one-target generator's documentation and is not exercised
    live by this package's tests.
    """

    def __init__(self, layout: str):
        if layout not in LAYOUTS:
            raise ValueError(f"Unknown COSMOS layout {layout!r}; expected one of {LAYOUTS}")
        self.layout = layout

    def command_name(self, full_name: str) -> str:
        return full_name

    def command_parameter(self, arg_name: str) -> str:
        return arg_name.upper()

    @staticmethod
    def channel_key(full_name: str) -> str:
        """Item-name root (upper-cased) of a channel inside a packet shared with other channels"""
        return identifier(full_name).upper()

    def channel_packet(self, template) -> str | None:
        """Packet name (upper-cased) carrying only this channel, ``None`` when the layout has no such packet"""
        return identifier(template.get_full_name()).upper() if self.layout == "fprime" else None

    def dedicated_item(self, template) -> str:
        """Item-name root (upper-cased) of the channel value inside its dedicated packet"""
        if self.layout == "fprime":
            return identifier(template.get_name()).upper()
        return self.channel_key(template.get_full_name())


class CosmosApi:
    """JSON-RPC + auth client for the COSMOS cmd-tlm-api (the pieces the transport needs)

    Distinct from ``fprime_cosmos.cosmos_api.CosmosClient`` (the plugin-install REST client, which also *sets* the
    password on a fresh COSMOS); this client only verifies an existing password.
    """

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


class StreamingSocket:
    """ActionCable client for the cmd-tlm-api StreamingChannel (DECOM packets) on a background thread

    ``token_provider`` is called for a fresh token whenever COSMOS rejects the subscription (expired token); a packet
    callback that raises is logged and skipped without dropping the subscription.
    """

    def __init__(
        self,
        cable_url: str,
        token_provider: Callable[[], str],
        scope: str,
        packets: list[str],
        callback: Callable[[dict], None],
        token: str | None = None,
    ):
        self.cable_url = cable_url
        self.token_provider = token_provider
        self.scope = scope
        self.packets = packets
        self.callback = callback
        self.token: str | None = None
        self.identifier = ""
        if token:
            self._set_token(token)
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

    def _set_token(self, token: str):
        self.token = token
        self.identifier = json.dumps({"channel": STREAMING_CHANNEL, "token": token})

    def _refresh_token(self):
        self._set_token(self.token_provider())

    def _connect(self):
        import websocket  # websocket-client; imported lazily so the plugin loads without it until selected

        if self.token is None:
            self._refresh_token()
        socket = websocket.create_connection(
            self.cable_url,
            header=[f"Sec-WebSocket-Protocol: {CABLE_PROTOCOLS}"],
            timeout=SOCKET_READ_TIMEOUT_SECONDS,
            suppress_origin=True,
        )
        try:
            socket.send(json.dumps({"command": "subscribe", "identifier": self.identifier}))
            deadline = time.monotonic() + SUBSCRIBE_TIMEOUT_SECONDS
            while time.monotonic() < deadline:
                frame = self._read_frame(socket)
                if frame is None:
                    continue
                kind = frame.get("type")
                if kind == "confirm_subscription":
                    break
                if kind == "reject_subscription":
                    raise SubscriptionRejectedError(
                        "COSMOS rejected the streaming subscription (authentication or scope?)"
                    )
                if kind == "disconnect":
                    raise CosmosTransportError(f"COSMOS closed the stream: {frame.get('reason')}")
            else:
                raise CosmosTransportError("COSMOS did not confirm the streaming subscription")
            data = {"action": "add", "packets": self.packets, "scope": self.scope, "token": self.token}
            socket.send(json.dumps({"command": "message", "identifier": self.identifier, "data": json.dumps(data)}))
        except BaseException:
            with contextlib.suppress(Exception):
                socket.close()
            raise
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

    def _deliver(self, frame: dict):
        if frame.get("type") == "disconnect":
            raise CosmosTransportError(f"COSMOS closed the stream: {frame.get('reason')}")
        message = frame.get("message")
        if not isinstance(message, list):
            return
        for packet in message:
            try:
                self.callback(packet)
            except Exception as exc:
                LOGGER.warning(
                    "Dropped COSMOS packet %s: %s", packet.get("__packet") if isinstance(packet, dict) else packet, exc
                )

    def _run(self):
        while not self._stop.is_set():
            try:
                self._socket = self._connect()
                LOGGER.info("COSMOS stream subscribed to %d packet(s)", len(self.packets))
                while not self._stop.is_set():
                    frame = self._read_frame(self._socket)
                    if frame is not None:
                        self._deliver(frame)
            except Exception as exc:
                if self._stop.is_set():
                    return
                if isinstance(exc, SubscriptionRejectedError):
                    self.token = None
                LOGGER.warning(
                    "COSMOS stream error (%s: %s); reconnecting in %ss",
                    type(exc).__name__,
                    exc,
                    RECONNECT_DELAY_SECONDS,
                )
                self._stop.wait(RECONNECT_DELAY_SECONDS)


class CosmosTransport(TransportClient):
    """``cosmos`` data transport: GDS pipeline <-> COSMOS cmd-tlm-api"""

    def __init__(
        self, cosmos_url=None, cosmos_scope=DEFAULT_SCOPE, cosmos_target=DEFAULT_TARGET, cosmos_layout="fprime"
    ):
        super().__init__()
        self.default_uri = self.get_connection_uri(cosmos_url=cosmos_url or DEFAULT_URL)
        self.scope = cosmos_scope or DEFAULT_SCOPE
        self.target = cosmos_target or DEFAULT_TARGET
        self.naming = CosmosNaming(cosmos_layout or "fprime")
        self.api: CosmosApi | None = None
        self.stream: StreamingSocket | None = None
        self.dictionaries = None
        self.channel_decoder = None
        self.event_decoder = None
        self._channel_index: dict[str, Any] = {}
        self._packet_channels: dict[str, Any] = {}
        self._failed_channels: set[str] = set()

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
        cls._check_url(cosmos_url)
        if cosmos_layout not in LAYOUTS:
            raise ValueError(f"--cosmos-layout must be one of {LAYOUTS}")
        if not cosmos_target or not cosmos_scope:
            raise ValueError("--cosmos-target and --cosmos-scope must not be empty")

    @staticmethod
    def _check_url(cosmos_url: str):
        if "://" in cosmos_url and not cosmos_url.startswith(("http://", "https://")):
            raise ValueError(f"--cosmos-url must be an http:// or https:// URL, got: {redact_url(cosmos_url)}")
        if "@" in urlsplit(cosmos_url if "://" in cosmos_url else "http://" + cosmos_url).netloc:
            raise ValueError(f"--cosmos-url must not carry credentials; set {PASSWORD_ENV} instead")

    @classmethod
    def get_connection_uri(cls, client=True, cosmos_url=DEFAULT_URL, **_):
        cls._check_url(cosmos_url)
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
    @classmethod
    def _parse_uri(cls, connection_uri: str) -> str:
        if connection_uri.startswith(COSMOS_SECURE_URI_SCHEME):
            url = "https://" + connection_uri[len(COSMOS_SECURE_URI_SCHEME) :]
        elif connection_uri.startswith(COSMOS_URI_SCHEME):
            url = "http://" + connection_uri[len(COSMOS_URI_SCHEME) :]
        else:
            url = connection_uri
        cls._check_url(url)
        return url

    def set_pipeline_references(self, dictionaries, channel_decoder, event_decoder):
        """Called by the pipeline (fprime-gds data_transport contract) so decoded ChData can be delivered

        ``event_decoder`` is kept for the day the generator models events; nothing is delivered to it yet.
        """
        self.dictionaries = dictionaries
        self.channel_decoder = channel_decoder
        self.event_decoder = event_decoder

    def connect(self, connection_uri=None, incoming_routing=None, outgoing_routing=None):
        url = self._parse_uri(self.default_uri if connection_uri is None else connection_uri)
        host = urlsplit(url).hostname or ""
        if url.startswith("http://") and host not in LOOPBACK_HOSTS:
            LOGGER.warning(
                "COSMOS URL %s is plain http: the password and session token travel in clear text", redact_url(url)
            )
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
        self.stream = StreamingSocket(
            self.api.cable_url(), self.api.authenticate, self.scope, packets, self._on_packet, token=self.api.token
        )
        self.stream.start()

    def disconnect(self):
        if self.stream is not None:
            self.stream.stop()
            self.stream = None
        if self.api is not None:
            self.api.session.close()
            self.api = None

    def send(self, data):
        LOGGER.debug("Binary packet not routed (COSMOS uses structured APIs)")

    def recv(self, timeout=None):
        return b""

    def data_callback(self, data, sender=None):
        if isinstance(data, CmdData):
            self._issue_command(data)
            return
        super().data_callback(data, sender)

    # ---- commands -------------------------------------------------------------------------------------------------
    def _issue_command(self, cmd_data: CmdData):
        """Issue the command through the COSMOS cmd API; a COSMOS rejection is raised to the caller"""
        template = cmd_data.get_template()
        name = self.naming.command_name(template.get_full_name())
        parameters = {
            self.naming.command_parameter(spec[0]): python_value(arg)
            for spec, arg in zip(template.get_args(), cmd_data.get_args(), strict=True)
        }
        if self.api is None:
            raise CosmosTransportError(f"Cannot issue {name}: not connected to COSMOS")
        try:
            self.api.call(COMMAND_METHOD, self.target, name, parameters)
        except CosmosTransportError as exc:
            LOGGER.error("Command %s %s %s failed: %s", self.target, name, parameters, exc)
            raise
        LOGGER.info("Command issued: %s %s %s", self.target, name, parameters)

    # ---- telemetry ------------------------------------------------------------------------------------------------
    def _index_channels(self):
        self._channel_index = {}
        self._packet_channels = {}
        for full_name, template in getattr(self.dictionaries, "channel_name", {}).items():
            self._channel_index[self.naming.channel_key(full_name)] = template
            packet = self.naming.channel_packet(template)
            if packet is not None:
                self._packet_channels[packet] = template

    def _subscription_packets(self) -> list[str]:
        names = self.api.call("get_all_tlm_names", self.target) or []
        if not names:
            raise CosmosTransportError(
                f"COSMOS target {self.target!r} has no telemetry packets in scope {self.scope!r}; "
                "is the plugin for this deployment installed?"
            )
        if self.naming.layout == "fprime":
            wanted = set(self._packet_channels) | {
                identifier(packet.get_name()).upper()
                for packet in (getattr(self.dictionaries, "packet", None) or {}).values()
            }
            selected = [name for name in names if name.upper() in wanted]
            if not selected:
                LOGGER.warning(
                    "No COSMOS packet of target %s matches a dictionary channel; subscribing to all %d packet(s)",
                    self.target,
                    len(names),
                )
                selected = names
            names = selected
        skipped = {CHANNELIZED_PACKET, UNKNOWN_PACKET}
        return [f"DECOM__TLM__{self.target}__{name}__CONVERTED" for name in names if name.upper() not in skipped]

    def _channels_in(self, packet_name: str | None, items: dict[str, Any]) -> list[tuple[Any, str]]:
        """(channel template, item-name root) pairs carried by one streamed packet"""
        channel_id = items.get(CHANNEL_ID_ITEM)
        if channel_id is not None:
            template = (getattr(self.dictionaries, "channel_id", None) or {}).get(int(channel_id))
            if template is None:
                LOGGER.warning("COSMOS packet %s carries unknown F Prime channel id %s", packet_name, channel_id)
                return []
            return [(template, self.naming.dedicated_item(template))]
        dedicated = self._packet_channels.get(packet_name.upper()) if packet_name else None
        if dedicated is not None:
            return [(dedicated, self.naming.dedicated_item(dedicated))]
        found: dict[str, Any] = {}
        for item_name in items:
            if item_name.startswith("__") or item_name in NON_VALUE_ITEMS:
                continue
            key = self._channel_root(item_name)
            if key is not None:
                found[key] = self._channel_index[key]
        return [(template, key) for key, template in found.items()]

    def _channel_root(self, item_name: str) -> str | None:
        """Channel key whose flattened items (``KEY``, ``KEY_LENGTH``, ``KEY.MEMBER``, ``KEY[i]``) include this item"""
        candidate = item_name
        while candidate:
            if candidate in self._channel_index:
                return candidate
            if candidate.endswith(STRING_LENGTH_SUFFIX):
                candidate = candidate[: -len(STRING_LENGTH_SUFFIX)]
                continue
            cut = max(candidate.rfind("."), candidate.rfind("["))
            if cut <= 0:
                return None
            candidate = candidate[:cut]
        return None

    def _on_packet(self, packet: dict):
        packet_name, items = split_stream_packet(packet)
        ch_time = self._packet_time(items)
        for template, root in self._channels_in(packet_name, items):
            try:
                val_obj = build_value_object(
                    assemble_value(items, root, template.get_type_obj()), template.get_type_obj()
                )
            except Exception as exc:
                if template.get_full_name() not in self._failed_channels:
                    self._failed_channels.add(template.get_full_name())
                    LOGGER.warning(
                        "Cannot rebuild channel %s from COSMOS packet %s: %s",
                        template.get_full_name(),
                        packet_name,
                        exc,
                    )
                continue
            if self.channel_decoder is not None:
                self.channel_decoder.send_to_all(ChData(val_obj, ch_time, template))

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
        except Exception as exc:
            LOGGER.debug("No usable F Prime time in packet (%s); using the receive time", exc)
        time_obj = TimeType()
        received = items.get("__time")
        with contextlib.suppress(Exception):
            if received is not None:
                stamp = datetime.datetime.fromtimestamp(int(received) / 1e9, tz=datetime.timezone.utc).replace(
                    tzinfo=None
                )
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


def coerce_scalar(value: Any, type_class: type) -> Any:
    """Python value of one COSMOS CONVERTED item for a primitive/enum/string F Prime type

    Raises ValueError when the value does not fit the type.
    """
    if issubclass(type_class, BoolType):
        if isinstance(value, str):
            word = value.strip().upper()
            if word in BOOL_TRUE_WORDS:
                return True
            if word in BOOL_FALSE_WORDS:
                return False
            raise ValueError(f"{value!r} is not a boolean")
        return bool(value)
    if issubclass(type_class, EnumType):
        if isinstance(value, str):
            if value not in type_class.ENUM_DICT:
                raise ValueError(f"{value!r} is not a {type_class.__name__} label")
            return value
        number = int(value)
        for label, number_of_label in type_class.ENUM_DICT.items():
            if number_of_label == number:
                return label
        raise ValueError(f"{value!r} is not a {type_class.__name__} value")
    if issubclass(type_class, IntegerType):
        if isinstance(value, float) and not value.is_integer():
            raise ValueError(f"{value!r} is not an integer")
        return int(value)
    if issubclass(type_class, FloatType):
        return float(value)
    if issubclass(type_class, StringType):
        return str(value)
    raise ValueError(f"Unsupported F Prime type {type_class.__name__}")


def assemble_value(items: dict[str, Any], root: str, type_class: type) -> Any:
    """Rebuild the Python value of a channel from the items ``fprime_cosmos.items.flatten`` produced under ``root``

    Structs become ``{member: value}`` (``ROOT.MEMBER`` items), arrays a list (one ``ROOT`` item holding the list for
    numeric arrays, else ``ROOT[i]`` items), strings ignore their ``ROOT_LENGTH`` companion. KeyError when an item is
    missing.
    """
    if issubclass(type_class, SerializableType):
        return {
            member: assemble_value(items, f"{root}.{member}".upper(), member_type)
            for member, member_type, _format, _description in type_class.MEMBER_LIST
        }
    if issubclass(type_class, ArrayType):
        member_type = type_class.MEMBER_TYPE
        whole = items.get(root)
        if isinstance(whole, (list, tuple)):
            return [coerce_scalar(member, member_type) for member in whole]
        return [assemble_value(items, f"{root}[{index}]", member_type) for index in range(type_class.LENGTH)]
    if root not in items or items[root] is None:
        raise KeyError(root)
    return coerce_scalar(items[root], type_class)


def build_value_object(value: Any, type_obj):
    """Fill an F Prime value object (or instantiate the dictionary's type class) from an assembled Python value"""
    if isinstance(type_obj, type):
        type_obj = type_obj()
    type_obj.val = value
    return type_obj
