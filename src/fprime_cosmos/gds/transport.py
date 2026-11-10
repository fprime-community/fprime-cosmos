"""fprime-gds ``data-transport`` plugin that talks to OpenC3 COSMOS instead of an F Prime comm layer

Architecture: deployment -> COSMOS (its own interface / the fprime-cosmos bridge) -> this transport -> GDS pipeline.
Commands are issued through the COSMOS JSON-RPC API with ``cmd_no_hazardous_check`` (structured parameters; the
hazardous-command confirmation step is bypassed because nobody is there to confirm it). Telemetry is received through
the cmd-tlm-api streaming websocket (ActionCable ``StreamingChannel``, DECOM CONVERTED values) and rebuilt into GDS
channel data: a packet carrying the generator's ``FPRIME_CHANNEL_ID`` item is resolved by channel id, a packet named
after a dictionary channel by its name, any other packet by the channel names its items spell (``CosmosNaming``), and
the value is reassembled from the items exactly as
``fprime_cosmos.items.flatten`` split it (primitives, enums, strings, arrays and structs). Events, files and data
products are not delivered: the generator does not model them yet and this transport maps telemetry channels only.

The COSMOS password is read from ``OPENC3_API_PASSWORD`` only; it never appears on a command line, in a URI, in a log
line or in an exception message.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import threading
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

import requests
from fprime_gds.common.data_types.ch_data import ChData
from fprime_gds.common.data_types.cmd_data import CmdData
from fprime_gds.common.models.serialize.array_type import ArrayType
from fprime_gds.common.models.serialize.bool_type import BoolType
from fprime_gds.common.models.serialize.enum_type import EnumType
from fprime_gds.common.models.serialize.numerical_types import FloatType, IntegerType, NumericalType
from fprime_gds.common.models.serialize.serializable_type import SerializableType
from fprime_gds.common.models.serialize.string_type import StringType
from fprime_gds.common.models.serialize.time_type import TimeType
from fprime_gds.common.transport import TransportationException, TransportClient
from fprime_gds.plugin.definitions import gds_plugin_implementation

from fprime_cosmos.dictionary import DictionaryError
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
from fprime_cosmos.items import BOOL_STATES

LOGGER = logging.getLogger("cosmos_transport")

COSMOS_URI_SCHEME = "cosmos://"
COSMOS_SECURE_URI_SCHEME = "cosmos+https://"
DEFAULT_URL = "http://localhost:2900"
DEFAULT_SCOPE = "DEFAULT"
DEFAULT_TARGET = "FPRIME"
PASSWORD_ENV = "OPENC3_API_PASSWORD"
LAYOUTS = ("fprime", "cfs")
LOOPBACK_HOSTS = ("localhost", "127.0.0.1", "::1")

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
RECONNECT_DELAY_MAX_SECONDS = 60.0
MAX_AUTH_FAILURES = 3
STREAM_JOIN_TIMEOUT_SECONDS = 5.0
#: ActionCable pings every 3 s; a socket silent for longer than this is treated as half-open and reconnected
STREAM_IDLE_TIMEOUT_SECONDS = 15.0
SOCKET_READ_TIMEOUT_SECONDS = 1.0
CONNECT_TIMEOUT_SECONDS = 10.0
SUBSCRIBE_TIMEOUT_SECONDS = 10.0
TIME_BASE_WORKSTATION = "TB_WORKSTATION_TIME"
BOOL_TRUE_WORDS = ("TRUE", "1", "ON")
BOOL_FALSE_WORDS = ("FALSE", "0", "OFF")

if not hasattr(TransportClient, "register_data_transport_plugin"):
    LOGGER.debug(
        "The installed fprime-gds has no data_transport plugin category (nasa/fprime-gds#386); "
        "--data-transport-selection cosmos is unavailable"
    )


class CosmosTransportError(TransportationException):
    """Raised when COSMOS rejects a request or the stream cannot be established"""


class CosmosUnreachableError(CosmosTransportError):
    """COSMOS could not be reached or answered a server-side error: retry, but do not charge the credential budget"""


class SubscriptionRejectedError(CosmosTransportError):
    """COSMOS refused the StreamingChannel subscription (token expired, wrong scope, ...)"""


def redact_url(url: str) -> str:
    """Drop user:password from a URL before it is logged or placed in an exception message"""
    if "@" not in url:
        return url
    scheme, separator, rest = url.partition("://") if "://" in url else ("", "", url)
    return f"{scheme}{separator}***@{rest.rsplit('@', 1)[-1]}"  # RFC 3986 userinfo may hide '/', '?' or '#'


class CosmosNaming:
    """Maps F Prime dictionary names onto the COSMOS packet/item/parameter names of one plugin layout

    Both generators build one telemetry packet per channel whose value items start at the channel leaf name
    (``COMMANDSDISPATCHED``) and spell the full dotted name (``CFSCORE.CMDDISP.COMMANDSDISPATCHED``) only inside
    packet-set packets such as ``HEALTH``. ``fprime`` (this package's generator, ``fprime_cosmos.emit``) names the
    per-channel packet after the channel's full name, so such a packet is also resolved by its name; ``cfs`` (the
    one-app-one-target cFS generator) has no per-channel packet naming the transport relies on, so its per-channel
    packets are resolved by their ``FPRIME_CHANNEL_ID`` item only and the whole target is subscribed. Commands are
    named after the dictionary full name with parameters named after the arguments in both layouts. COSMOS
    upper-cases packet, item, parameter and state names, so those are compared upper-cased here; the command name is
    sent as spelled in the dictionary and upper-cased by COSMOS itself. The ``cfs`` layout is checked against that
    generator's source by unit tests only and is not exercised live by this package's tests.
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
        """Packet name (upper-cased) the transport resolves to this channel alone; ``None`` when the layout's
        per-channel packets are recognised by their ``FPRIME_CHANNEL_ID`` item only (``cfs``)"""
        return identifier(template.get_full_name()).upper() if self.layout == "fprime" else None

    def dedicated_item(self, template) -> str:
        """Item-name root (upper-cased) of the channel value inside its dedicated packet

        Both generators build the per-channel packet with an ``emit_channel`` whose value items start at the channel
        leaf name (``COMMANDSDISPATCHED``); the full dotted name is used only inside packet-set packets.
        """
        return identifier(template.get_name()).upper()


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
        self._lock = threading.RLock()

    def authenticate(self) -> str:
        """Exchange the password for a session token (thread-safe; the stream thread refreshes it too)"""
        if not self.password:
            raise CosmosTransportError(f"A COSMOS password is required: set {PASSWORD_ENV}")
        with self._lock:
            return self._authenticate()

    def _authenticate(self) -> str:
        try:
            exists = self._check(
                self.session.get(f"{self.url}{AUTH_PATH}/token-exists", timeout=self.timeout, allow_redirects=False)
            )
            try:
                has_password = exists.json().get("result")
            except (ValueError, AttributeError) as error:
                raise CosmosTransportError(f"{redact_url(self.url)} did not answer as a COSMOS cmd-tlm-api") from error
            if not has_password:
                raise CosmosTransportError(f"COSMOS at {redact_url(self.url)} has no password set yet")
            response = self._check(
                self.session.post(
                    f"{self.url}{AUTH_PATH}/verify",
                    data={"password": self.password},
                    timeout=self.timeout,
                    allow_redirects=False,
                )
            )
        except requests.RequestException as error:
            raise CosmosUnreachableError(f"Cannot reach COSMOS at {redact_url(self.url)}: {error}") from error
        self.token = response.text.strip()
        if not self.token:
            raise CosmosTransportError(f"COSMOS at {redact_url(self.url)} rejected the password")
        self.session.headers["Authorization"] = self.token
        return self.token

    def call(self, method: str, *params: Any, **keyword_params: Any) -> Any:
        """One JSON-RPC call against /openc3-api/api; returns the result or raises CosmosTransportError

        A 401 (session token invalidated, e.g. COSMOS restarted) re-authenticates once and retries.
        """
        with self._lock:
            response = self._post(method, params, keyword_params)
            if response.status_code == 401 and self.password:  # 403 is a role problem a new login cannot cure
                LOGGER.info("COSMOS session token rejected (%s); re-authenticating", response.status_code)
                self._authenticate()
                response = self._post(method, params, keyword_params)
        return self._result(method, response)

    def _post(self, method: str, params: tuple, keyword_params: dict) -> requests.Response:
        self._id += 1
        body = {"jsonrpc": "2.0", "method": method, "params": list(params), "id": self._id}
        body["keyword_params"] = {"scope": self.scope, **keyword_params}
        try:
            return self.session.post(
                f"{self.url}{API_PATH}",
                data=json.dumps(body),
                headers={"Content-Type": "application/json-rpc"},
                timeout=self.timeout,
                allow_redirects=False,
            )
        except requests.RequestException as error:
            raise CosmosUnreachableError(f"Cannot reach COSMOS at {redact_url(self.url)}: {error}") from error

    @staticmethod
    def _result(method: str, response: requests.Response) -> Any:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {"error": {"message": f"unexpected JSON-RPC reply {type(payload).__name__}"}}
        elif "result" not in payload and "error" not in payload:
            payload = {"error": {"message": f"not a JSON-RPC reply: {response.text[:200]!r}"}}
        if response.status_code >= 300 or "error" in payload:
            error = payload.get("error")
            data = error.get("data") if isinstance(error, dict) else None
            message = data.get("message") if isinstance(data, dict) else (data if isinstance(data, str) else None)
            message = message or (error.get("message") if isinstance(error, dict) else None) or response.text
            raise CosmosTransportError(f"COSMOS {method} failed ({response.status_code}): {message}")
        return payload.get("result")

    @staticmethod
    def _check(response: requests.Response) -> requests.Response:
        """Accept 2xx only: a redirect would replay the password elsewhere, so it is a failure too"""
        if response.status_code >= 300:
            error = CosmosUnreachableError if response.status_code >= 500 else CosmosTransportError
            raise error(f"{response.request.method} {redact_url(response.url)} failed ({response.status_code})")
        return response

    def cable_url(self) -> str:
        scheme = "wss" if self.url.startswith("https://") else "ws"
        return f"{scheme}://{self.url.split('://', 1)[1]}{CABLE_PATH}?scope={self.scope}"


class StreamingSocket:
    """ActionCable client for the cmd-tlm-api StreamingChannel (DECOM packets) on a background thread

    The first subscription is made synchronously by ``start()`` (so ``connect()`` fails loudly); afterwards a lost
    socket, a ``disconnect`` frame, COSMOS's end-of-stream signals (``[]`` complete marker, ``resubscribe``) and
    ``STREAM_IDLE_TIMEOUT_SECONDS`` without a frame all reconnect with exponential backoff
    (``RECONNECT_DELAY_SECONDS`` .. ``RECONNECT_DELAY_MAX_SECONDS``). A rejected subscription (before or after
    confirmation, a ``disconnect`` with ``reconnect: false``, an ``{"error": ...}`` frame or a password the
    cmd-tlm-api refuses) clears the token so ``token_provider`` is asked for a fresh one, and after
    ``MAX_AUTH_FAILURES`` such rejections without a delivered packet in between the thread gives up (``failure`` is
    set, an ERROR is logged) rather than re-sending the password forever; an unreachable COSMOS
    (``CosmosUnreachableError``) retries without counting. A packet callback that raises is logged once per packet
    name and skipped without dropping the subscription.
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
        self.failure: Exception | None = None
        self._dropped: set[str] = set()
        self._socket = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        if token:
            self._set_token(token)

    def start(self):
        """Subscribe synchronously (a stream that cannot be established fails the caller), then read in a thread"""
        self._stop.clear()
        self.failure = None
        self._connect()
        LOGGER.info("COSMOS stream subscribed to %d packet(s)", len(self.packets))
        self._thread = threading.Thread(target=self._run, name="cosmos-stream", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._close_socket()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=STREAM_JOIN_TIMEOUT_SECONDS)
            if self._thread.is_alive():
                LOGGER.warning("COSMOS stream thread did not stop within %ss", STREAM_JOIN_TIMEOUT_SECONDS)

    def _close_socket(self):
        socket, self._socket = self._socket, None
        if socket is not None:
            with contextlib.suppress(Exception):
                socket.close()

    def _set_token(self, token: str):
        self.token = token
        self.identifier = json.dumps({"channel": STREAMING_CHANNEL, "token": token})

    def _refresh_token(self):
        try:
            self._set_token(self.token_provider())
        except CosmosUnreachableError:
            raise  # transient: retried with backoff, not charged to the credential budget
        except CosmosTransportError as error:
            # counts toward MAX_AUTH_FAILURES so a bad password is not re-sent forever
            raise SubscriptionRejectedError(f"COSMOS re-authentication failed: {error}") from None

    def _connect(self):
        import websocket  # websocket-client; imported here to keep it out of every GDS process's import path

        if self.token is None:
            self._refresh_token()
        try:
            socket = websocket.create_connection(
                self.cable_url,
                header=[f"Sec-WebSocket-Protocol: {CABLE_PROTOCOLS}"],
                timeout=CONNECT_TIMEOUT_SECONDS,
                suppress_origin=True,
                redirect_limit=0,  # a redirect would carry the session token elsewhere
            )
        except (OSError, websocket.WebSocketException) as error:
            raise CosmosUnreachableError(
                f"Cannot open the COSMOS stream at {redact_url(self.cable_url)}: {error}"
            ) from error
        self._socket = socket
        status = socket.getstatus() if callable(getattr(socket, "getstatus", None)) else 101
        if status != 101:
            self._close_socket()
            raise CosmosTransportError(f"COSMOS stream handshake answered {status} instead of 101")
        socket.settimeout(SOCKET_READ_TIMEOUT_SECONDS)  # visible to stop() while the handshake is in progress
        try:
            socket.send(json.dumps({"command": "subscribe", "identifier": self.identifier}))
            deadline = time.monotonic() + SUBSCRIBE_TIMEOUT_SECONDS
            while time.monotonic() < deadline and not self._stop.is_set():
                try:
                    frame = self._read_frame(socket)
                except CosmosTransportError:
                    if self._stop.is_set():
                        raise CosmosTransportError("stopped during the streaming subscription") from None
                    raise
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
                    error = SubscriptionRejectedError if frame.get("reconnect") is False else CosmosTransportError
                    raise error(f"COSMOS closed the stream: {frame.get('reason')}")
            else:
                if self._stop.is_set():
                    raise CosmosTransportError("stopped during the streaming subscription")  # pragma: no cover
                raise CosmosTransportError("COSMOS did not confirm the streaming subscription")
            data = {"action": "add", "packets": self.packets, "scope": self.scope, "token": self.token}
            socket.send(json.dumps({"command": "message", "identifier": self.identifier, "data": json.dumps(data)}))
        except BaseException:
            self._close_socket()
            raise
        return socket

    @staticmethod
    def _read_frame(socket) -> dict | None:
        import websocket

        try:
            raw = socket.recv()
        except websocket.WebSocketTimeoutException:
            return None
        except websocket.WebSocketConnectionClosedException:
            raise CosmosTransportError("COSMOS stream connection closed") from None
        if not raw:
            return None
        try:
            frame = json.loads(raw)
        except ValueError:
            return None
        return frame if isinstance(frame, dict) else None

    def _deliver(self, frame: dict) -> bool:
        """Handle one ActionCable frame; True when it carried telemetry packets (proof the subscription works)"""
        kind = frame.get("type")
        if kind == "disconnect":
            error = SubscriptionRejectedError if frame.get("reconnect") is False else CosmosTransportError
            raise error(f"COSMOS closed the stream: {frame.get('reason')}")
        if kind == "reject_subscription":
            raise SubscriptionRejectedError("COSMOS rejected the streaming subscription after it was confirmed")
        message = frame.get("message")
        if isinstance(message, dict) and "error" in message:
            raise SubscriptionRejectedError(f"COSMOS refused the streaming request: {message['error']}")
        if isinstance(message, dict) and message.get("__openc3_cable__") == "resubscribe":
            raise CosmosTransportError("COSMOS asked the stream to resubscribe")
        if message == []:
            raise CosmosTransportError("COSMOS ended the stream (complete marker)")
        if not isinstance(message, list):
            return False
        for packet in message:
            try:
                self.callback(packet)
            except Exception as exc:
                name = str(packet.get("__packet")) if isinstance(packet, dict) else type(packet).__name__
                if name not in self._dropped:
                    self._dropped.add(name)
                    LOGGER.warning("Dropped COSMOS packet %s (further drops of it are silent): %s", name, exc)
        return True

    def _run(self):
        delay = RECONNECT_DELAY_SECONDS
        auth_failures = 0
        while not self._stop.is_set():
            try:
                socket = self._socket
                if socket is None:
                    socket = self._connect()
                    if self._stop.is_set():
                        self._close_socket()
                        return
                    LOGGER.info("COSMOS stream re-subscribed to %d packet(s)", len(self.packets))
                last_frame = time.monotonic()
                while not self._stop.is_set():
                    frame = self._read_frame(socket)
                    if frame is not None:
                        last_frame = time.monotonic()
                        if self._deliver(frame):  # a delivered packet proves the subscription works
                            delay = RECONNECT_DELAY_SECONDS
                            auth_failures = 0
                    elif time.monotonic() - last_frame > STREAM_IDLE_TIMEOUT_SECONDS:
                        raise CosmosTransportError(
                            f"COSMOS stream silent for {STREAM_IDLE_TIMEOUT_SECONDS:.0f}s (no ping); reconnecting"
                        )
            except Exception as exc:
                self._close_socket()
                if self._stop.is_set():
                    return
                if isinstance(exc, SubscriptionRejectedError):
                    self.token = None
                    auth_failures += 1
                    if auth_failures >= MAX_AUTH_FAILURES:
                        LOGGER.error(
                            "COSMOS rejected the streaming subscription %d times; giving up on telemetry", auth_failures
                        )
                        self.failure = exc  # published last: observers of `failure` also see the log line
                        return
                LOGGER.warning("COSMOS stream error (%s: %s); reconnecting in %.0fs", type(exc).__name__, exc, delay)
                self._stop.wait(delay)
                delay = min(delay * 2, RECONNECT_DELAY_MAX_SECONDS)


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
        self._pending_commands = 0

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
        if "@" in cosmos_url:  # anywhere: an unencoded password may contain '/', '?' or '#' and escape the netloc
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
            url = (
                connection_uri if "://" in connection_uri else f"http://{connection_uri}"
            )  # inverse of get_connection_uri
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
        self.disconnect()
        self.api = CosmosApi(url, self.scope, os.environ.get(PASSWORD_ENV))
        try:
            self._require_websocket_client()
            if self.dictionaries is None:
                raise CosmosTransportError("set_pipeline_references() must be called before connect()")
            self.api.authenticate()
            self._index_channels()
            packets = self._subscription_packets()
        except BaseException:
            self.disconnect()
            raise
        LOGGER.info(
            "Connected to COSMOS %s scope=%s target=%s layout=%s; %d packet(s)",
            redact_url(url),
            self.scope,
            self.target,
            self.naming.layout,
            len(packets),
        )
        try:
            self.stream = StreamingSocket(
                self.api.cable_url(), self.api.authenticate, self.scope, packets, self._on_packet, token=self.api.token
            )
            self.stream.start()
        except BaseException:
            self.disconnect()
            raise

    @staticmethod
    def _require_websocket_client():
        """Fail connect() clearly when websocket-client is missing or shadowed by another ``websocket`` module

        Without it the stream would fail anyway, but with an AttributeError or a bare ImportError.
        """
        try:
            import websocket
        except ImportError as error:
            raise CosmosTransportError("websocket-client is required for the cosmos transport") from error
        if not callable(getattr(websocket, "create_connection", None)):
            raise CosmosTransportError("the installed 'websocket' module is not websocket-client")

    def disconnect(self):
        if self.stream is not None:
            self.stream.stop()
            self.stream = None
        if self.api is not None:
            self.api.session.close()
            self.api = None

    def send(self, data):
        """Encoded GDS packets: the echo of a command just issued through the cmd API is expected, anything else
        (file uplink, raw packets) has no COSMOS path and fails the caller rather than stalling it"""
        if self._pending_commands > 0:
            self._pending_commands -= 1
            return
        raise CosmosTransportError(
            "Binary GDS packets (file uplink, raw packets) are not carried by the COSMOS transport"
        )

    def recv(self, timeout=None):
        return b""

    def data_callback(self, data, sender=None):
        if isinstance(data, CmdData):
            if self.stream is not None and self.stream.failure is not None:
                raise CosmosTransportError(f"COSMOS telemetry stream gave up: {self.stream.failure}")
            self._issue_command(data)
            self._pending_commands += 1
            return
        super().data_callback(data, sender)

    # ---- commands -------------------------------------------------------------------------------------------------
    def _issue_command(self, cmd_data: CmdData):
        """Issue the command through the COSMOS cmd API; a COSMOS rejection is raised to the caller"""
        template = cmd_data.get_template()
        name = self.naming.command_name(template.get_full_name())
        specs, args = template.get_args(), cmd_data.get_args()
        if len(specs) != len(args):
            raise CosmosTransportError(f"{name}: {len(args)} argument(s) for {len(specs)} parameter(s)")
        parameters: dict[str, Any] = {}
        for (arg_name, _description, arg_type), arg in zip(specs, args, strict=True):
            parameters.update(command_parameters(self.naming.command_parameter(arg_name), arg_type, python_value(arg)))
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
        try:
            for full_name, template in getattr(self.dictionaries, "channel_name", {}).items():
                key = self.naming.channel_key(full_name)
                if key in self._channel_index:
                    LOGGER.warning(
                        "Channels %s and %s collide on COSMOS name %s",
                        self._channel_index[key].get_full_name(),
                        full_name,
                        key,
                    )
                self._channel_index[key] = template
                packet = self.naming.channel_packet(template)
                if packet is not None:
                    self._packet_channels[packet] = template
        except DictionaryError as error:
            raise CosmosTransportError(f"Dictionary name not usable in COSMOS: {error}") from error

    def _subscription_packets(self) -> list[str]:
        names = self.api.call("get_all_tlm_names", self.target) or []
        if not names:
            raise CosmosTransportError(
                f"COSMOS target {self.target!r} has no telemetry packets in scope {self.scope!r}; "
                "is the plugin for this deployment installed?"
            )
        skipped = {CHANNELIZED_PACKET, UNKNOWN_PACKET}
        if self.naming.layout == "fprime":
            try:
                packet_names = {
                    identifier(packet.get_name()).upper()
                    for packet in (getattr(self.dictionaries, "packet", None) or {}).values()
                }
            except DictionaryError as error:
                raise CosmosTransportError(f"Dictionary name not usable in COSMOS: {error}") from error
            wanted = set(self._packet_channels) | packet_names
            unmatched = [name for name in names if name.upper() not in wanted | skipped]
            names = [name for name in names if name.upper() in wanted]
            if unmatched:
                LOGGER.info(
                    "%d COSMOS packet(s) of target %s are not in the dictionary and are not subscribed: %s",
                    len(unmatched),
                    self.target,
                    ", ".join(sorted(unmatched)[:10]) + (", ..." if len(unmatched) > 10 else ""),
                )
            if not names:
                raise CosmosTransportError(
                    f"None of the COSMOS packets of target {self.target!r} matches a dictionary channel or packet; "
                    "was the plugin generated from this dictionary?"
                )
        return [f"DECOM__TLM__{self.target}__{name}__CONVERTED" for name in names if name.upper() not in skipped]

    def _channels_in(self, packet_name: str | None, items: dict[str, Any]) -> list[tuple[Any, str]]:
        """(channel template, item-name root) pairs carried by one streamed packet"""
        channel_id = items.get(CHANNEL_ID_ITEM)
        if channel_id is not None:
            template = (getattr(self.dictionaries, "channel_id", None) or {}).get(int(channel_id))
            if template is None:
                key = f"{packet_name}:{channel_id}"
                if key not in self._failed_channels:
                    self._failed_channels.add(key)
                    LOGGER.warning("COSMOS packet %s carries unknown F Prime channel id %s", packet_name, channel_id)
                return []
            return [(template, self._dedicated_root(template, items))]
        dedicated = self._packet_channels.get(packet_name.upper()) if packet_name else None
        if dedicated is not None:
            return [(dedicated, self._dedicated_root(dedicated, items))]
        found: dict[str, Any] = {}
        for item_name in items:
            if item_name.startswith("__") or item_name in NON_VALUE_ITEMS:
                continue
            key = self._channel_root(item_name)
            if key is not None:
                found[key] = self._channel_index[key]
        return [(template, key) for key, template in found.items()]

    def _dedicated_root(self, template, items: dict[str, Any]) -> str:
        """Leaf-name root of a dedicated packet, or the full dotted key when a generator spells that instead"""
        root = self.naming.dedicated_item(template)
        if any(name == root or name.startswith((f"{root}.", f"{root}[", f"{root}_")) for name in items):
            return root
        dotted = self.naming.channel_key(template.get_full_name())
        return (
            dotted if any(name == dotted or name.startswith((f"{dotted}.", f"{dotted}[")) for name in items) else root
        )

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

    def _packet_time(self, items: dict[str, Any]) -> TimeType:
        """F Prime time from the FPRIME_TIME_* items; the COSMOS receive time (workstation time base) when they are
        missing, unparsable or out of range; time zero when the packet has no usable ``__time`` either

        The label is resolved through the ``TimeBase`` enum of the loaded dictionary (so project-specific bases work);
        a label absent from it (a COSMOS plugin generated from another dictionary) is reported once and the packet is
        kept with the receive time rather than dropped or re-labelled as workstation time.
        """
        base = items.get("FPRIME_TIME_BASE")
        if base is not None:
            try:
                seconds, useconds = int(items["FPRIME_TIME_SECONDS"]), int(items["FPRIME_TIME_USECONDS"])
                context = int(items.get("FPRIME_TIME_CONTEXT", 0))
            except Exception as exc:
                LOGGER.debug("No usable F Prime time in packet (%s); using the receive time", exc)
            else:
                try:
                    time_base = time_base_from_label(base) if isinstance(base, str) else int(base)
                    TimeType(time_base=time_base)  # validates the base alone
                except Exception as exc:
                    key = f"time base {base!r}"
                    if key not in self._failed_channels:
                        self._failed_channels.add(key)
                        LOGGER.warning(
                            "F Prime time base %r is not in the loaded dictionary (%s); using the COSMOS receive time",
                            base,
                            exc,
                        )
                else:
                    try:
                        return TimeType(time_base=time_base, time_context=context, seconds=seconds, useconds=useconds)
                    except Exception as exc:  # out-of-range seconds/useconds/context: keep the packet, receive time
                        LOGGER.debug("F Prime time out of range (%s); using the receive time", exc)
        received = items.get("__time")
        try:
            nanoseconds = int(received)
            return TimeType(
                time_base=TimeType.TimeBase(TIME_BASE_WORKSTATION),
                seconds=nanoseconds // 1_000_000_000,
                useconds=(nanoseconds % 1_000_000_000) // 1_000,
            )
        except Exception as exc:
            LOGGER.debug("No usable receive time in packet (%s); using time zero", exc)
        return TimeType()


def split_stream_packet(packet: dict) -> tuple[str | None, dict[str, Any]]:
    """Split an ActionCable DECOM packet into (packet name, {ITEM: value}).

    COSMOS 7 sends ``__packet`` = ``DECOM__TLM__<TARGET>__<PACKET>__CONVERTED`` (older builds: plain packet name)
    plus the items keyed by plain item name; ``__time``/``__type`` metadata is kept only for ``__time``.
    """
    packet_name = packet.get("__packet") or packet.get("packet")
    if isinstance(packet_name, str) and "__" in packet_name:
        packet_name = stream_packet_name(packet_name)
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


def stream_packet_name(key: str) -> str:
    """``PACKET`` out of ``DECOM__TLM__TARGET__PACKET__CONVERTED`` (or ``TARGET__PACKET``); tolerates ``__`` in names"""
    for suffix in ("__CONVERTED", "__RAW", "__FORMATTED", "__WITH_UNITS"):
        if key.endswith(suffix):
            key = key[: -len(suffix)]
            break
    parts = key.split("__", 3)
    if parts[0] in ("DECOM", "RAW") and len(parts) == 4:
        return parts[3]
    return key.split("__", 1)[-1]


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


def command_parameters(name: str, type_class: type, value: Any) -> dict[str, Any]:
    """COSMOS command parameters for one F Prime argument, flattened as ``fprime_cosmos.items.flatten`` emits them

    Structs become ``NAME.MEMBER`` parameters, arrays of numbers one array parameter, other arrays ``NAME[i]``; a
    string's ``NAME_LENGTH`` companion is derived by COSMOS from the string and not sent. Booleans are sent as their
    COSMOS state names, enums as their labels. Parameter names are upper-cased (COSMOS does the same).
    """
    if issubclass(type_class, SerializableType):
        parameters: dict[str, Any] = {}
        for member, member_type, _format, _description in type_class.MEMBER_LIST:
            parameters.update(command_parameters(f"{name}.{member}", member_type, value[member]))
        return parameters
    if issubclass(type_class, ArrayType):
        member_type = type_class.MEMBER_TYPE
        if issubclass(member_type, NumericalType):
            return {name.upper(): list(value)}
        parameters = {}
        for index, member in enumerate(value):
            parameters.update(command_parameters(f"{name}[{index}]", member_type, member))
        return parameters
    if issubclass(type_class, BoolType):
        return {name.upper(): BOOL_STATES[1] if value else BOOL_STATES[0]}
    return {name.upper(): value}


def time_base_from_label(label: str):
    """fprime-gds ``TimeBase`` enum for a label as COSMOS reports it (upper-cased); ValueError when unknown"""
    from fprime_gds.common.utils.config_manager import ConfigManager

    time_base_type = ConfigManager().get_type("TimeBase")
    return time_base_type(coerce_scalar(label, time_base_type))


def coerce_scalar(value: Any, type_class: type) -> Any:
    """Python value of one COSMOS CONVERTED item for a primitive/enum/string F Prime type

    Raises ValueError (or TypeError for a value of the wrong shape, e.g. a list where a number is expected) when the
    value does not fit the type; callers treat both as "cannot rebuild".
    """
    if isinstance(value, dict) and value.get("json_class") == "Float":  # COSMOS's form for NaN / +-Infinity
        value = float(value.get("raw"))
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
            by_upper = {label.upper(): label for label in type_class.ENUM_DICT}  # COSMOS reports states upper-cased
            if value.upper() not in by_upper:
                raise ValueError(f"{value!r} is not a {type_class.__name__} label")
            return by_upper[value.upper()]
        if isinstance(value, float) and not value.is_integer():
            raise ValueError(f"{value!r} is not a {type_class.__name__} value")
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
        if isinstance(value, dict) and value.get("json_class") == "String":  # COSMOS form of non-UTF-8 bytes
            raw = value.get("raw")
            if not isinstance(raw, (list, bytes, bytearray)):
                raise ValueError(f"COSMOS binary string carries {type(raw).__name__} instead of bytes")
            limit = getattr(type_class, "MAX_LENGTH", None)
            if limit is not None and len(raw) > limit:  # checked before allocating/decoding anything
                raise ValueError(f"String size {len(raw)} is greater than {limit}")
            return bytes(raw).decode("utf-8", "replace")
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
