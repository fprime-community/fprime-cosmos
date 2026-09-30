"""fprime-cosmos: run an F Prime deployment against COSMOS, with the fprime-gds command line

A drop-in for `fprime-gds`/`fprime-yamcs`: the dictionary, deployment, application and logging options
(-d/--deployment, --dictionary, --app, -n/--no-app, --application-arguments, -l/--logs, ...) and the
communication/framing plugin options are the fprime-gds ones. The launcher generates the COSMOS plugin from
the dictionary, installs it when COSMOS does not already run a plugin with the same plugin digest,
starts fprime-comm-bridge with the selected communication adapter and framing, and then starts the
deployment connected to the bridge.
"""

from __future__ import annotations

import ipaddress
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fprime_gds.common.communication.adapters.udp_fast import UdpFastAdapter
from fprime_gds.executables.cli import (
    BinaryDeployment,
    ConfigDrivenParser,
    DictionaryParser,
    LogDeployParser,
    ParserBase,
    PluginArgumentParser,
)
from fprime_gds.executables.run_deployment import BASE_MODULE_ARGUMENTS, app_connection, launch_app, launch_process
from fprime_gds.plugin.system import Plugins

from fprime_cosmos.cosmos_api import CosmosApiError
from fprime_cosmos.dictionary import DictionaryError, FprimeDictionary
from fprime_cosmos.install import CosmosParser, install_from_arguments
from fprime_cosmos.items import UnsupportedTypeError
from fprime_cosmos.plugin_builder import build_plugin

DESCRIPTION = "Run an F Prime deployment with COSMOS"
BRIDGE_MODULE = "fprime_gds.executables.comm_bridge"
LAUNCHER_PLUGIN_CATEGORIES = ["communication", "framing"]
# fprime-comm-bridge defaults: the deployment's TcpClient connects to the bridge's tcp-fast-server; COSMOS
# receives plain F Prime packets, so Space Packet and Space Data Link framing is stripped in the bridge
DEFAULT_COMMUNICATION = "tcp-fast-server"
DEFAULT_FRAMING = "space-packet-space-data-link"
NO_COMMUNICATION = "none"
BRIDGE_LAUNCH_TIME = 1  # seconds the bridge must stay up before it is probed
BRIDGE_READY_TIMEOUT = 30  # seconds to wait for the bridge to accept a connection before starting the deployment
BRIDGE_POLL_INTERVAL = 0.2
DOCKER_INTERFACE_PREFIXES = ("docker", "br-")
DOCKER_HOST_INTERFACE = "docker0"  # host.docker.internal resolves to the default bridge gateway
BIND_ANY = "0.0.0.0"  # noqa: S104 - last resort when the Docker host address cannot be determined
LOOPBACK = "127.0.0.1"


class CosmosPluginArgumentParser(PluginArgumentParser):
    """The fprime-gds communication and framing plugin options, defaulting to the bridge's choices"""

    FPRIME_CHOICES = {
        **PluginArgumentParser.FPRIME_CHOICES,
        "communication": DEFAULT_COMMUNICATION,
        "framing": DEFAULT_FRAMING,
    }

    def __init__(self, plugin_system: Plugins | None = None):
        super().__init__(plugin_system or Plugins(LAUNCHER_PLUGIN_CATEGORIES))


class LauncherParser(ParserBase):
    """Options specific to the COSMOS launcher"""

    DESCRIPTION = "COSMOS launcher options"

    def get_arguments(self) -> dict[tuple[str, ...], dict[str, Any]]:
        return {
            ("--cosmos-plugin-dir",): {
                "type": Path,
                "default": Path.cwd() / "openc3-plugin",
                "help": "Directory the COSMOS plugin and gem are generated into. [default: %(default)s]",
            },
            ("--skip-install",): {
                "action": "store_true",
                "help": "Generate the plugin but do not install it into COSMOS",
            },
        }

    def handle_arguments(self, args, **kwargs):
        return args


def parse_args(arguments: list[str] | None = None):
    """Parse the fprime-gds style command line plus the COSMOS options"""
    handlers = [
        DictionaryParser,
        BinaryDeployment,
        LogDeployParser,
        CosmosPluginArgumentParser,
        CosmosParser,
        LauncherParser,
    ]
    args, _ = ConfigDrivenParser.parse_args(handlers, DESCRIPTION, arguments)
    return args


def docker_interfaces() -> list[tuple[str, str]]:
    """(interface, IPv4 address) of the local Docker bridge interfaces"""
    try:
        output = subprocess.run(["ip", "-4", "-o", "addr"], check=True, capture_output=True, text=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return []
    interfaces = []
    for line in output.splitlines():
        fields = line.split()
        if len(fields) >= 4 and fields[1].startswith(DOCKER_INTERFACE_PREFIXES):
            interfaces.append((fields[1], str(ipaddress.ip_interface(fields[3]).ip)))
    return interfaces


def docker_gateway_addresses() -> list[str]:
    """IPv4 addresses of local Docker bridge interfaces"""
    return [address for _interface, address in docker_interfaces()]


def docker_host_address() -> str | None:
    """The address containers reach the host at (host.docker.internal), so the bridge need not bind every interface"""
    return next(
        (address for interface, address in docker_interfaces() if interface.startswith(DOCKER_HOST_INTERFACE)), None
    )


def docker_container_addresses() -> list[str]:
    """IPv4 addresses of the running Docker containers, i.e. the sources COSMOS sends commands from"""
    template = "{{range .NetworkSettings.Networks}}{{println .IPAddress}}{{end}}"
    try:
        containers = subprocess.run(["docker", "ps", "-q"], check=True, capture_output=True, text=True).stdout.split()
        if not containers:
            return []
        command = ["docker", "inspect", "--format", template, *containers]
        output = subprocess.run(command, check=True, capture_output=True, text=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return []
    return [line.strip() for line in output.splitlines() if line.strip()]


def docker_source_addresses() -> list[str]:
    """Addresses a local Docker COSMOS may send from: container addresses plus the bridge gateways"""
    return list(dict.fromkeys([*docker_container_addresses(), *docker_gateway_addresses()]))


def cosmos_is_local(cosmos_url: str) -> bool:
    host = urlsplit(cosmos_url).hostname or ""
    try:
        return host in ("localhost", LOOPBACK) or socket.gethostbyname(host) == socket.gethostbyname(
            socket.gethostname()
        )
    except OSError:
        return False


def apply_docker_defaults(args) -> None:
    """Let a COSMOS running in local Docker containers reach the bridge's UDP side

    Only applied when COSMOS is local and --udp-fast-bind-address/--udp-fast-allowed-source keep their defaults.
    """
    if not cosmos_is_local(args.cosmos_url):
        return
    if args.udp_fast_bind_address != UdpFastAdapter.DEFAULT_BIND_ADDRESS or args.udp_fast_allowed_sources is not None:
        return
    sources = docker_source_addresses()
    if not sources:
        return
    args.udp_fast_bind_address = docker_host_address() or BIND_ANY
    args.udp_fast_allowed_sources = sources


def bridge_arguments(args) -> list[str]:
    """fprime-comm-bridge command line reproducing the launcher's dictionary and plugin selections"""
    return ["--dictionary", str(args.dictionary), *CosmosPluginArgumentParser().reproduce_cli_args(args)]


def launch_comm_bridge(args):
    """Start fprime-comm-bridge between the deployment and COSMOS"""
    apply_docker_defaults(args)
    command = [*BASE_MODULE_ARGUMENTS, BRIDGE_MODULE, *bridge_arguments(args)]
    return launch_process(
        command, name=f"fprime-comm-bridge[{args.communication_selection}]", launch_time=BRIDGE_LAUNCH_TIME
    )


def wait_for_bridge(connection: tuple[str, int], timeout: float = BRIDGE_READY_TIMEOUT) -> None:
    """Block until the bridge accepts a TCP connection at `connection`; RuntimeError when `timeout` seconds pass"""
    address, port = connection
    deadline = time.monotonic() + timeout
    while True:
        try:
            with socket.create_connection((address, port), timeout=BRIDGE_POLL_INTERVAL):
                return
        except OSError as error:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"fprime-comm-bridge not accepting connections at {address}:{port}: {error}"
                ) from error
        time.sleep(BRIDGE_POLL_INTERVAL)


def launch_deployment_app(args):
    """Start the deployment once the bridge accepts connections (fprime-gds --application-arguments overrides)"""
    connection = app_connection(args)
    if connection is not None:
        wait_for_bridge(connection)
    return launch_app(args, connection)


def main(arguments: list[str] | None = None) -> int:
    args = parse_args(arguments)
    try:
        dictionary = FprimeDictionary(args.dictionary, args.packet_set_name)
        artifacts = build_plugin(dictionary, args.cosmos_plugin_dir, args.target_name)
        print(f"[INFO] Generated {artifacts.gem_path}")
        if not args.skip_install:
            install_from_arguments(args, artifacts)
    except (DictionaryError, UnsupportedTypeError, CosmosApiError, ValueError) as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        return 1
    launchers = []
    if args.communication_selection != NO_COMMUNICATION:
        launchers.append(launch_comm_bridge)
    if not args.noapp:
        if app_connection(args) is not None or args.application_arguments is not None:
            launchers.append(launch_deployment_app)
        else:
            print(
                f"[WARNING] App cannot be auto-launched with the {args.communication_selection} adapter without "
                "--application-arguments; start it manually"
            )
    if not launchers:
        return 0
    try:
        processes = [launcher(args) for launcher in launchers]
        print("[INFO] F Prime deployment and COSMOS bridge running. CTRL-C to shutdown all components.")
        status = processes[0].wait()  # the bridge (or the deployment without one); the rest are stopped at exit
    except KeyboardInterrupt:
        print("[INFO] CTRL-C received. Exiting.")
        return 0
    except Exception as error:  # noqa: BLE001 - launch failures are reported, then the launcher exits
        print(f"[ERROR] {error}", file=sys.stderr)
        return 1
    return 0 if not status else 1


if __name__ == "__main__":
    sys.exit(main())
