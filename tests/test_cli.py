"""Command line entry points: fprime-gds style arguments plus the COSMOS options"""

import socket
from argparse import Namespace
from unittest.mock import patch

import pytest
from fprime_gds.common.communication.adapters.udp_fast import UdpFastAdapter
from fprime_gds.executables.run_deployment import BASE_MODULE_ARGUMENTS

from fprime_cosmos import __main__ as launcher
from fprime_cosmos.generate import __main__ as generate
from fprime_cosmos.generate.__main__ import main as generate_main
from tests.conftest import REFERENCE_DICTIONARY

GEM_DIRECTORY = "openc3-cosmos-fprime-yamcsdeployment"


def test_generate_cli(tmp_path, capsys):
    assert generate_main(["--dictionary", str(REFERENCE_DICTIONARY), "-o", str(tmp_path), "--no-gem"]) == 0
    assert (tmp_path / GEM_DIRECTORY / "plugin.txt").is_file()
    assert "Plugin written" in capsys.readouterr().out


def test_generate_cli_detects_dictionary_from_deployment(tmp_path):
    deployment = tmp_path / "deployment"
    (deployment / "dict").mkdir(parents=True)
    (deployment / "dict" / "ReferenceDeploymentTopologyDictionary.json").write_bytes(REFERENCE_DICTIONARY.read_bytes())
    assert generate_main(["-d", str(deployment), "-o", str(tmp_path / "out"), "--no-gem"]) == 0
    assert (tmp_path / "out" / GEM_DIRECTORY / "plugin.txt").is_file()


def test_generate_cli_bad_dictionary(tmp_path, capsys):
    with pytest.raises(SystemExit):
        generate_main(["--dictionary", str(tmp_path / "nope.json"), "-o", str(tmp_path)])
    assert "[ERROR]" in capsys.readouterr().err


def test_generate_install_requires_gem(tmp_path, monkeypatch):
    installed = []
    monkeypatch.setattr(generate, "install_from_arguments", lambda args, artifacts: installed.append(artifacts))
    assert generate_main(["--dictionary", str(REFERENCE_DICTIONARY), "-o", str(tmp_path), "--install"]) == 0
    assert installed[0].gem_path.is_file()
    with pytest.raises(SystemExit):
        generate_main(["--dictionary", str(REFERENCE_DICTIONARY), "-o", str(tmp_path), "--install", "--no-gem"])


def test_generate_without_install_does_not_touch_cosmos(tmp_path, monkeypatch):
    def fail(*args):
        raise AssertionError("COSMOS must not be contacted without --install")

    monkeypatch.setattr(generate, "install_from_arguments", fail)
    assert generate_main(["--dictionary", str(REFERENCE_DICTIONARY), "-o", str(tmp_path)]) == 0


def test_generate_rejects_bad_cosmos_variable(tmp_path):
    with pytest.raises(SystemExit):
        generate_main(["--dictionary", str(REFERENCE_DICTIONARY), "-o", str(tmp_path), "--cosmos-variable", "novalue"])


def launcher_argv(tmp_path, *extra):
    """fprime-gds style arguments generating into tmp_path without contacting COSMOS"""
    return [
        "--dictionary",
        str(REFERENCE_DICTIONARY),
        "-l",
        str(tmp_path / "logs"),
        "--cosmos-plugin-dir",
        str(tmp_path / "plugin"),
        "--skip-install",
        *extra,
    ]


@pytest.fixture
def launches(monkeypatch):
    """Record bridge and deployment launches instead of starting processes"""
    started = []

    class Process:
        def __init__(self, name):
            self.name = name

        returncode = 0

        def wait(self):
            started.append(("wait", self.name))
            return self.returncode

    def launch_process(command, name=None, **kwargs):
        started.append(("bridge", command))
        return Process("bridge")

    def launch_app(args, connection=None):
        started.append(("app", args.app, connection, args.application_arguments))
        return Process("app")

    monkeypatch.setattr(launcher, "launch_process", launch_process)
    monkeypatch.setattr(launcher, "launch_app", launch_app)
    monkeypatch.setattr(
        launcher, "wait_for_bridge", lambda connection, timeout=None: started.append(("ready", connection))
    )
    monkeypatch.setattr(launcher, "docker_source_addresses", lambda: [])
    return started


def test_launcher_generate_only(tmp_path, launches):
    assert launcher.main(launcher_argv(tmp_path, "-n", "--communication-selection", "none")) == 0
    assert list((tmp_path / "plugin").glob("*.gem"))
    assert launches == []


def test_launcher_bridge_reproduces_gds_arguments(tmp_path, launches):
    argv = launcher_argv(tmp_path, "-n", "--communication-selection", "tcp-fast-client", "--tcp-fast-port", "60000")
    assert launcher.main(argv) == 0
    (kind, command), waited = launches
    assert kind == "bridge" and waited == ("wait", "bridge")
    assert command[: len(BASE_MODULE_ARGUMENTS) + 1] == [*BASE_MODULE_ARGUMENTS, launcher.BRIDGE_MODULE]
    assert command[len(BASE_MODULE_ARGUMENTS) + 1 :][:2] == ["--dictionary", str(REFERENCE_DICTIONARY)]
    options = dict(zip(command, command[1:], strict=False))
    assert options["--communication-selection"] == "tcp-fast-client"
    assert options["--tcp-fast-port"] == "60000"
    assert options["--framing-selection"] == launcher.DEFAULT_FRAMING
    assert options["--udp-fast-bind-address"] == UdpFastAdapter.DEFAULT_BIND_ADDRESS
    assert "--udp-fast-allowed-source" not in command


def test_launcher_defaults_to_bridge_tcp_fast_server(tmp_path, launches):
    assert launcher.main(launcher_argv(tmp_path, "-n")) == 0
    (_kind, command), _waited = launches
    assert dict(zip(command, command[1:], strict=False))["--communication-selection"] == launcher.DEFAULT_COMMUNICATION


def test_launcher_starts_app_after_bridge(tmp_path, launches):
    (tmp_path / "bin").mkdir()
    app = tmp_path / "bin" / "Ref"
    app.write_text("#!/bin/sh\n")
    argv = launcher_argv(tmp_path, "-d", str(tmp_path), "--tcp-fast-port", "60000")
    assert launcher.main(argv) == 0
    assert [entry[0] for entry in launches] == ["bridge", "ready", "app", "wait"]
    assert launches[2] == ("app", app, ("127.0.0.1", 60000), None)


def test_launcher_forwards_application_arguments(tmp_path, launches):
    (tmp_path / "bin").mkdir()
    app = tmp_path / "bin" / "Ref"
    app.write_text("#!/bin/sh\n")
    argv = launcher_argv(tmp_path, "-d", str(tmp_path), "--application-arguments", "extra", "argument")
    assert launcher.main(argv) == 0
    assert launches[1] == ("ready", ("127.0.0.1", 50000))
    assert launches[2] == ("app", app, ("127.0.0.1", 50000), ["extra", "argument"])


def test_launcher_installs_unless_skipped(tmp_path, launches, monkeypatch):
    installed = []
    monkeypatch.setattr(launcher, "install_from_arguments", lambda args, artifacts: installed.append(artifacts))
    argv = [arg for arg in launcher_argv(tmp_path, "-n") if arg != "--skip-install"]
    assert launcher.main(argv) == 0
    assert installed[0].gem_path.is_file()
    assert [entry[0] for entry in launches] == ["bridge", "wait"]


def test_launcher_skips_app_without_a_connection(tmp_path, launches, capsys):
    (tmp_path / "bin").mkdir()
    (tmp_path / "bin" / "Ref").write_text("#!/bin/sh\n")
    argv = launcher_argv(tmp_path, "-d", str(tmp_path), "--communication-selection", "tcp-fast-client")
    assert launcher.main(argv) == 0
    assert [entry[0] for entry in launches] == ["bridge", "wait"]
    assert "cannot be auto-launched with the tcp-fast-client adapter" in capsys.readouterr().out
    argv = launcher_argv(
        tmp_path, "-d", str(tmp_path), "--communication-selection", "uart", "--application-arguments", "serial"
    )
    assert launcher.main(argv) == 0
    assert [entry[0] for entry in launches[2:]] == ["bridge", "app", "wait"]


def test_launcher_reports_bridge_failure(tmp_path, launches, monkeypatch):
    bridge = launcher.launch_process(["x"], name="bridge")
    bridge.returncode = 2
    monkeypatch.setattr(launcher, "launch_process", lambda *args, **kwargs: bridge)
    launches.clear()
    assert launcher.main(launcher_argv(tmp_path, "-n")) == 1


def test_wait_for_bridge_returns_once_listening_and_times_out_otherwise():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        connection = listener.getsockname()
        launcher.wait_for_bridge(connection, timeout=2)
        listener.accept()[0].close()
    with pytest.raises(RuntimeError, match="not accepting connections"):
        launcher.wait_for_bridge(connection, timeout=0.5)


def test_launcher_rejects_missing_app(tmp_path):
    with pytest.raises(SystemExit):
        launcher.main(launcher_argv(tmp_path, "--app", str(tmp_path / "nope")))


def bridge_args(**overrides):
    values = {
        "cosmos_url": "http://localhost:2900",
        "udp_fast_bind_address": UdpFastAdapter.DEFAULT_BIND_ADDRESS,
        "udp_fast_allowed_sources": None,
    }
    values.update(overrides)
    return Namespace(**values)


def test_docker_defaults_for_local_docker_cosmos(monkeypatch):
    monkeypatch.setattr(launcher, "docker_source_addresses", lambda: ["172.18.0.6", "172.17.0.1"])
    monkeypatch.setattr(launcher, "docker_host_address", lambda: "172.17.0.1")
    args = bridge_args()
    launcher.apply_docker_defaults(args)
    assert args.udp_fast_bind_address == "172.17.0.1"
    assert args.udp_fast_allowed_sources == ["172.18.0.6", "172.17.0.1"]


def test_docker_defaults_respect_user_udp_settings(monkeypatch):
    monkeypatch.setattr(launcher, "docker_source_addresses", lambda: ["172.18.0.6"])
    monkeypatch.setattr(launcher, "docker_host_address", lambda: "172.17.0.1")
    args = bridge_args(udp_fast_allowed_sources=["10.0.0.1"])
    launcher.apply_docker_defaults(args)
    assert args.udp_fast_bind_address == UdpFastAdapter.DEFAULT_BIND_ADDRESS
    args = bridge_args(udp_fast_bind_address="192.168.1.5")
    launcher.apply_docker_defaults(args)
    assert args.udp_fast_allowed_sources is None


def test_docker_defaults_only_for_local_cosmos(monkeypatch):
    monkeypatch.setattr(launcher, "docker_source_addresses", lambda: ["172.18.0.6"])
    addresses = {"cosmos.example.invalid": "203.0.113.5", "cosmos.local": "10.0.0.7", "myhost": "10.0.0.7"}
    monkeypatch.setattr(launcher.socket, "gethostname", lambda: "myhost")

    def resolve(host):
        if host not in addresses:
            raise OSError(host)
        return addresses[host]

    monkeypatch.setattr(launcher.socket, "gethostbyname", resolve)
    args = bridge_args(cosmos_url="http://cosmos.example.invalid:2900")
    launcher.apply_docker_defaults(args)
    assert args.udp_fast_allowed_sources is None
    assert launcher.cosmos_is_local("http://user@cosmos.local:2900/path")
    assert launcher.cosmos_is_local("http://[::1]:2900") is False


def test_docker_defaults_without_docker(monkeypatch):
    monkeypatch.setattr(launcher, "docker_source_addresses", lambda: [])
    args = bridge_args()
    launcher.apply_docker_defaults(args)
    assert args.udp_fast_bind_address == UdpFastAdapter.DEFAULT_BIND_ADDRESS


def test_docker_defaults_bind_any_without_docker0(monkeypatch):
    monkeypatch.setattr(launcher, "docker_source_addresses", lambda: ["172.18.0.6"])
    monkeypatch.setattr(launcher, "docker_host_address", lambda: None)
    args = bridge_args()
    launcher.apply_docker_defaults(args)
    assert args.udp_fast_bind_address == launcher.BIND_ANY


def test_docker_gateway_addresses(monkeypatch):
    output = (
        "1: lo    inet 127.0.0.1/8 scope host lo\n"
        "3: docker0    inet 172.17.0.1/16 brd 172.17.255.255 scope global docker0\n"
        "4: br-5    inet 172.18.0.1/16 brd 172.18.255.255 scope global br-5\n"
    )
    monkeypatch.setattr(launcher.subprocess, "run", lambda *a, **k: type("R", (), {"stdout": output})())
    assert launcher.docker_gateway_addresses() == ["172.17.0.1", "172.18.0.1"]
    assert launcher.docker_host_address() == "172.17.0.1"


def test_docker_container_addresses(monkeypatch):
    outputs = iter(["abc\ndef\n", "172.18.0.6\n\n172.18.0.7\n"])
    monkeypatch.setattr(launcher.subprocess, "run", lambda *a, **k: type("R", (), {"stdout": next(outputs)})())
    assert launcher.docker_container_addresses() == ["172.18.0.6", "172.18.0.7"]


def test_docker_source_addresses_without_docker(monkeypatch):
    def fail(*a, **k):
        raise OSError("no docker")

    monkeypatch.setattr(launcher.subprocess, "run", fail)
    assert launcher.docker_source_addresses() == []


def test_launch_comm_bridge_applies_docker_defaults(tmp_path):
    args = launcher.parse_args(launcher_argv(tmp_path, "-n"))
    with (
        patch.object(launcher, "docker_source_addresses", return_value=["172.18.0.6"]),
        patch.object(launcher, "docker_host_address", return_value="172.17.0.1"),
        patch.object(launcher, "launch_process") as launch,
    ):
        launcher.launch_comm_bridge(args)
    command = launch.call_args.args[0]
    assert launch.call_args.kwargs["name"] == "fprime-comm-bridge[tcp-fast-server]"
    options = dict(zip(command, command[1:], strict=False))
    assert options["--udp-fast-bind-address"] == "172.17.0.1"
    assert options["--udp-fast-allowed-source"] == "172.18.0.6"
