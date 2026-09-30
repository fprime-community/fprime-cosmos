"""Installing a built plugin into COSMOS: shared by fprime-to-cosmos --install and fprime-cosmos"""

import pytest
from fprime_gds.executables.cli import ParserBase

from fprime_cosmos import install
from fprime_cosmos.plugin_builder import PluginArtifacts

PLUGIN_TXT = """VARIABLE fprime_target_name FPRIME
VARIABLE fprime_bridge_host host.docker.internal
VARIABLE fprime_bridge_port 50001
TARGET FPRIME <%= fprime_target_name %>
"""
DEFAULTS = {"fprime_bridge_host": "host.docker.internal", "fprime_bridge_port": "50001"}


class FakeClient:
    scope = "DEFAULT"

    def __init__(self, installed, variables=None):
        self.installed = installed
        self.variables = {**DEFAULTS, **(variables or {})}
        self.installs = []

    def authenticate(self):
        pass

    def plugins(self):
        return list(self.installed)

    def plugin(self, name):
        variables = {name: {"value": value} for name, value in self.variables.items()}
        return {"variables": {"fprime_target_name": {"value": self.installed[name]}, **variables}}

    def install_gem(self, gem_path, variables, existing):
        self.installs.append((gem_path, variables, existing))
        return "new"


def artifacts(tmp_path, digest="abc"):
    gem = tmp_path / f"openc3-cosmos-fprime-ref-1.0.0.{digest}.gem"
    gem.touch()
    (tmp_path / "plugin.txt").write_text(PLUGIN_TXT)
    return PluginArtifacts(tmp_path, "openc3-cosmos-fprime-ref", f"1.0.0.{digest}", gem)


def test_declared_variables(tmp_path):
    assert install.declared_variables(artifacts(tmp_path)) == {"fprime_target_name": "FPRIME", **DEFAULTS}


def test_parse_variables():
    assert install.parse_variables(["a=1", "b=x=y"]) == {"a": "1", "b": "x=y"}
    with pytest.raises(ValueError):
        install.parse_variables(["novalue"])


def test_installed_plugin_for_target_ignores_other_targets_and_plugins():
    client = FakeClient(
        {
            "openc3-cosmos-tool-admin-7.4.1.gem__0": None,
            "openc3-cosmos-fprime-ref-two-1.0.0.111.gem__0": "OTHER",
            "openc3-cosmos-fprime-yamcs-1.0.0.222.gem__0": "FPRIME",
        }
    )
    assert install.installed_plugin_for_target(client, "FPRIME") == "openc3-cosmos-fprime-yamcs-1.0.0.222.gem__0"
    assert install.installed_plugin_for_target(client, "NONE") is None


def test_ensure_installed_skips_same_digest_and_upgrades_target_owner(tmp_path):
    same = "openc3-cosmos-fprime-ref-1.0.0.abc.gem__3"
    client = FakeClient({same: "FPRIME"})
    install.ensure_installed(client, artifacts(tmp_path), {}, "FPRIME", force=False)
    assert client.installs == []
    install.ensure_installed(client, artifacts(tmp_path), {}, "FPRIME", force=True)
    assert client.installs[-1][2] == same

    other = "openc3-cosmos-fprime-other-1.0.0.999.gem__0"
    client = FakeClient({other: "FPRIME", "openc3-cosmos-fprime-ref-1.0.0.abc.gem__0": "REF2"})
    install.ensure_installed(client, artifacts(tmp_path), {}, "FPRIME", force=False)
    assert client.installs[-1][2] == other
    client = FakeClient({})
    install.ensure_installed(client, artifacts(tmp_path), {"fprime_bridge_port": "1"}, "FPRIME", force=False)
    assert client.installs[-1][1:] == ({"fprime_bridge_port": "1"}, None)


def test_ensure_installed_applies_changed_variables_to_same_gem(tmp_path):
    same = "openc3-cosmos-fprime-ref-1.0.0.abc.gem__3"
    client = FakeClient({same: "FPRIME"}, variables={"fprime_bridge_port": 50001})
    install.ensure_installed(client, artifacts(tmp_path), {"fprime_bridge_port": "50001"}, "FPRIME", force=False)
    assert client.installs == []
    variables = {"fprime_bridge_host": "192.168.1.5"}
    install.ensure_installed(client, artifacts(tmp_path), variables, "FPRIME", force=False)
    assert client.installs == [(str(artifacts(tmp_path).gem_path), variables, same)]


def test_ensure_installed_reverts_dropped_override_to_plugin_default(tmp_path):
    same = "openc3-cosmos-fprime-ref-1.0.0.abc.gem__3"
    client = FakeClient({same: "FPRIME"}, variables={"fprime_bridge_host": "192.168.1.5"})
    install.ensure_installed(client, artifacts(tmp_path), {}, "FPRIME", force=False)
    assert client.installs == [(str(artifacts(tmp_path).gem_path), {}, same)]


def test_install_from_arguments_uses_cosmos_options(tmp_path, monkeypatch):
    created = {}
    owner = "openc3-cosmos-fprime-other-1.0.0.999.gem__0"
    clients = []

    def fake_client(url, password, scope):
        created.update(url=url, password=password, scope=scope)
        clients.append(FakeClient({owner: "REF"}))
        return clients[-1]

    monkeypatch.setattr(install, "CosmosClient", fake_client)
    args, _ = ParserBase.parse_args(
        [install.CosmosParser],
        "test",
        ["--cosmos-url", "http://c:2900", "--cosmos-password", "pw", "--cosmos-variable", "fprime_target_name=REF"],
    )
    assert args.cosmos_variables == {"fprime_target_name": "REF"}
    install.install_from_arguments(args, artifacts(tmp_path))
    assert created == {"url": "http://c:2900", "password": "pw", "scope": "DEFAULT"}
    assert clients[0].installs == [(str(artifacts(tmp_path).gem_path), {"fprime_target_name": "REF"}, owner)]
    with pytest.raises(SystemExit):
        ParserBase.parse_args([install.CosmosParser], "test", ["--cosmos-variable", "broken"])
