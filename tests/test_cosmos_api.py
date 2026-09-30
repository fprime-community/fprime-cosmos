"""The COSMOS client follows the two-phase plugin install protocol"""

import json

import pytest
import requests

from fprime_cosmos import cosmos_api
from fprime_cosmos.cosmos_api import CosmosApiError, CosmosClient


class FakeResponse:
    def __init__(self, status=200, body=None, text=None):
        self.status_code = status
        self._body = body
        self.text = text if text is not None else json.dumps(body)
        self.request = type("Request", (), {"method": "X"})()
        self.url = "http://cosmos/x"

    def json(self):
        if self._body is None:
            raise ValueError("not json")
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []
        self.headers = {}

    def request(self, method, url, **kwargs):
        self.calls.append((method, url.split("/openc3-api")[1], kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(cosmos_api, "POLL_SECONDS", 0)
    client = CosmosClient("http://cosmos/", "secret", scope="OPS")
    client.session = FakeSession([])
    return client


def test_authenticate_existing_password(client):
    client.session.responses = [FakeResponse(body={"result": True}), FakeResponse(text="tok\n")]
    client.authenticate()
    assert client.session.headers["Authorization"] == "tok"
    method, path, kwargs = client.session.calls[1]
    assert (method, path, kwargs["data"]) == ("POST", "/auth/verify", {"password": "secret"})
    assert kwargs["params"] == {"scope": "OPS"}


def test_authenticate_sets_password_on_fresh_cosmos(client):
    client.session.responses = [FakeResponse(body={"result": False}), FakeResponse(text="tok")]
    client.authenticate()
    assert client.session.calls[1][1] == "/auth/set"


def test_authenticate_requires_password():
    with pytest.raises(CosmosApiError, match="password"):
        CosmosClient("http://cosmos", None).authenticate()


def test_http_error_reports_message(client):
    client.session.responses = [FakeResponse(status=500, body={"status": "error", "message": "boom"})]
    with pytest.raises(CosmosApiError, match="boom"):
        client.plugins()


def test_connection_error(client):
    client.session.responses = [requests.ConnectionError("refused")]
    with pytest.raises(CosmosApiError, match="Cannot reach COSMOS"):
        client.plugins()


def test_plugin_reads_installed_hash(client):
    client.session.responses = [FakeResponse(body={"name": "p__0", "variables": {"fprime_target_name": "X"}})]
    assert client.plugin("p__0")["variables"] == {"fprime_target_name": "X"}
    assert client.session.calls[0][:2] == ("GET", "/plugins/p__0")


def test_install_gem_two_phases(client, tmp_path):
    gem = tmp_path / "plugin.gem"
    gem.write_bytes(b"gem")
    plugin_hash = {
        "name": "plugin.gem",
        "variables": {"fprime_bridge_host": {"value": "host.docker.internal", "description": "d"}},
    }
    client.session.responses = [
        FakeResponse(body=plugin_hash),
        FakeResponse(text="proc__1"),
        FakeResponse(body={"state": "Running"}),
        FakeResponse(body={"state": "Complete", "output": ""}),
    ]
    assert client.install_gem(str(gem), {"fprime_bridge_host": "10.0.0.1"}) == "plugin.gem"
    upload, install, *polls = client.session.calls
    assert upload[0:2] == ("POST", "/plugins")
    assert upload[2]["files"]["plugin"][0] == "plugin.gem"
    assert install[0:2] == ("POST", "/plugins/install/plugin.gem")
    sent = json.loads(install[2]["data"]["plugin_hash"])
    assert sent["variables"]["fprime_bridge_host"]["value"] == "10.0.0.1"
    assert [poll[1] for poll in polls] == ["/process_status/proc__1"] * 2


def test_install_gem_upgrade_uses_put(client, tmp_path):
    gem = tmp_path / "plugin.gem"
    gem.write_bytes(b"gem")
    client.session.responses = [
        FakeResponse(body={"name": "plugin.gem", "variables": {}}),
        FakeResponse(text='"proc__2"'),
        FakeResponse(body={"state": "Complete"}),
    ]
    assert client.install_gem(str(gem), existing="old.gem__0") == "old.gem__0"
    upload, install, _ = client.session.calls
    assert upload[0:2] == ("PUT", "/plugins/old.gem__0")
    assert install[0:2] == ("POST", "/plugins/install/plugin.gem")
    assert json.loads(install[2]["data"]["plugin_hash"])["name"] == "old.gem__0"


def test_install_unknown_variable(client, tmp_path):
    gem = tmp_path / "plugin.gem"
    gem.write_bytes(b"gem")
    client.session.responses = [FakeResponse(body={"name": "plugin.gem", "variables": {}})]
    with pytest.raises(CosmosApiError, match="Unknown plugin variables"):
        client.install_gem(str(gem), {"nope": "1"})


def test_install_crash_reports_output(client):
    client.session.responses = [
        FakeResponse(text="proc__3"),
        FakeResponse(body={"state": "Crashed", "output": "Error: bad"}),
    ]
    with pytest.raises(CosmosApiError, match="Crashed:\nError: bad"):
        client.install({"name": "plugin.gem"})


def test_install_timeout(client):
    client.session.responses = [FakeResponse(text="proc__4"), FakeResponse(body={"state": "Running"})]
    with pytest.raises(CosmosApiError, match="did not finish"):
        client.install({"name": "plugin.gem"}, timeout=0)


def test_variable_value():
    assert cosmos_api.variable_value({"value": "REF"}) == "REF"
    assert cosmos_api.variable_value("REF") == "REF"
