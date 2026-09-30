"""Minimal client for the OpenC3 COSMOS plugin API

Installs follow the same two phases as the COSMOS admin page: upload the gem (phase 1) to obtain the
plugin hash with its variables, then confirm the hash (phase 2) and wait for the install process.
"""

from __future__ import annotations

import json
import time
from typing import Any

import requests

API = "/openc3-api"
INSTALL_TIMEOUT_SECONDS = 600
POLL_SECONDS = 2.0
SUCCESS_STATES = {"Complete", "Warning"}
FAILURE_STATES = {"Crashed", "Error", "Expired"}
TERMINAL_STATES = SUCCESS_STATES | FAILURE_STATES


class CosmosApiError(Exception):
    """Raised when COSMOS rejects a request or an install fails"""


def variable_value(variable: Any) -> Any:
    """Value of a plugin variable as COSMOS reports it: a `{"value": ...}` hash or a bare scalar"""
    return variable.get("value") if isinstance(variable, dict) else variable


class CosmosClient:
    def __init__(self, url: str, password: str | None, scope: str = "DEFAULT", timeout: float = 30.0):
        self.url = url.rstrip("/")
        self.password = password
        self.scope = scope
        self.timeout = timeout
        self.session = requests.Session()

    def _check(self, response: requests.Response) -> requests.Response:
        if response.status_code >= 400:
            try:
                message = response.json().get("message", response.text)
            except ValueError:
                message = response.text
            raise CosmosApiError(f"{response.request.method} {response.url} failed ({response.status_code}): {message}")
        return response

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        params = {"scope": self.scope, **kwargs.pop("params", {})}
        try:
            response = self.session.request(
                method, f"{self.url}{API}{path}", params=params, timeout=self.timeout, **kwargs
            )
        except requests.RequestException as error:
            raise CosmosApiError(f"Cannot reach COSMOS at {self.url}: {error}") from error
        return self._check(response)

    def authenticate(self) -> None:
        """Exchange the password for a session token (setting the password on a fresh COSMOS if needed)"""
        if not self.password:
            raise CosmosApiError("A COSMOS password is required (--cosmos-password or OPENC3_API_PASSWORD)")
        exists = self._request("GET", "/auth/token-exists").json().get("result")
        if exists:
            token = self._request("POST", "/auth/verify", data={"password": self.password}).text
        else:
            print("[INFO] COSMOS has no password yet; setting it to the supplied password")
            token = self._request("POST", "/auth/set", data={"password": self.password}).text
        self.session.headers["Authorization"] = token.strip()

    def plugins(self) -> list[str]:
        """Names of the plugins installed in the scope"""
        return list(self._request("GET", "/plugins").json())

    def plugin(self, name: str) -> dict:
        """Hash of an installed plugin, including its variables as COSMOS stored them"""
        return self._request("GET", f"/plugins/{name}").json()

    def upload(self, gem_path: str, existing: str | None = None) -> dict:
        """Phase 1: upload the gem and return the plugin hash COSMOS proposes"""
        with open(gem_path, "rb") as gem:
            files = {"plugin": (gem_path.rsplit("/", 1)[-1], gem, "application/octet-stream")}
            if existing:
                return self._request("PUT", f"/plugins/{existing}", files=files).json()
            return self._request("POST", "/plugins", files=files).json()

    def install(self, plugin_hash: dict, gem_name: str | None = None, timeout: float = INSTALL_TIMEOUT_SECONDS) -> str:
        """Phase 2: confirm the plugin hash and wait until the install process finishes

        `gem_name` is the uploaded gem (from phase 1); `plugin_hash["name"]` names the plugin instance, which for an
        upgrade is the already installed plugin so COSMOS replaces it instead of creating a second copy.
        """
        name = gem_name or plugin_hash["name"]
        response = self._request("POST", f"/plugins/install/{name}", data={"plugin_hash": json.dumps(plugin_hash)})
        process = response.text.strip().strip('"')
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self._request("GET", f"/process_status/{process}").json()
            state = status.get("state") if status else None
            if state in TERMINAL_STATES:
                if state not in SUCCESS_STATES:
                    raise CosmosApiError(f"Plugin install {name} ended in state {state}:\n{status.get('output', '')}")
                return plugin_hash["name"]
            time.sleep(POLL_SECONDS)
        raise CosmosApiError(f"Plugin install {name} did not finish within {timeout} seconds")

    def install_gem(self, gem_path: str, variables: dict[str, str] | None = None, existing: str | None = None) -> str:
        """Upload and install a gem, overriding plugin variables, returning the installed plugin name"""
        plugin_hash = self.upload(gem_path, existing)
        gem_name = plugin_hash["name"]
        if existing:
            plugin_hash["name"] = existing
        if variables:
            unknown = set(variables) - set(plugin_hash.get("variables", {}))
            if unknown:
                raise CosmosApiError(f"Unknown plugin variables: {sorted(unknown)}")
            for name, value in variables.items():
                current = plugin_hash["variables"][name]
                if isinstance(current, dict):
                    current["value"] = value
                else:
                    plugin_hash["variables"][name] = value
        return self.install(plugin_hash, gem_name)
