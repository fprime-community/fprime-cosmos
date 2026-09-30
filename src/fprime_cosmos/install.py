"""Installing a generated plugin into a running COSMOS, shared by fprime-to-cosmos --install and fprime-cosmos"""

from __future__ import annotations

import argparse
import os
from typing import Any

from fprime_gds.executables.cli import ParserBase

from fprime_cosmos.cosmos_api import CosmosClient, variable_value
from fprime_cosmos.plugin_builder import GEM_NAME_PREFIX, STATIC_TARGET, PluginArtifacts

TARGET_VARIABLE = "fprime_target_name"
PASSWORD_ENVIRONMENT = "OPENC3_API_PASSWORD"  # noqa: S105 - name of the variable, not a secret


class CosmosParser(ParserBase):
    """COSMOS connection and plugin options; the tool decides whether installing is the default"""

    DESCRIPTION = "COSMOS options"

    def get_arguments(self) -> dict[tuple[str, ...], dict[str, Any]]:
        return {
            ("--target-name",): {
                "default": STATIC_TARGET,
                "help": "Default COSMOS target name. [default: %(default)s]",
            },
            ("--cosmos-url",): {
                "default": "http://localhost:2900",
                "help": "COSMOS base URL. [default: %(default)s]",
            },
            ("--cosmos-password",): {
                "default": os.environ.get(PASSWORD_ENVIRONMENT),
                "help": f"COSMOS password. [default: ${PASSWORD_ENVIRONMENT}]",
            },
            ("--cosmos-scope",): {
                "default": "DEFAULT",
                "help": "COSMOS scope. [default: %(default)s]",
            },
            ("--cosmos-variable",): {
                "action": "append",
                "default": [],
                "metavar": "NAME=VALUE",
                "help": "Override a plugin variable (repeatable)",
            },
            ("--force-install",): {
                "action": "store_true",
                "help": "Reinstall the plugin even if this dictionary is already installed",
            },
        }

    def handle_arguments(self, args, **kwargs):
        args.cosmos_variables = parse_variables(args.cosmos_variable)
        return args


def parse_variables(pairs: list[str]) -> dict[str, str]:
    variables = {}
    for pair in pairs:
        name, separator, value = pair.partition("=")
        if not separator or not name:
            raise ValueError(f"Plugin variable must be NAME=VALUE, got '{pair}'")
        variables[name] = value
    return variables


def installed_plugin_for_target(client: CosmosClient, target: str) -> str | None:
    """The fprime plugin currently serving `target`, whichever dictionary it was generated from"""
    for name in client.plugins():
        if not name.startswith(f"{GEM_NAME_PREFIX}-"):
            continue
        if variable_value(client.plugin(name).get("variables", {}).get(TARGET_VARIABLE)) == target:
            return name
    return None


def declared_variables(artifacts: PluginArtifacts) -> dict[str, str]:
    """Plugin variables and defaults declared by the generated plugin.txt"""
    declared = {}
    for line in (artifacts.directory / "plugin.txt").read_text().splitlines():
        fields = line.split()
        if len(fields) >= 3 and fields[0] == "VARIABLE":
            declared[fields[1]] = " ".join(fields[2:])
    return declared


def variables_match(client: CosmosClient, plugin: str, artifacts: PluginArtifacts, variables: dict[str, str]) -> bool:
    """True when the installed plugin carries the effective value (override or plugin default) of every variable"""
    installed = client.plugin(plugin).get("variables", {})
    effective = {**declared_variables(artifacts), **variables}
    return all(str(variable_value(installed.get(name))) == str(value) for name, value in effective.items())


def ensure_installed(
    client: CosmosClient, artifacts: PluginArtifacts, variables: dict[str, str], target: str, force: bool
) -> None:
    """Install the gem unless a plugin with the same plugin digest and variables already serves the target"""
    client.authenticate()
    existing = installed_plugin_for_target(client, target)
    same_gem = existing is not None and existing.startswith(f"{artifacts.plugin_prefix}.gem__")
    if same_gem and not force:
        if variables_match(client, existing, artifacts, variables):
            print(
                f"[INFO] COSMOS target {target} already runs {existing}; skipping install (--force-install overrides)"
            )
            return
        print(f"[INFO] Plugin variables changed for {existing}")
    gem = artifacts.gem_path.name
    action = f"Upgrading {existing} to {gem}" if existing else f"Installing {gem}"
    print(f"[INFO] {action} in COSMOS scope {client.scope}")
    client.install_gem(str(artifacts.gem_path), variables, existing)
    print(f"[INFO] Target {target} now runs {gem}")


def install_from_arguments(args: argparse.Namespace, artifacts: PluginArtifacts) -> None:
    """Install `artifacts` into the COSMOS described by the CosmosParser options

    Raises CosmosApiError for COSMOS failures.
    """
    variables = args.cosmos_variables
    client = CosmosClient(args.cosmos_url, args.cosmos_password, args.cosmos_scope)
    target = variables.get(TARGET_VARIABLE, args.target_name)
    ensure_installed(client, artifacts, variables, target, args.force_install)
