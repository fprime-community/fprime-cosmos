"""fprime-to-cosmos-cfs: add F Prime applications hosted in cFS to a nasa/cfs-cosmos-plugin tree

The cFS plugin (its source checkout or the directory its Makefile stages) is copied as is; one command and one
telemetry definition file per F Prime dictionary are added to its CFS target and the result is packaged as a
single gem. With --install the gem is uploaded to a running COSMOS through its plugins API.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from fprime_gds.executables.cli import ConfigDrivenParser, ParserBase

from fprime_cosmos.cfs import DEFAULT_TARGET_NAME, CfsOverlayError, build_overlay
from fprime_cosmos.cosmos_api import CosmosApiError
from fprime_cosmos.dictionary import DictionaryError, FprimeDictionary
from fprime_cosmos.install import CosmosParser, install_from_arguments
from fprime_cosmos.items import UnsupportedTypeError
from fprime_cosmos.plugin_builder import STATIC_TARGET

DESCRIPTION = "Add F Prime cFS application dictionaries to a cfs-cosmos-plugin tree and package one gem"


class CfsGeneratorParser(ParserBase):
    """Inputs and outputs of the overlay"""

    DESCRIPTION = "cFS overlay options"

    def get_arguments(self) -> dict[tuple[str, ...], dict[str, Any]]:
        return {
            ("--cfs-plugin",): {
                "type": Path,
                "required": True,
                "help": "cfs-cosmos-plugin source checkout or staged plugin directory (contains plugin.txt)",
            },
            ("--dictionary",): {
                "type": Path,
                "action": "append",
                "required": True,
                "metavar": "JSON",
                "help": "F Prime dictionary of an application hosted in cFS (repeatable)",
            },
            ("-o", "--output"): {
                "type": Path,
                "default": Path("openc3-plugin"),
                "help": "Output directory. [default: %(default)s]",
            },
            ("--no-channelized",): {
                "action": "store_true",
                "help": "Omit channelized (TlmChan) telemetry definitions, e.g. when only packetized telemetry is "
                "downlinked and the FW_PACKET_TELEM message id is taken by a cFS packet",
            },
            ("--no-gem",): {
                "action": "store_true",
                "help": "Write the plugin directory only; do not package a gem",
            },
            ("--install",): {
                "action": "store_true",
                "help": f"Also install the gem into COSMOS (--target-name defaults to {DEFAULT_TARGET_NAME})",
            },
        }

    def handle_arguments(self, args, **kwargs):
        if args.install and args.no_gem:
            raise ValueError("--install needs the gem; drop --no-gem")
        return args


def parse_args(arguments: list[str] | None = None):
    args, _ = ConfigDrivenParser.parse_args([CfsGeneratorParser, CosmosParser], DESCRIPTION, arguments)
    if args.target_name == STATIC_TARGET:
        args.target_name = DEFAULT_TARGET_NAME
    return args


def main(arguments: list[str] | None = None) -> int:
    args = parse_args(arguments)
    try:
        dictionaries = [FprimeDictionary(path) for path in args.dictionary]
        artifacts = build_overlay(
            args.cfs_plugin, dictionaries, args.output, channelized=not args.no_channelized, gem=not args.no_gem
        )
        print(f"[INFO] Plugin written to {artifacts.directory}")
        if artifacts.gem_path:
            print(f"[INFO] Gem written to {artifacts.gem_path}")
        if args.install:
            install_from_arguments(args, artifacts)
    except (DictionaryError, CfsOverlayError, UnsupportedTypeError, CosmosApiError, ValueError) as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
