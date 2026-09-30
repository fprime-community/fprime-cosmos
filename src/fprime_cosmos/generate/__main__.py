"""fprime-to-cosmos: generate a COSMOS plugin (directory and gem) from an F Prime dictionary

The dictionary is identified exactly as for fprime-gds (--dictionary, or detected from -d/--deployment or
the current F Prime project). By default the plugin is only written to disk, ready for `openc3cli load` or
the COSMOS admin page. With --install the gem is also uploaded to a running COSMOS through its plugins API.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from fprime_gds.executables.cli import ConfigDrivenParser, DictionaryParser, ParserBase

from fprime_cosmos.cosmos_api import CosmosApiError
from fprime_cosmos.dictionary import DictionaryError, FprimeDictionary
from fprime_cosmos.install import CosmosParser, install_from_arguments
from fprime_cosmos.items import UnsupportedTypeError
from fprime_cosmos.plugin_builder import build_plugin

DESCRIPTION = "Generate a COSMOS plugin from an F Prime dictionary"


class GeneratorParser(ParserBase):
    """Where and what to generate"""

    DESCRIPTION = "Plugin generation options"

    def get_arguments(self) -> dict[tuple[str, ...], dict[str, Any]]:
        return {
            ("-o", "--output"): {
                "type": Path,
                "default": Path("openc3-plugin"),
                "help": "Output directory. [default: %(default)s]",
            },
            ("--no-gem",): {
                "action": "store_true",
                "help": "Write the plugin directory only; do not package a gem",
            },
            ("--install",): {
                "action": "store_true",
                "help": "Also install the gem into COSMOS",
            },
        }

    def handle_arguments(self, args, **kwargs):
        if args.install and args.no_gem:
            raise ValueError("--install needs the gem; drop --no-gem")
        return args


def parse_args(arguments: list[str] | None = None):
    args, _ = ConfigDrivenParser.parse_args([DictionaryParser, CosmosParser, GeneratorParser], DESCRIPTION, arguments)
    return args


def main(arguments: list[str] | None = None) -> int:
    args = parse_args(arguments)
    try:
        dictionary = FprimeDictionary(args.dictionary, args.packet_set_name)
        artifacts = build_plugin(dictionary, args.output, args.target_name, gem=not args.no_gem)
        print(f"[INFO] Plugin written to {artifacts.directory}")
        if artifacts.gem_path:
            print(f"[INFO] Gem written to {artifacts.gem_path}")
        if args.install:
            install_from_arguments(args, artifacts)
    except (DictionaryError, UnsupportedTypeError, CosmosApiError, ValueError) as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
