"""F Prime dictionary access built on the fprime-gds loaders

The fprime-gds `Dictionaries` model loads and type-resolves the JSON topology dictionary. Everything that
shapes the wire format (descriptor and identifier widths, boolean encodings, APIDs) is read from the
dictionary's `typeDefinitions` and `constants` so that a project with a non-default FpConfig produces a
matching COSMOS plugin.
"""

from __future__ import annotations

import hashlib
import warnings
from dataclasses import dataclass
from pathlib import Path

from fprime_gds.common.loaders.pkt_json_loader import PktJsonLoader
from fprime_gds.common.models.dictionaries import Dictionaries
from fprime_gds.common.models.serialize.enum_type import EnumType
from fprime_gds.common.templates.ch_template import ChTemplate
from fprime_gds.common.templates.cmd_template import CmdTemplate
from fprime_gds.common.templates.pkt_template import PktTemplate
from fprime_gds.common.utils.config_manager import ConfigManager

COM_PACKET_TYPE_ENUM = "Fw.ComPacketType"
APID_ENUM = "ComCfg.Apid"
TIME_BASE_ENUM = "TimeBase"

# Fw::ComPacketType values, used only when the dictionary does not publish the enumeration
FALLBACK_PACKET_TYPES = {
    "FW_PACKET_COMMAND": 0,
    "FW_PACKET_TELEM": 1,
    "FW_PACKET_LOG": 2,
    "FW_PACKET_FILE": 3,
    "FW_PACKET_PACKETIZED_TLM": 4,
    "FW_PACKET_DP": 5,
    "FW_PACKET_IDLE": 6,
    "FW_PACKET_UNKNOWN": 0xFF,
}

# Fw::Time serializes seconds and microseconds as U32 regardless of configuration
TIME_SECONDS_BITS = 32
TIME_USECONDS_BITS = 32


class DictionaryError(Exception):
    """Raised when the dictionary cannot support plugin generation"""


@dataclass(frozen=True)
class WireLayout:
    """Bit widths and constants describing the F Prime wire format for this dictionary"""

    descriptor_bits: int
    opcode_bits: int
    channel_id_bits: int
    packet_id_bits: int
    string_length_bits: int
    time_base_bits: int
    time_context_bits: int
    time_base_states: dict[str, int]
    bool_true: int
    bool_false: int
    packet_types: dict[str, int]

    @property
    def command_descriptor(self) -> int:
        return self.packet_types["FW_PACKET_COMMAND"]

    @property
    def telemetry_descriptor(self) -> int:
        return self.packet_types["FW_PACKET_TELEM"]

    @property
    def packetized_descriptor(self) -> int:
        return self.packet_types["FW_PACKET_PACKETIZED_TLM"]


def _type_bits(config: ConfigManager, name: str) -> int:
    return config.get_type(name).getMaxSize() * 8


def _enum_values(config: ConfigManager, name: str) -> dict[str, int] | None:
    try:
        enum_type = config.get_type(name)
    except Exception:  # noqa: BLE001 - ConfigManager raises a bare Exception for unknown names
        return None
    if not (isinstance(enum_type, type) and issubclass(enum_type, EnumType)):
        return None
    return dict(enum_type.ENUM_DICT)


def _packet_types(config: ConfigManager) -> dict[str, int]:
    for name in (COM_PACKET_TYPE_ENUM, APID_ENUM):
        values = _enum_values(config, name)
        if values and all(key in values for key in ("FW_PACKET_COMMAND", "FW_PACKET_TELEM")):
            return values
    warnings.warn(
        f"Dictionary defines neither {COM_PACKET_TYPE_ENUM} nor {APID_ENUM}; assuming default packet types",
        stacklevel=2,
    )
    return dict(FALLBACK_PACKET_TYPES)


class FprimeDictionary:
    """A loaded F Prime JSON dictionary with the pieces needed to generate a COSMOS plugin"""

    def __init__(self, path: str | Path, packet_set_name: str | None = None):
        self.path = Path(path).resolve()
        if not self.path.is_file():
            raise DictionaryError(f"Dictionary '{self.path}' does not exist")
        self.packet_set_name = self._select_packet_set(packet_set_name)
        try:
            self.dictionaries = Dictionaries.load_dictionaries_into_config(str(self.path), None, self.packet_set_name)
            self.layout = self._read_layout(ConfigManager.get_instance())
        except Exception as error:
            raise DictionaryError(f"Failed to load dictionary '{self.path}': {error}") from error

    def _select_packet_set(self, requested: str | None) -> str | None:
        names = PktJsonLoader(str(self.path)).get_packet_set_names(None)
        if requested is not None:
            if requested not in names:
                raise DictionaryError(f"Packet set '{requested}' not in dictionary. Available: {names}")
            return requested
        if len(names) > 1:
            raise DictionaryError(f"Dictionary has multiple packet sets {names}; choose one with --packet-set-name")
        return names[0] if names else None

    def _read_layout(self, config: ConfigManager) -> WireLayout:
        packet_types = _packet_types(config)
        return WireLayout(
            descriptor_bits=_type_bits(config, "FwPacketDescriptorType"),
            opcode_bits=_type_bits(config, "FwOpcodeType"),
            channel_id_bits=_type_bits(config, "FwChanIdType"),
            packet_id_bits=_type_bits(config, "FwTlmPacketizeIdType"),
            string_length_bits=_type_bits(config, "FwSizeStoreType"),
            time_base_bits=_type_bits(config, TIME_BASE_ENUM),
            time_context_bits=_type_bits(config, "FwTimeContextStoreType"),
            time_base_states=_enum_values(config, TIME_BASE_ENUM) or {},
            bool_true=int(config.get_constant("FW_SERIALIZE_TRUE_VALUE")),
            bool_false=int(config.get_constant("FW_SERIALIZE_FALSE_VALUE")),
            packet_types=packet_types,
        )

    @property
    def metadata(self) -> dict:
        return self.dictionaries.metadata

    @property
    def deployment_name(self) -> str:
        return self.metadata.get("deploymentName") or self.path.stem

    @property
    def commands(self) -> list[CmdTemplate]:
        return list(self.dictionaries.command_id.values())

    @property
    def channels(self) -> list[ChTemplate]:
        return list(self.dictionaries.channel_id.values())

    @property
    def packets(self) -> list[PktTemplate]:
        return list((self.dictionaries.packet or {}).values())

    def content_hash(self) -> str:
        """Short digest of the dictionary file, recorded in the gem metadata"""
        return hashlib.sha256(self.path.read_bytes()).hexdigest()[:12]
