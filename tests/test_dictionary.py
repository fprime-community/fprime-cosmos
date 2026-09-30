"""The dictionary model exposes the wire layout read from the reference dictionary's type definitions"""

import json

import pytest

from fprime_cosmos.dictionary import DictionaryError, FprimeDictionary
from tests.conftest import REFERENCE_DICTIONARY


def test_metadata(reference_dictionary):
    assert reference_dictionary.deployment_name == "FprimeYamcsReference.YamcsDeployment"
    assert reference_dictionary.packet_set_name == "YamcsDeploymentPackets"
    assert len(reference_dictionary.commands) == 47
    assert len(reference_dictionary.channels) == 90
    assert len(reference_dictionary.packets) == 21


def test_layout_matches_dictionary_types(reference_dictionary):
    layout = reference_dictionary.layout
    assert layout.descriptor_bits == 16
    assert layout.opcode_bits == 32
    assert layout.channel_id_bits == 32
    assert layout.packet_id_bits == 16
    assert layout.string_length_bits == 16
    assert layout.time_base_bits == 16
    assert layout.time_context_bits == 8
    assert layout.bool_true == 255
    assert layout.bool_false == 0
    assert layout.command_descriptor == 0
    assert layout.telemetry_descriptor == 1
    assert layout.packetized_descriptor == 4
    assert layout.time_base_states["TB_WORKSTATION_TIME"] == 2


def test_content_hash_is_stable(reference_dictionary):
    assert reference_dictionary.content_hash() == FprimeDictionary(REFERENCE_DICTIONARY).content_hash()
    assert len(reference_dictionary.content_hash()) == 12


def test_missing_dictionary(tmp_path):
    with pytest.raises(DictionaryError):
        FprimeDictionary(tmp_path / "missing.json")


def test_unknown_packet_set():
    with pytest.raises(DictionaryError, match="not in dictionary"):
        FprimeDictionary(REFERENCE_DICTIONARY, "NoSuchPacketSet")


def test_multiple_packet_sets_require_selection(tmp_path):
    data = json.loads(REFERENCE_DICTIONARY.read_text())
    second = dict(data["telemetryPacketSets"][0], name="SecondSet")
    data["telemetryPacketSets"].append(second)
    path = tmp_path / "dictionary.json"
    path.write_text(json.dumps(data))
    with pytest.raises(DictionaryError, match="multiple packet sets"):
        FprimeDictionary(path)
    assert FprimeDictionary(path, "SecondSet").packet_set_name == "SecondSet"


def test_dictionary_without_packet_sets(tmp_path):
    data = json.loads(REFERENCE_DICTIONARY.read_text())
    data["telemetryPacketSets"] = []
    path = tmp_path / "dictionary.json"
    path.write_text(json.dumps(data))
    dictionary = FprimeDictionary(path)
    assert dictionary.packet_set_name is None
    assert dictionary.packets == []
