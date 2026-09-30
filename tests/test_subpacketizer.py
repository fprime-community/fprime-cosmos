"""The generated subpacketizer splits channel records, including variable-length strings, against the real openc3"""

import datetime
import importlib.util
import struct
import sys
from types import SimpleNamespace

import pytest

from fprime_cosmos.plugin_builder import build_plugin

openc3 = pytest.importorskip("openc3")
from openc3.packets.packet_config import PacketConfig  # noqa: E402

TARGET = "FPRIME"
STRING_CHANNEL = "CdhCore.version.FrameworkVersion"
U32_CHANNEL = "CdhCore.cmdDisp.CommandsDispatched"
TIME = struct.pack(">HBII", 2, 0, 1700000000, 5)
RECEIVED = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


@pytest.fixture(scope="module")
def plugin(reference_dictionary, tmp_path_factory):
    root = build_plugin(reference_dictionary, tmp_path_factory.mktemp("plugin"), gem=False).directory
    target = root / "targets" / TARGET
    channels = target / "cmd_tlm" / "channels.txt"
    channels.write_text(channels.read_text().replace("<%= target_name %>", TARGET))
    sys.path.insert(0, str(target / "lib"))
    try:
        config = PacketConfig()
        config.process_file(str(channels), TARGET)
        source = target / "lib" / "fprime_subpacketizer.py"
        spec = importlib.util.spec_from_file_location("fprime_subpacketizer", source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(target / "lib"))
    return SimpleNamespace(packets=config.telemetry[TARGET], module=module, layout=reference_dictionary.layout)


def record(packet, payload: bytes) -> bytes:
    return struct.pack(">I", packet.id_items[0].id_value) + TIME + payload


def identify_by_channel_id(packets):
    by_id = {packet.id_items[0].id_value: packet for packet in packets.values() if packet.subpacket}

    def identify(data, target_names, subpackets):
        assert target_names == [TARGET] and subpackets
        return by_id.get(struct.unpack_from(">I", data)[0])

    return identify


def test_record_length_matches_defined_length_for_fixed_channels(plugin):
    packet = plugin.packets[U32_CHANNEL.upper()]
    packet.buffer = record(packet, struct.pack(">I", 7)) + b"trailing"
    assert plugin.module.record_length(packet) == packet.defined_length == 4 + len(TIME) + 4


def test_record_length_includes_string_payload(plugin):
    packet = plugin.packets[STRING_CHANNEL.upper()]
    text = b"v4.0.0-dirty"
    packet.buffer = record(packet, struct.pack(">H", len(text)) + text) + b"trailing"
    assert plugin.module.record_length(packet) == 4 + len(TIME) + 2 + len(text)
    assert packet.defined_length == 4 + len(TIME) + 2


def test_subpacketizer_splits_string_then_u32(plugin, monkeypatch):
    string_packet = plugin.packets[STRING_CHANNEL.upper()]
    u32_packet = plugin.packets[U32_CHANNEL.upper()]
    text = b"v4.0.0"
    channels = record(string_packet, struct.pack(">H", len(text)) + text) + record(u32_packet, struct.pack(">I", 42))
    parent = plugin.packets["FPRIME_CHANNELS"]
    parent.buffer = plugin.layout.telemetry_descriptor.to_bytes(plugin.layout.descriptor_bits // 8, "big") + channels
    parent.received_time = RECEIVED
    telemetry = SimpleNamespace(identify=identify_by_channel_id(plugin.packets))
    monkeypatch.setattr(plugin.module, "System", SimpleNamespace(telemetry=telemetry))

    packets = plugin.module.FprimeSubpacketizer().call(parent)

    names = [STRING_CHANNEL.upper(), U32_CHANNEL.upper(), "FPRIME_CHANNELS"]
    assert [packet.packet_name for packet in packets] == names
    assert packets[0].read("FrameworkVersion") == text.decode()
    assert len(packets[0].buffer) == 4 + len(TIME) + 2 + len(text)
    assert packets[0].received_time == RECEIVED
    assert packets[1].read("CommandsDispatched") == 42
