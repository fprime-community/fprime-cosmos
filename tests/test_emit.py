"""Emitted COSMOS definitions match the golden files generated from the reference dictionary"""

import re
from pathlib import Path

import pytest

from fprime_cosmos import emit
from fprime_cosmos.dictionary import DictionaryError
from fprime_cosmos.items import CosmosItem

GOLDEN = Path(__file__).parent / "golden"
EMITTERS = {"commands.txt": emit.emit_commands, "channels.txt": emit.emit_channels, "packets.txt": emit.emit_packets}


@pytest.mark.parametrize("filename", sorted(EMITTERS))
def test_golden(reference_dictionary, filename):
    generated = EMITTERS[filename](reference_dictionary)
    expected = (GOLDEN / filename).read_text()
    assert generated == expected, f"{filename} differs from golden; regenerate with tests/update_golden.py"


@pytest.mark.parametrize("filename", sorted(EMITTERS))
def test_only_target_name_erb_remains(reference_dictionary, filename):
    generated = EMITTERS[filename](reference_dictionary)
    assert set(re.findall(r"<%.*?%>", generated)) == {emit.TARGET}


def test_commands_have_descriptor_then_opcode(reference_dictionary):
    command = next(c for c in reference_dictionary.commands if c.get_full_name().endswith("CMD_NO_OP_STRING"))
    lines = emit.emit_command(command, reference_dictionary.layout)
    assert lines[0].startswith("COMMAND <%= target_name %> CdhCore.cmdDisp.CMD_NO_OP_STRING BIG_ENDIAN")
    assert lines[1] == '  APPEND_ID_PARAMETER FPRIME_DESCRIPTOR 16 UINT 0 65535 0 "F Prime packet descriptor"'
    assert (
        lines[3]
        == f'  APPEND_ID_PARAMETER FPRIME_OPCODE 32 UINT 0 4294967295 {command.get_op_code()} "F Prime command opcode"'
    )
    assert '  APPEND_PARAMETER arg1_LENGTH 16 UINT 0 65535 0 "Length in bytes of arg1 (maximum 40)"' in lines
    assert "    VARIABLE_BIT_SIZE arg1_LENGTH 8 0" in lines


def test_channel_subpacket_layout(reference_dictionary):
    channel = next(c for c in reference_dictionary.channels if c.get_name() == "CommandsDispatched")
    lines = emit.emit_channel(channel, reference_dictionary.layout)
    assert lines[1] == "  SUBPACKET"
    assert lines[2] == f'  APPEND_ID_ITEM FPRIME_CHANNEL_ID 32 UINT {channel.get_id()} "F Prime channel identifier"'
    assert '  APPEND_ITEM FPRIME_TIME_BASE 16 UINT "F Prime time base"' in lines
    assert '  APPEND_ITEM FPRIME_TIME_CONTEXT 8 UINT "F Prime time context"' in lines
    assert lines[-2] == '  APPEND_ITEM CommandsDispatched 32 UINT "Number of commands dispatched"'


def test_parent_packet_uses_subpacketizer(reference_dictionary):
    lines = emit.emit_channelized_parent(reference_dictionary.layout)
    assert lines[1] == "  SUBPACKETIZER fprime_subpacketizer.py"
    assert '  APPEND_ID_ITEM FPRIME_DESCRIPTOR 16 UINT 1 "F Prime packet descriptor"' in lines
    assert lines[-2:] == ['  APPEND_ITEM CHANNELS 0 BLOCK "Concatenated channel records"', "    HIDDEN"]


def test_packetized_packet_ids(reference_dictionary):
    packet = reference_dictionary.packets[0]
    lines = emit.emit_packet(packet, reference_dictionary.layout)
    assert '  APPEND_ID_ITEM FPRIME_DESCRIPTOR 16 UINT 4 "F Prime packet descriptor"' in lines
    assert f'  APPEND_ID_ITEM FPRIME_PACKET_ID 16 UINT {packet.get_id()} "F Prime packet identifier"' in lines


@pytest.mark.parametrize(
    ("python", "printf"),
    [
        ("{}", None),
        (None, None),
        ("{:d}", "%d"),
        ("{:.3f} V", "%.3f V"),
        ("{:08x}", "%08x"),
        ("{:>10}", None),
        ("{:.2e}", "%.2e"),
        ("{} %", "%s %%"),
        ("{:.2f}%", "%.2f%%"),
        ("{{{}}}", "{%s}"),
        ("100%", "100%%"),
    ],
)
def test_printf_format(python, printf):
    assert emit.printf_format(python) == printf


@pytest.mark.parametrize("name", ["a<%= x %>", "a b", "a\nb", "1abc", ""])
def test_identifier_rejects_unsafe_names(name):
    with pytest.raises(DictionaryError):
        emit.identifier(name)


def test_identifier_accepts_flattened_names():
    assert emit.identifier("Ref.recvBuffComp.PktState.member[3]") == "Ref.recvBuffComp.PktState.member[3]"
    with pytest.raises(DictionaryError):
        emit.number("1e308 ")


def test_catch_all_is_last_in_packets(reference_dictionary):
    channels = emit.emit_channels(reference_dictionary)
    packets = emit.emit_packets(reference_dictionary)
    assert emit.UNKNOWN_PACKET not in channels
    definitions = re.findall(rf"^TELEMETRY {re.escape(emit.TARGET)} (\S+)", packets, re.MULTILINE)
    assert definitions[-1] == emit.UNKNOWN_PACKET and len(definitions) > 1


def test_quote_flattens_whitespace_and_quotes():
    assert emit.quote(' say "hi"\n there ') == "\"say 'hi' there\""
    assert emit.quote(None) == '""'


class StubChannel:
    """Just the ChTemplate accessors `_channel_modifiers` reads"""

    def __init__(self, fmt=None, low_red=None, low_yellow=None, high_yellow=None, high_red=None):
        self.values = (fmt, low_red, low_yellow, high_yellow, high_red)

    def get_format_str(self):
        return self.values[0]

    def get_low_red(self):
        return self.values[1]

    def get_low_yellow(self):
        return self.values[2]

    def get_high_yellow(self):
        return self.values[3]

    def get_high_red(self):
        return self.values[4]


def test_channel_modifiers_limits_default_to_type_range_and_neighbouring_bound():
    item = CosmosItem("V", 8, "UINT")
    assert emit._channel_modifiers(StubChannel(high_red=200), item) == ["LIMITS DEFAULT 1 ENABLED 0 0 200 200"]
    assert emit._channel_modifiers(StubChannel("{:.2f}", -1.5, -1.0, 1.0, 1.5), CosmosItem("V", 32, "FLOAT")) == [
        'FORMAT_STRING "%.2f"',
        "LIMITS DEFAULT 1 ENABLED -1.5 -1.0 1.0 1.5",
    ]
    assert emit._channel_modifiers(StubChannel("{}"), item) == []
    enum_item = CosmosItem("V", 8, "UINT", states=(("A", 0),))
    assert emit._channel_modifiers(StubChannel("{:d}"), enum_item) == []
