"""Type flattening covers every F Prime type kind with dictionary-derived widths"""

import pytest
from fprime_gds.common.models.serialize.array_type import ArrayType
from fprime_gds.common.models.serialize.bool_type import BoolType
from fprime_gds.common.models.serialize.enum_type import EnumType
from fprime_gds.common.models.serialize.numerical_types import F64Type, I16Type, U8Type, U32Type
from fprime_gds.common.models.serialize.serializable_type import SerializableType
from fprime_gds.common.models.serialize.string_type import StringType

from fprime_cosmos.dictionary import WireLayout
from fprime_cosmos.items import CosmosItem, UnsupportedTypeError, flatten

LAYOUT = WireLayout(
    descriptor_bits=16,
    opcode_bits=32,
    channel_id_bits=32,
    packet_id_bits=16,
    string_length_bits=8,
    time_base_bits=16,
    time_context_bits=8,
    time_base_states={},
    bool_true=1,
    bool_false=0,
    packet_types={"FW_PACKET_COMMAND": 0, "FW_PACKET_TELEM": 1, "FW_PACKET_PACKETIZED_TLM": 4},
)


def names(items):
    return [(item.name, item.bit_size, item.data_type) for item in items]


def test_scalars():
    assert names(flatten("a", U8Type, LAYOUT)) == [("a", 8, "UINT")]
    assert names(flatten("b", I16Type, LAYOUT)) == [("b", 16, "INT")]
    assert names(flatten("c", F64Type, LAYOUT)) == [("c", 64, "FLOAT")]


def test_bool_uses_layout_encoding():
    (item,) = flatten("flag", BoolType, LAYOUT, "a flag")
    assert (item.bit_size, item.data_type, item.states) == (8, "UINT", (("FALSE", 0), ("TRUE", 1)))
    assert item.default == 0
    assert item.description == "a flag"


def test_enum_states_and_default():
    enum = EnumType.construct_type("Mode", {"OFF": 0, "ON": 1, "AUTO": 2}, U8Type, "ON")
    (item,) = flatten("mode", enum, LAYOUT)
    assert item.bit_size == 8
    assert item.data_type == "UINT"
    assert item.states == (("OFF", 0), ("ON", 1), ("AUTO", 2))
    assert item.default == 1
    assert item.fprime_type == "Mode"


def test_string_is_length_prefixed_with_layout_width():
    string = StringType.construct_type("String_40", 40)
    length, value = flatten("arg1", string, LAYOUT)
    assert (length.name, length.bit_size, length.data_type) == ("arg1_LENGTH", 8, "UINT")
    assert (value.name, value.bit_size, value.data_type, value.length_item) == ("arg1", 0, "STRING", "arg1_LENGTH")
    assert value.is_variable_string
    assert "maximum 40" in length.description


def test_numeric_array_becomes_cosmos_array():
    array = ArrayType.construct_type("F64x3", F64Type, 3, "{}")
    (item,) = flatten("vec", array, LAYOUT)
    assert item.is_array
    assert (item.bit_size, item.data_type, item.array_bit_size) == (64, "FLOAT", 192)


def test_struct_and_non_numeric_array_are_flattened_in_order():
    enum = EnumType.construct_type("State", {"A": 0, "B": 1}, U8Type)
    struct = SerializableType.construct_type(
        "Pair", [("state", enum, "{}", "the state"), ("count", U32Type, "{}", "the count")]
    )
    array = ArrayType.construct_type("Pairs", struct, 2, "{}")
    items = flatten("pairs", array, LAYOUT, "outer")
    assert names(items) == [
        ("pairs[0].state", 8, "UINT"),
        ("pairs[0].count", 32, "UINT"),
        ("pairs[1].state", 8, "UINT"),
        ("pairs[1].count", 32, "UINT"),
    ]
    assert items[0].states == (("A", 0), ("B", 1))
    assert items[1].description == "the count"


def test_value_ranges():
    assert CosmosItem("a", 8, "UINT").value_range() == (0, 255)
    assert CosmosItem("a", 16, "INT").value_range() == (-32768, 32767)
    assert CosmosItem("a", 32, "FLOAT").value_range() == ("MIN", "MAX")
    with pytest.raises(UnsupportedTypeError):
        CosmosItem("a", 0, "STRING").value_range()


def test_unsupported_type():
    class Odd:
        pass

    with pytest.raises(UnsupportedTypeError):
        flatten("x", Odd, LAYOUT)  # type: ignore[arg-type]
