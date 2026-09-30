"""Maps fprime-gds type classes onto COSMOS items

COSMOS items are flat, so structured F Prime types are flattened: struct members become `name.member`,
non-numeric array elements become `name[i]`, and numeric arrays become COSMOS array items. F Prime strings
are length-prefixed, so each string becomes a length item followed by a `VARIABLE_BIT_SIZE` string item.
"""

from __future__ import annotations

from dataclasses import dataclass

from fprime_gds.common.models.serialize.array_type import ArrayType
from fprime_gds.common.models.serialize.bool_type import BoolType
from fprime_gds.common.models.serialize.enum_type import EnumType
from fprime_gds.common.models.serialize.numerical_types import FloatType, IntegerType, NumericalType
from fprime_gds.common.models.serialize.serializable_type import SerializableType
from fprime_gds.common.models.serialize.string_type import StringType
from fprime_gds.common.models.serialize.type_base import BaseType

from fprime_cosmos.dictionary import WireLayout

BOOL_STATES = ("FALSE", "TRUE")


class UnsupportedTypeError(Exception):
    """Raised when an F Prime type has no COSMOS representation"""


@dataclass(frozen=True)
class CosmosItem:
    """One COSMOS item/parameter line plus its modifiers"""

    name: str
    bit_size: int
    data_type: str
    description: str = ""
    states: tuple[tuple[str, int], ...] = ()
    array_bit_size: int | None = None
    length_item: str | None = None
    default: int | float | str | None = None
    fprime_type: str = ""

    @property
    def is_array(self) -> bool:
        return self.array_bit_size is not None

    @property
    def is_variable_string(self) -> bool:
        return self.length_item is not None

    def value_range(self) -> tuple[int | float | str, int | float | str]:
        """COSMOS minimum/maximum tokens for a command parameter of this item"""
        if self.data_type == "UINT":
            return 0, (1 << self.bit_size) - 1
        if self.data_type == "INT":
            return -(1 << (self.bit_size - 1)), (1 << (self.bit_size - 1)) - 1
        if self.data_type == "FLOAT":
            return "MIN", "MAX"
        raise UnsupportedTypeError(f"No value range for {self.data_type}")


def is_signed(type_class: type[IntegerType]) -> bool:
    return type_class.range()[0] < 0


def scalar_type(type_class: type[NumericalType]) -> str:
    if issubclass(type_class, FloatType):
        return "FLOAT"
    return "INT" if is_signed(type_class) else "UINT"


def flatten(name: str, type_class: type[BaseType], layout: WireLayout, description: str = "") -> list[CosmosItem]:
    """Flatten an F Prime type into the ordered COSMOS items that serialize it"""
    fprime_type = type_class.__name__
    if issubclass(type_class, BoolType):
        states = ((BOOL_STATES[0], layout.bool_false), (BOOL_STATES[1], layout.bool_true))
        bits = type_class.getMaxSize() * 8
        return [CosmosItem(name, bits, "UINT", description, states, default=layout.bool_false, fprime_type="bool")]
    if issubclass(type_class, EnumType):
        rep_type = type_class.REP_TYPE
        states = tuple(type_class.ENUM_DICT.items())
        default_name = type_class.DEFAULT or next(iter(type_class.ENUM_DICT))
        default = type_class.ENUM_DICT[default_name]
        bits = rep_type.getMaxSize() * 8
        return [
            CosmosItem(name, bits, scalar_type(rep_type), description, states, default=default, fprime_type=fprime_type)
        ]
    if issubclass(type_class, NumericalType):
        bits = type_class.getMaxSize() * 8
        data_type = scalar_type(type_class)
        default = 0.0 if data_type == "FLOAT" else 0
        return [CosmosItem(name, bits, data_type, description, default=default, fprime_type=fprime_type)]
    if issubclass(type_class, StringType):
        length_name = f"{name}_LENGTH"
        max_length = type_class.MAX_LENGTH
        length_description = f"Length in bytes of {name}" + (f" (maximum {max_length})" if max_length else "")
        return [
            CosmosItem(
                length_name,
                layout.string_length_bits,
                "UINT",
                length_description,
                default=0,
                fprime_type="FwSizeStoreType",
            ),
            CosmosItem(name, 0, "STRING", description, length_item=length_name, default="", fprime_type=fprime_type),
        ]
    if issubclass(type_class, ArrayType):
        member_type = type_class.MEMBER_TYPE
        length = type_class.LENGTH
        if issubclass(member_type, NumericalType):
            bits = member_type.getMaxSize() * 8
            return [
                CosmosItem(
                    name,
                    bits,
                    scalar_type(member_type),
                    description,
                    array_bit_size=bits * length,
                    fprime_type=fprime_type,
                )
            ]
        items: list[CosmosItem] = []
        for index in range(length):
            items.extend(flatten(f"{name}[{index}]", member_type, layout, description))
        return items
    if issubclass(type_class, SerializableType):
        items = []
        for member_name, member_type, _member_format, member_description in type_class.MEMBER_LIST:
            items.extend(flatten(f"{name}.{member_name}", member_type, layout, member_description or description))
        return items
    raise UnsupportedTypeError(f"Cannot map F Prime type {fprime_type} for {name}")
