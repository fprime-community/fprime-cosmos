# Derived from openc3-cosmos-fprime lib/fprime_subpacketizer.py
# Copyright 2026 OpenC3, Inc. Licensed under the MIT License; see the NOTICE file
# in the fprime-cosmos distribution for the full license text.
"""Splits an F Prime channelized telemetry packet into one COSMOS subpacket per channel

F Prime's TlmChan component concatenates channel records (id, time, value) into a single
downlink packet. Each channel is declared as a COSMOS SUBPACKET keyed on the channel id, so
this subpacketizer identifies records one at a time from the CHANNELS block and emits them
alongside the parent packet.
"""

from openc3.subpacketizers.subpacketizer import Subpacketizer
from openc3.system.system import System
from openc3.utilities.logger import Logger


class FprimeSubpacketizer(Subpacketizer):
    def call(self, packet):
        packets = []
        channels = packet.read("CHANNELS")
        while channels:
            subpacket = System.telemetry.identify(channels, target_names=[packet.target_name], subpackets=True)
            if subpacket is None:
                Logger.warn(
                    f"{packet.target_name} {packet.packet_name}: unknown channel record, {len(channels)} bytes left"
                )
                break
            subpacket.buffer = channels
            length = record_length(subpacket)
            subpacket.buffer = channels[:length]
            subpacket = subpacket.clone()
            subpacket.received_time = packet.received_time
            packets.append(subpacket)
            channels = channels[length:]
        packets.append(packet)
        return packets


def record_length(subpacket):
    """Bytes of the channel record at the head of the subpacket's buffer

    `defined_length` covers the fixed items only; a channel carrying a VARIABLE_BIT_SIZE string is measured from the
    item offsets COSMOS recalculated when the buffer was assigned.
    """
    end_bits = subpacket.defined_length_bits
    for item in subpacket.sorted_items:
        if item.data_type == "DERIVED" or item.parent_item is not None or item.bit_offset < 0:
            continue
        if item.variable_bit_size:
            bits = subpacket.calculate_total_bit_size(item)
        elif item.array_size is not None:
            bits = item.array_size
        else:
            bits = item.bit_size
        end_bits = max(end_bits, item.bit_offset + bits)
    return (end_bits + 7) // 8
