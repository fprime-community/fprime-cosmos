# Derived from openc3-cosmos-fprime lib/fprime_subpacketizer.py
# Copyright 2026 OpenC3, Inc. Licensed under the MIT License; see the NOTICE file
# in the fprime-cosmos distribution for the full license text.
#
# Resolves the F Prime packet carried by a cFS-hosted F Prime parent packet into COSMOS subpackets
#
# The parent packets are identified by their cFS message id alone (like every packet of the cFS target) and
# carry the F Prime packet as a block:
# - <app>.FPRIME_CHANNELS: F Prime channelized telemetry; the CHANNELS block holds concatenated channel
#   records (id, time, value), each declared as a SUBPACKET keyed on the channel id (FPRIME_CHANNEL_ID).
# - <app>.FPRIME_PACKETS: F Prime packetized telemetry; the PACKET block holds one F Prime packet, declared
#   as a SUBPACKET keyed on the descriptor and packet id (FPRIME_PACKET_ID).
# Candidate subpackets are those of the same target whose names carry the parent's application prefix and
# which declare the key item, so several F Prime applications hosted in one cFS target never claim each
# other's identifiers. Ruby port for the (Ruby) cFS target of nasa/cfs-cosmos-plugin.

require 'openc3/subpacketizers/subpacketizer'
require 'openc3/system/system'
require 'openc3/utilities/logger'

module OpenC3
  class FprimeSubpacketizer < Subpacketizer
    PARENTS = {
      'FPRIME_CHANNELS' => ['CHANNELS', 'FPRIME_CHANNEL_ID'],
      'FPRIME_PACKETS' => ['PACKET', 'FPRIME_PACKET_ID']
    }.freeze

    def call(packet)
      kind = PARENTS.keys.find { |name| packet.packet_name.end_with?(name) }
      return [packet] unless kind

      block, key_item = PARENTS[kind]
      prefix = packet.packet_name[0...-kind.length]
      candidates = System.telemetry.packets(packet.target_name).values.select do |candidate|
        candidate.subpacket && candidate.packet_name.start_with?(prefix) && candidate.items.key?(key_item)
      end
      packets = []
      data = packet.read(block)
      while data && !data.empty?
        subpacket = candidates.find { |candidate| candidate.identify?(data) }
        if subpacket.nil?
          Logger.warn("#{packet.target_name} #{packet.packet_name}: unknown F Prime record, #{data.length} bytes left")
          break
        end
        subpacket = subpacket.clone
        subpacket.buffer = data
        length = record_length(subpacket)
        subpacket.buffer = data[0, length]
        subpacket.received_time = packet.received_time
        packets << subpacket
        data = data[length..-1]
      end
      packets << packet
      packets
    end

    # Bytes of the F Prime record at the head of the subpacket's buffer
    #
    # defined_length covers the fixed items only; a record carrying a VARIABLE_BIT_SIZE string is measured from
    # the item offsets COSMOS recalculated when the buffer was assigned.
    def record_length(subpacket)
      end_bits = subpacket.defined_length_bits
      subpacket.sorted_items.each do |item|
        next if item.data_type == :DERIVED || !item.parent_item.nil? || item.bit_offset < 0

        bits = if item.variable_bit_size
                 subpacket.calculate_total_bit_size(item)
               elsif item.array_size
                 item.array_size
               else
                 item.bit_size
               end
        end_bits = [end_bits, item.bit_offset + bits].max
      end
      (end_bits + 7) / 8
    end
  end
end
