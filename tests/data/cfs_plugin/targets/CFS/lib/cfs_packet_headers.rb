def cfs_cmd_hdr(target_name, cmd_name, func_code, pkt_desc)
    "COMMAND #{target_name} #{cmd_name} LITTLE_ENDIAN \"#{pkt_desc}\""
end
