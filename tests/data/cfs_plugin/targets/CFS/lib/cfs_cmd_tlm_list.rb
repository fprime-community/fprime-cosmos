# Trimmed stand-in for the cFS message list (test fixture): cpu 1 base stream identifiers
$CFS_CMD_TLM_LIST = {
    CFE_ES_CMD: FswMsgInfo.new(
        base_stream_id: 0x1806,
        packet_names: ["CFE_ES_CMD_NOOP"]
    ),
    CFE_ES_HK_TLM: FswMsgInfo.new(
        base_stream_id: 0x0800,
        packet_names: ["CFE_ES_HK_TLM"]
    ),
    CFE_EVS_HK_TLM: FswMsgInfo.new(
        base_stream_id: 0x0801,
        packet_names: ["CFE_EVS_HK_TLM"]
    ),
}
