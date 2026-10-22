"""End-to-end check of the cFS overlay against a running fprime_cfs_reference and COSMOS

Not a pytest module: it needs a live stack. Builds the combined plugin from a cfs-cosmos-plugin tree and the
reference dictionary, installs it into COSMOS, then through the COSMOS API enables TO_LAB output, sends a
standard cFS command (CFE_ES no-op) and an F Prime command (CfsCore.cmdDisp.CMD_NO_OP) over the CFS-1 UDP
interface, and checks that cFS housekeeping and the F Prime Health packet are received and decoded.

    python tests/e2e/cfs_reference.py --cfs-plugin ../cfs-cosmos-plugin \\
        --dictionary ../fprime_cfs_reference/build-artifacts/exe/Linux/fprime_app/dict/*Dictionary.json \\
        [--cosmos-url http://localhost:2900] [--tlm-ip 127.0.0.1] [--skip-install]

Prerequisites: core-cpu1 of fprime_cfs_reference running on this host (CI_LAB on UDP 1234, TO_LAB to UDP 2234),
COSMOS running with UDP 2234 published on openc3-operator and `$OPENC3_API_PASSWORD` set. Exit status 0 on success.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
from pathlib import Path

from fprime_cosmos.cfs import DEFAULT_TARGET_NAME, build_overlay
from fprime_cosmos.cosmos_api import CosmosClient
from fprime_cosmos.dictionary import FprimeDictionary
from fprime_cosmos.install import ensure_installed

CFS_ES_NOOP = "CFE_ES_CMD_NOOP"
CFS_ES_SEND_HK = "CFE_ES_SEND_HK_CMD"
CFS_ES_HK = "CFE_ES_HK"
CFS_ES_HK_MID = 0x0800
TO_LAB_ENABLE = "TO_LAB_CMD_ENABLE_OUTPUT"
TO_LAB_ADD_PACKET = "TO_LAB_CMD_ADD_PACKET"
FPRIME_NOOP = "FPRIMEDEPLOYMENT.CFSCORE.CMDDISP.CMD_NO_OP"
FPRIME_HEALTH = "FPRIMEDEPLOYMENT.HEALTH"
FPRIME_PACKETS = "FPRIMEDEPLOYMENT.FPRIME_PACKETS"
FPRIME_DISPATCHED = "CFSCORE.CMDDISP.COMMANDSDISPATCHED"


class Api:
    """The COSMOS JSON-RPC API (cmd/tlm), authenticated like the plugin installer"""

    def __init__(self, client: CosmosClient):
        self.client = client
        self.client.authenticate()
        self.counter = 0

    def call(self, method: str, *params):
        self.counter += 1
        body = {
            "jsonrpc": "2.0",
            "method": method,
            "params": list(params),
            "id": self.counter,
            "keyword_params": {"scope": self.client.scope},
        }
        response = self.client._request("POST", "/api", json=body)  # noqa: SLF001 - reuse the authenticated session
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(f"{method}{params}: {payload['error']}")
        return payload.get("result")


def check(label: str, condition: bool, detail: str = "") -> bool:
    print(f"[{'PASS' if condition else 'FAIL'}] {label}{(': ' + detail) if detail else ''}")
    return condition


def wait_for(api: Api, target: str, packet: str, minimum: int, timeout: float) -> int:
    deadline = time.monotonic() + timeout
    count = api.call("get_tlm_cnt", target, packet)
    while count < minimum and time.monotonic() < deadline:
        time.sleep(1.0)
        count = api.call("get_tlm_cnt", target, packet)
    return count


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--cfs-plugin", type=Path, required=True)
    parser.add_argument("--dictionary", type=Path, required=True)
    parser.add_argument("--cosmos-url", default="http://localhost:2900")
    parser.add_argument("--cosmos-password", default=os.environ.get("OPENC3_API_PASSWORD"))
    parser.add_argument("--target-name", default=DEFAULT_TARGET_NAME)
    parser.add_argument("--tlm-ip", default="127.0.0.1", help="Where TO_LAB sends telemetry (the COSMOS host)")
    parser.add_argument("--skip-install", action="store_true", help="The combined plugin is already installed")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args(arguments)

    client = CosmosClient(args.cosmos_url, args.cosmos_password)
    if not args.skip_install:
        with tempfile.TemporaryDirectory() as output:
            artifacts = build_overlay(
                args.cfs_plugin, [FprimeDictionary(args.dictionary)], Path(output), channelized=False
            )
            print(f"[INFO] Built {artifacts.gem_path.name}")
            ensure_installed(client, artifacts, {}, args.target_name, force=False)
    api = Api(client)
    target = args.target_name

    results = []
    hk_before = api.call("get_tlm_cnt", target, CFS_ES_HK)
    health_before = api.call("get_tlm_cnt", target, FPRIME_HEALTH)
    api.call("cmd", f"{target} {TO_LAB_ENABLE} with DEST_IP '{args.tlm_ip}'")
    # The reference's TO_LAB subscription table only carries the F Prime packetized telemetry; cFS housekeeping
    # is requested (the reference scheduler only ticks F Prime) and routed to the ground on demand
    api.call("cmd", f"{target} {TO_LAB_ADD_PACKET} with STREAM_VALUE {CFS_ES_HK_MID}, BUF_LIMIT 4")
    api.call("cmd", f"{target} {CFS_ES_NOOP}")
    api.call("cmd", f"{target} {CFS_ES_SEND_HK}")
    hk_count = wait_for(api, target, CFS_ES_HK, hk_before + 1, args.timeout)
    results.append(
        check("standard cFS telemetry decoded", hk_count > hk_before, f"{CFS_ES_HK} count {hk_before} -> {hk_count}")
    )
    es_cmd_counter = api.call("tlm", f"{target} {CFS_ES_HK} COMMAND_COUNTER")
    results.append(
        check("standard cFS command counted", es_cmd_counter is not None, f"COMMAND_COUNTER = {es_cmd_counter}")
    )

    dispatched_before = api.call("tlm", f"{target} {FPRIME_HEALTH} {FPRIME_DISPATCHED}") or 0
    api.call("cmd", f"{target} {FPRIME_NOOP}")
    deadline = time.monotonic() + args.timeout
    dispatched = dispatched_before
    while dispatched == dispatched_before and time.monotonic() < deadline:
        time.sleep(1.0)
        dispatched = api.call("tlm", f"{target} {FPRIME_HEALTH} {FPRIME_DISPATCHED}") or 0
    results.append(
        check(
            "F Prime command round trip",
            dispatched == dispatched_before + 1,
            f"{FPRIME_DISPATCHED} {dispatched_before} -> {dispatched}",
        )
    )
    # The reference packetizes Health on change, so the packet count moves with the command above
    health_count = wait_for(api, target, FPRIME_HEALTH, health_before + 1, args.timeout)
    results.append(
        check(
            "F Prime packetized telemetry decoded",
            health_count > health_before,
            f"{FPRIME_HEALTH} count {health_before} -> {health_count}",
        )
    )
    descriptor = api.call("tlm", f"{target} {FPRIME_HEALTH} FPRIME_DESCRIPTOR")
    stream = api.call("tlm", f"{target} {FPRIME_PACKETS} CCSDS_STREAMID")
    results.append(
        check(
            "F Prime header items",
            descriptor == 32 and stream == 0x0820,
            f"descriptor {descriptor}, stream id {stream}",
        )
    )
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
