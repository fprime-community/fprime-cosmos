"""Regenerate the golden files from the reference dictionaries: python tests/update_golden.py"""

from pathlib import Path

from fprime_cosmos import cfs, emit
from fprime_cosmos.dictionary import FprimeDictionary

HERE = Path(__file__).parent

if __name__ == "__main__":
    dictionary = FprimeDictionary(HERE / "data" / "ReferenceDeploymentTopologyDictionary.json")
    golden = HERE / "golden"
    golden.mkdir(exist_ok=True)
    (golden / "commands.txt").write_text(emit.emit_commands(dictionary))
    (golden / "channels.txt").write_text(emit.emit_channels(dictionary))
    (golden / "packets.txt").write_text(emit.emit_packets(dictionary))
    app = cfs.cfs_app(FprimeDictionary(HERE / "data" / "FPrimeDeploymentTopologyDictionary.json"))
    commands, telemetry = cfs.definition_files(app)
    (golden / "cfs").mkdir(exist_ok=True)
    (golden / "cfs" / commands).write_text(cfs.emit_commands(app))
    (golden / "cfs" / telemetry).write_text(cfs.emit_telemetry(app))
