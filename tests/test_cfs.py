"""cFS overlay: identifiers from ComCfg, collision checks, staging of the cFS plugin and the generated files"""

import gzip
import tarfile
from dataclasses import replace
from pathlib import Path

import pytest

from fprime_cosmos import cfs
from fprime_cosmos.dictionary import DictionaryError
from fprime_cosmos.generate.cfs import main as cfs_main
from tests.conftest import CFS_DICTIONARY, CFS_PLUGIN, REFERENCE_DICTIONARY

GOLDEN = Path(__file__).parent / "golden" / "cfs"


def test_identifiers_from_comcfg(cfs_dictionary):
    app = cfs.cfs_app(cfs_dictionary)
    assert app.name == "FPrimeDeployment"
    assert app.slug == "fprimedeployment"
    assert (app.command_mid, app.channels_mid, app.packets_mid, app.function_code) == (0x1800, 0x0801, 0x0820, 0)
    assert cfs.definition_files(app) == ("fprime_fprimedeployment_cmd_def.txt", "fprime_fprimedeployment_tlm_def.txt")


def test_dictionary_without_cfs_constants_is_rejected(reference_dictionary):
    with pytest.raises(DictionaryError, match="ComCfg"):
        cfs.cfs_app(reference_dictionary)


def test_bad_apid_and_function_code(cfs_dictionary):
    app = cfs.cfs_app(cfs_dictionary)
    original = cfs_dictionary.apids, cfs_dictionary.cfs_function_code
    try:
        cfs_dictionary.apids = {**original[0], cfs.PACKETS_APID: 0x800}
        with pytest.raises(DictionaryError, match="11 bit APID"):
            cfs.cfs_app(cfs_dictionary)
        cfs_dictionary.apids = {key: value for key, value in original[0].items() if key != cfs.COMMAND_APID}
        with pytest.raises(DictionaryError, match="lacks FW_PACKET_COMMAND"):
            cfs.cfs_app(cfs_dictionary)
        cfs_dictionary.apids = original[0]
        cfs_dictionary.cfs_function_code = None
        with pytest.raises(DictionaryError, match="FprimeCommandFunctionCode"):
            cfs.cfs_app(cfs_dictionary)
        cfs_dictionary.cfs_function_code = 0x80
        with pytest.raises(DictionaryError, match="7 bits"):
            cfs.cfs_app(cfs_dictionary)
    finally:
        cfs_dictionary.apids, cfs_dictionary.cfs_function_code = original
    assert app == cfs.cfs_app(cfs_dictionary)


def test_golden(cfs_dictionary):
    app = cfs.cfs_app(cfs_dictionary)
    commands, telemetry = cfs.definition_files(app)
    assert cfs.emit_commands(app) == (GOLDEN / commands).read_text()
    assert cfs.emit_telemetry(app) == (GOLDEN / telemetry).read_text()


def test_telemetry_layout(cfs_dictionary):
    app = cfs.cfs_app(cfs_dictionary)
    telemetry = cfs.emit_telemetry(app)
    assert "SPARE_FOR_64_ALIGN" not in telemetry
    assert "CATCHALL" not in telemetry
    assert telemetry.count("SUBPACKETIZER fprime_subpacketizer.rb") == 2
    assert "unix_time_conversion.py" not in telemetry
    # Every top-level packet identifies on CCSDS_STREAMID alone, keeping the cFS target in its shared ID mode
    blocks = [block for block in telemetry.split("\n\n") if block.startswith("TELEMETRY")]
    top_level = [block for block in blocks if "\n  SUBPACKET\n" not in block]
    assert len(top_level) == 2 and len(blocks) == 2 + len(app.dictionary.packets) + len(app.dictionary.channels)
    for block in top_level:
        ids = [line.split()[1] for line in block.splitlines() if line.startswith("  APPEND_ID_ITEM")]
        assert ids == ["CCSDS_STREAMID"]
    without_channels = cfs.emit_telemetry(app, channelized=False)
    assert "FPRIME_CHANNELS" not in without_channels and "FPRIME_CHANNEL_ID" not in without_channels
    assert "TELEMETRY <%= target_name %> FPrimeDeployment.FPRIME_PACKETS BIG_ENDIAN" in without_channels
    assert "TELEMETRY <%= target_name %> FPrimeDeployment.Health BIG_ENDIAN" in without_channels
    assert "  APPEND_ID_ITEM FPRIME_PACKET_ID 16 UINT 1" in without_channels
    header = cfs.emit_command(app.dictionary.commands[0], app)[:7]
    assert [line.split()[1] for line in header[1:]] == [
        "CCSDS_STREAMID",
        '"0x%04X"',
        "CCSDS_SEQUENCE",
        "CCSDS_LENGTH",
        "CCSDS_FC",
        "CCSDS_CHECKSUM",
    ]


def test_cfs_plugin_message_ids():
    assert cfs.cfs_plugin_cpus(CFS_PLUGIN) == 3
    ids = cfs.cfs_plugin_message_ids(CFS_PLUGIN)
    assert ids == {
        0x1806: "targets/CFS/lib/cfs_cmd_tlm_list.rb:4 (CFS-1)",
        0x1906: "targets/CFS/lib/cfs_cmd_tlm_list.rb:4 (CFS-2)",
        0x1A06: "targets/CFS/lib/cfs_cmd_tlm_list.rb:4 (CFS-3)",
        0x0800: "targets/CFS/lib/cfs_cmd_tlm_list.rb:8 (CFS-1)",
        0x0900: "targets/CFS/lib/cfs_cmd_tlm_list.rb:8 (CFS-2)",
        0x0A00: "targets/CFS/lib/cfs_cmd_tlm_list.rb:8 (CFS-3)",
        0x0801: "targets/CFS/lib/cfs_cmd_tlm_list.rb:12 (CFS-1)",
        0x0901: "targets/CFS/lib/cfs_cmd_tlm_list.rb:12 (CFS-2)",
        0x0A01: "targets/CFS/lib/cfs_cmd_tlm_list.rb:12 (CFS-3)",
    }
    assert cfs.cfs_plugin_message_ids(Path("/nonexistent")) == {}
    assert cfs.cfs_plugin_cpus(Path("/nonexistent")) == 2


def test_collisions(cfs_dictionary):
    app = cfs.cfs_app(cfs_dictionary)
    with pytest.raises(cfs.CfsOverlayError, match=r"0x0801 .*already defined by the cFS plugin at .*:12 \(CFS-1\)"):
        cfs.check_collisions([app], CFS_PLUGIN)
    cfs.check_collisions([app], CFS_PLUGIN, channelized=False)
    with pytest.raises(cfs.CfsOverlayError, match="Application name FPrimeDeployment is used by both"):
        cfs.check_collisions([app, app], CFS_PLUGIN, channelized=False)
    other = replace(app, name="Other")
    with pytest.raises(cfs.CfsOverlayError, match="Message id 0x1800 collides"):
        cfs.check_collisions([app, other], CFS_PLUGIN, channelized=False)
    disjoint = replace(app, name="Other", command_mid=0x1890, packets_mid=0x0890)
    cfs.check_collisions([app, disjoint], CFS_PLUGIN, channelized=False)


def test_write_overlay_preserves_plugin(cfs_dictionary, tmp_path):
    app = cfs.cfs_app(cfs_dictionary)
    out = cfs.write_overlay(CFS_PLUGIN, [app], tmp_path / "plugin", channelized=False)
    assert (out / "plugin.txt").read_bytes() == (CFS_PLUGIN / "plugin.txt").read_bytes()
    assert sorted(p.name for p in out.iterdir()) == [
        "LICENSE.txt",
        "README.md",
        "Rakefile",
        "lib",
        "plugin.txt",
        "requirements.txt",
        "targets",
    ]
    assert (out / "lib" / "cfs_helper.py").is_file()
    assert sorted(p.name for p in (out / "targets" / "CFS" / "cmd_tlm").iterdir()) == [
        "cfe_es_cmd_def.txt",
        "cfe_es_tlm_def.txt",
        "fprime_fprimedeployment_cmd_def.txt",
        "fprime_fprimedeployment_tlm_def.txt",
    ]
    assert sorted(p.name for p in (out / "targets" / "CFS" / "lib").iterdir()) == [
        "cfs_cmd_tlm_list.rb",
        "cfs_globals.rb",
        "cfs_packet_headers.rb",
        "fprime_subpacketizer.rb",
    ]
    assert (
        out / "targets" / "CFS" / "cmd_tlm" / "fprime_fprimedeployment_tlm_def.txt"
    ).read_text() == cfs.emit_telemetry(app, channelized=False)


def test_write_overlay_rejects_non_plugin(cfs_dictionary, tmp_path):
    app = cfs.cfs_app(cfs_dictionary)
    with pytest.raises(cfs.CfsOverlayError, match="no plugin.txt"):
        cfs.write_overlay(tmp_path, [app], tmp_path / "out")
    (tmp_path / "plugin.txt").write_text("TARGET X X\n")
    with pytest.raises(cfs.CfsOverlayError, match="targets/CFS/cmd_tlm"):
        cfs.write_overlay(tmp_path, [app], tmp_path / "out")


def test_build_overlay_gem(cfs_dictionary, tmp_path):
    artifacts = cfs.build_overlay(CFS_PLUGIN, [cfs_dictionary], tmp_path, channelized=False)
    assert artifacts.gem_name == "openc3-cosmos-cfs"
    assert artifacts.directory == tmp_path / "openc3-cosmos-cfs"
    assert artifacts.gem_path == tmp_path / f"openc3-cosmos-cfs-{artifacts.version}.gem"
    with tarfile.open(artifacts.gem_path) as gem:
        metadata = gzip.decompress(gem.extractfile("metadata.gz").read()).decode()
        with tarfile.open(fileobj=gem.extractfile("data.tar.gz"), mode="r:gz") as data:
            names = data.getnames()
    assert "name: openc3-cosmos-cfs" in metadata
    assert f'fprime_dictionary_hash: "{cfs_dictionary.content_hash()}"' in metadata
    assert "targets/CFS/cmd_tlm/fprime_fprimedeployment_cmd_def.txt" in names
    assert "targets/CFS/cmd_tlm/cfe_es_cmd_def.txt" in names and "plugin.txt" in names
    assert not any(name.startswith(("docs/", "Makefile")) for name in names)
    again = cfs.build_overlay(CFS_PLUGIN, [cfs_dictionary], tmp_path / "again", channelized=False, gem=False)
    assert again.version == artifacts.version and again.gem_path is None


def test_build_overlay_requires_dictionaries(tmp_path):
    with pytest.raises(cfs.CfsOverlayError, match="At least one"):
        cfs.build_overlay(CFS_PLUGIN, [], tmp_path)


def test_cli(tmp_path, capsys):
    base = ["--cfs-plugin", str(CFS_PLUGIN), "--dictionary", str(CFS_DICTIONARY), "-o", str(tmp_path)]
    assert cfs_main([*base, "--no-gem", "--no-channelized"]) == 0
    out = capsys.readouterr().out
    assert "Plugin written" in out and "Gem written" not in out
    assert (
        tmp_path / "openc3-cosmos-cfs" / "targets" / "CFS" / "cmd_tlm" / "fprime_fprimedeployment_cmd_def.txt"
    ).is_file()
    assert cfs_main(base) == 1
    assert "[ERROR] Message id 0x0801" in capsys.readouterr().err
    assert cfs_main([*base, "--dictionary", str(REFERENCE_DICTIONARY), "--no-channelized"]) == 1
    assert "[ERROR] ReferenceDeploymentTopologyDictionary.json" in capsys.readouterr().err
    assert (
        cfs_main(["--cfs-plugin", str(tmp_path / "nope"), "--dictionary", str(CFS_DICTIONARY), "-o", str(tmp_path)])
        == 1
    )
    assert "no plugin.txt" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        cfs_main([*base, "--no-gem", "--install"])
