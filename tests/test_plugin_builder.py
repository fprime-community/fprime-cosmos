"""The plugin builder writes a complete COSMOS plugin tree and gem"""

import gzip
import tarfile

from fprime_cosmos.plugin_builder import build_plugin, gem_name_for, plugin_digest, slug, version_for


def test_names(reference_dictionary):
    assert slug("Fprime Yamcs.Reference!") == "fprime-yamcs-reference"
    assert gem_name_for(reference_dictionary) == "openc3-cosmos-fprime-yamcsdeployment"


def test_version_tracks_plugin_tree(reference_dictionary, tmp_path):
    first = build_plugin(reference_dictionary, tmp_path / "a", gem=False)
    again = build_plugin(reference_dictionary, tmp_path / "b", gem=False)
    assert first.version == again.version == f"1.0.0.{plugin_digest(first.directory)}"
    (first.directory / "targets" / "FPRIME" / "lib" / "fprime_subpacketizer.py").write_text("changed")
    assert version_for(first.directory) != again.version


def test_build_plugin_tree(reference_dictionary, tmp_path):
    artifacts = build_plugin(reference_dictionary, tmp_path, gem=False)
    root = artifacts.directory
    assert artifacts.gem_path is None
    assert (root / "plugin.txt").read_text().count("VARIABLE fprime_target_name FPRIME") == 1
    assert (root / "targets" / "FPRIME" / "target.txt").read_text().strip() == "LANGUAGE python"
    assert (root / "targets" / "FPRIME" / "lib" / "fprime_subpacketizer.py").is_file()
    assert sorted(p.name for p in (root / "targets" / "FPRIME" / "cmd_tlm").iterdir()) == [
        "channels.txt",
        "commands.txt",
        "packets.txt",
    ]


def test_build_plugin_custom_target_and_gem(reference_dictionary, tmp_path):
    artifacts = build_plugin(reference_dictionary, tmp_path, target_name="REF")
    assert (artifacts.directory / "targets" / "REF" / "cmd_tlm" / "commands.txt").is_file()
    plugin_txt = (artifacts.directory / "plugin.txt").read_text()
    assert "TARGET REF <%= fprime_target_name %>" in plugin_txt
    assert "VARIABLE fprime_target_name REF" in plugin_txt
    assert artifacts.gem_path.name == f"{artifacts.plugin_prefix}.gem"
    with tarfile.open(artifacts.gem_path) as gem:
        metadata = gzip.decompress(gem.extractfile("metadata.gz").read()).decode()
    assert f'fprime_dictionary_hash: "{reference_dictionary.content_hash()}"' in metadata
    assert '- "targets/REF/lib/fprime_subpacketizer.py"' in metadata


def test_rebuild_replaces_previous_output(reference_dictionary, tmp_path):
    first = build_plugin(reference_dictionary, tmp_path, gem=False)
    stale = first.directory / "targets" / "FPRIME" / "cmd_tlm" / "stale.txt"
    stale.write_text("x")
    build_plugin(reference_dictionary, tmp_path, gem=False)
    assert not stale.exists()
