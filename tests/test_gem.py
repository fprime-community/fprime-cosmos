"""The pure-Python gem builder produces archives RubyGems can read"""

import gzip
import shutil
import subprocess
import tarfile

import pytest

from fprime_cosmos.gem import GemSpec, build_gem, yaml_string


@pytest.fixture
def gem(tmp_path):
    source = tmp_path / "src"
    (source / "targets" / "T" / "cmd_tlm").mkdir(parents=True)
    (source / "plugin.txt").write_text("TARGET T T\n")
    (source / "targets" / "T" / "cmd_tlm" / "cmd.txt").write_text('COMMAND T A BIG_ENDIAN "a"\n')
    (source / "__pycache__").mkdir()
    (source / "__pycache__" / "x.pyc").write_bytes(b"\x00")
    spec = GemSpec(
        "openc3-cosmos-test",
        "1.0.0.abc",
        "Summary",
        'Desc "quoted"',
        metadata={"openc3_cosmos_minimum_version": "6.10.0"},
    )
    return build_gem(spec, source, tmp_path / "out" / "test.gem")


def test_gem_structure(gem):
    with tarfile.open(gem) as archive:
        names = archive.getnames()
        assert names == ["metadata.gz", "data.tar.gz", "checksums.yaml.gz"]
        metadata = gzip.decompress(archive.extractfile("metadata.gz").read()).decode()
        with tarfile.open(fileobj=archive.extractfile("data.tar.gz"), mode="r:gz") as data:
            data_names = sorted(data.getnames())
    assert "name: openc3-cosmos-test" in metadata
    assert "  version: 1.0.0.abc" in metadata
    assert 'openc3_cosmos_minimum_version: "6.10.0"' in metadata
    assert '- "plugin.txt"' in metadata
    assert data_names == ["plugin.txt", "targets/T/cmd_tlm/cmd.txt"]


@pytest.mark.skipif(shutil.which("gem") is None, reason="RubyGems not installed")
def test_rubygems_reads_gem(gem):
    output = subprocess.run(
        ["gem", "specification", str(gem), "files"], check=True, capture_output=True, text=True
    ).stdout
    assert "plugin.txt" in output and "targets/T/cmd_tlm/cmd.txt" in output
    metadata = subprocess.run(
        ["gem", "specification", str(gem), "metadata"], check=True, capture_output=True, text=True
    ).stdout
    assert "openc3_cosmos_minimum_version: 6.10.0" in metadata


def test_yaml_string_escapes():
    assert yaml_string('a "b"\nc\\') == '"a \\"b\\" c\\\\"'
