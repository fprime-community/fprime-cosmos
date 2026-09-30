"""Builds a RubyGems `.gem` archive without Ruby

A gem is a tar archive holding `metadata.gz` (a YAML Gem::Specification), `data.tar.gz` (the files) and
`checksums.yaml.gz`. COSMOS only needs the specification fields rendered here, so a fixed template is
sufficient and users do not need a Ruby toolchain to package the generated plugin.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import tarfile
import time
from dataclasses import dataclass, field
from pathlib import Path

METADATA_TEMPLATE = """--- !ruby/object:Gem::Specification
name: {name}
version: !ruby/object:Gem::Version
  version: {version}
platform: ruby
authors:
- {author}
autorequire:
bindir: bin
cert_chain: []
date: {date} 00:00:00.000000000 Z
dependencies: []
description: {description}
email:
- {email}
executables: []
extensions: []
extra_rdoc_files: []
files:
{files}
homepage: {homepage}
licenses:
- {license}
metadata:
{metadata}
post_install_message:
rdoc_options: []
require_paths:
- lib
required_ruby_version: !ruby/object:Gem::Requirement
  requirements:
  - - ">="
    - !ruby/object:Gem::Version
      version: '3.0'
required_rubygems_version: !ruby/object:Gem::Requirement
  requirements:
  - - ">="
    - !ruby/object:Gem::Version
      version: '0'
requirements: []
rubygems_version: 3.3.5
signing_key:
specification_version: 4
summary: {summary}
test_files: []
"""


@dataclass(frozen=True)
class GemSpec:
    name: str
    version: str
    summary: str
    description: str
    author: str = "fprime-cosmos"
    email: str = "fprime@jpl.nasa.gov"
    homepage: str = "https://github.com/fprime-community/fprime-cosmos"
    license: str = "Apache-2.0"
    metadata: dict[str, str] = field(default_factory=dict)


def yaml_string(value: str) -> str:
    """Quote a scalar so that YAML reads it back verbatim"""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
    return f'"{escaped}"'


def render_metadata(spec: GemSpec, files: list[str], date: str) -> str:
    return METADATA_TEMPLATE.format(
        name=spec.name,
        version=spec.version,
        author=yaml_string(spec.author),
        date=date,
        description=yaml_string(spec.description),
        email=yaml_string(spec.email),
        files="\n".join(f"- {yaml_string(name)}" for name in files),
        homepage=yaml_string(spec.homepage),
        license=spec.license,
        metadata="\n".join(f"  {key}: {yaml_string(value)}" for key, value in spec.metadata.items()),
        summary=yaml_string(spec.summary),
    )


def _gzip(data: bytes, mtime: int) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=mtime) as archive:
        archive.write(data)
    return buffer.getvalue()


def _tar_entry(archive: tarfile.TarFile, name: str, data: bytes, mode: int, mtime: int) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mode = mode
    info.mtime = mtime
    info.uname = info.gname = "wheel"
    archive.addfile(info, io.BytesIO(data))


def build_gem(spec: GemSpec, source: Path, destination: Path) -> Path:
    """Package every file under source into a gem at destination"""
    source = Path(source)
    destination = Path(destination)
    mtime = int(time.time())
    date = time.strftime("%Y-%m-%d", time.gmtime(mtime))
    paths = sorted(path for path in source.rglob("*") if path.is_file() and "__pycache__" not in path.parts)
    names = [path.relative_to(source).as_posix() for path in paths]

    data_buffer = io.BytesIO()
    with tarfile.open(fileobj=data_buffer, mode="w", format=tarfile.USTAR_FORMAT) as data_tar:
        for path, name in zip(paths, names, strict=True):
            _tar_entry(data_tar, name, path.read_bytes(), 0o644, mtime)
    data_gz = _gzip(data_buffer.getvalue(), mtime)
    metadata_gz = _gzip(render_metadata(spec, names, date).encode(), mtime)
    digests = {"SHA256": hashlib.sha256, "SHA512": hashlib.sha512}
    checksums = "---\n" + "".join(
        f"{name}:\n  metadata.gz: {digest(metadata_gz).hexdigest()}\n  data.tar.gz: {digest(data_gz).hexdigest()}\n"
        for name, digest in digests.items()
    )
    checksums_gz = _gzip(checksums.encode(), mtime)

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(destination, mode="w", format=tarfile.USTAR_FORMAT) as gem:
        _tar_entry(gem, "metadata.gz", metadata_gz, 0o444, mtime)
        _tar_entry(gem, "data.tar.gz", data_gz, 0o444, mtime)
        _tar_entry(gem, "checksums.yaml.gz", checksums_gz, 0o444, mtime)
    return destination
