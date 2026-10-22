# fprime-cosmos: Tools to Make Open C3 COSMOS Integration Easier

Runs an [F Prime](https://github.com/nasa/fprime) deployment against [OpenC3 COSMOS](https://openc3.com).

`fprime-cosmos` reads the F Prime JSON topology dictionary, generates a native COSMOS plugin (commands,
channelized telemetry and packetized telemetry), installs it into a running COSMOS through the plugin API,
and starts `fprime-comm-bridge` (from `fprime-gds`) to move packets between the flight software and COSMOS.
No Ruby toolchain, no hand-edited plugin files and no knowledge of COSMOS internals are required.

```text
F Prime deployment  <-- framing / transport -->  fprime-comm-bridge  <-- UDP, F Prime packets -->  COSMOS
                                                 (owns framing)                                    (this plugin)
```

## Installation

Requires Python 3.10 or newer and COSMOS 6.10.0 or newer (the generated gem declares
`openc3_cosmos_minimum_version`). The local-Docker defaulting described under Options uses the `docker` and `ip`
command-line tools.

```bash
pip install "git+https://github.com/fprime-community/fprime-cosmos.git"
```

This pulls in `fprime-gds`, which provides the dictionary loaders and `fprime-comm-bridge`. The bridge ships in
`fprime-gds` 4.4.0a4 and later (pre-releases, selected automatically because the dependency floor names one); to use
the development branch instead:

```bash
pip install "git+https://github.com/nasa/fprime-gds.git@devel"
```

Prefer `$OPENC3_API_PASSWORD` over `--cosmos-password`; command-line arguments are visible to other users of the
machine. The default `--cosmos-url` is plain HTTP on `localhost`; use an `https://` URL for a remote COSMOS.

## Quick start

1. Start COSMOS (for example the [cosmos-project](https://github.com/OpenC3/cosmos-project) with
   `./openc3.sh run`). COSMOS must be able to receive UDP telemetry on port 50000 from the bridge, so publish
   that port on the `openc3-operator` container (add to `compose.yaml`/an override file):

   ```yaml
   services:
     openc3-operator:
       ports:
         - "127.0.0.1:50000:50000/udp"
   ```

2. Run the launcher from the deployment's F Prime project (the same command line as `fprime-gds` and
   `fprime-yamcs`; the deployment, dictionary and binary are detected from `settings.ini`/`build-artifacts`):

   ```bash
   export OPENC3_API_PASSWORD=...
   fprime-cosmos
   ```

   The launcher

   - generates the plugin into `./openc3-plugin/` and packages it as a gem,
   - installs (or upgrades) the plugin in COSMOS, skipping the install when the same plugin is already
     installed (a digest of the generated plugin is part of the gem version),
   - starts `fprime-comm-bridge` with the selected communication adapter (default `tcp-fast-server`) and framing
     (default `space-packet-space-data-link`),
   - starts the deployment once the bridge accepts connections (`-a 127.0.0.1 -p 50000` by default; `--application-arguments`
     overrides, `-n` skips it), logging it under `logs/fprime-cosmos-<date>/`. Everything is stopped when the
     bridge exits or on Ctrl-C.

   The fprime-gds dictionary, deployment, logging, communication and framing options apply, so the flight link is
   configured exactly as for the GDS, for example:

   ```bash
   fprime-cosmos -d build-artifacts/Linux/MyDeployment              # explicit deployment directory
   fprime-cosmos --dictionary MyDeploymentTopologyDictionary.json -n  # deployment started elsewhere
   fprime-cosmos --communication-selection tcp-fast-client --tcp-fast-address 192.168.1.10
   fprime-cosmos --framing-selection fprime                          # deployment uses F Prime framing
   fprime-cosmos --communication-selection none -n                   # generate and install only
   ```

3. Open COSMOS: the `FPRIME` target appears with one command per F Prime command, one telemetry packet per
   channel and one per telemetry packet-set entry.

### Options

Shared with fprime-gds (`fprime-cosmos --help` lists them all, including the `fprime-gds.yml` configuration file):

| Option | Purpose |
| --- | --- |
| `-d`, `--deployment DIR` | Deployment build-artifacts directory (default: detected from `settings.ini`) |
| `--dictionary PATH` | Dictionary (default: detected from the deployment) |
| `--packet-set-name NAME` | Packet set to use when the dictionary defines several |
| `--app PATH`, `-n`, `--no-app` | Deployment binary to start (default: detected), or do not start one |
| `--application-arguments ...` | Arguments for the deployment (default `-a <bridge address> -p <bridge port>`) |
| `-l`, `--logs DIR` | Log directory (default `./logs`) |
| `--communication-selection`, `--tcp-fast-*`, ... | Bridge link to the flight software (default `tcp-fast-server` on port 50000) |
| `--framing-selection`, `--frame-size`, `--scid`, ... | Bridge framing (default `space-packet-space-data-link`) |
| `--udp-fast-*` | Bridge link to COSMOS (see below) |

COSMOS specific:

| Option | Purpose |
| --- | --- |
| `--target-name NAME` | COSMOS target name (default `FPRIME`) |
| `--cosmos-plugin-dir DIR` | Where the plugin and gem are generated (default `./openc3-plugin`) |
| `--cosmos-url URL` | COSMOS base URL (default `http://localhost:2900`) |
| `--cosmos-password`, `$OPENC3_API_PASSWORD` | COSMOS password (set on first use if COSMOS has none) |
| `--cosmos-scope SCOPE` | COSMOS scope (default `DEFAULT`) |
| `--cosmos-variable NAME=VALUE` | Override a plugin variable (see below) |
| `--force-install` | Reinstall even when the generated plugin digest matches |
| `--skip-install` | Generate the plugin but do not install it |

Plugin variables (all defaults match the bridge defaults):

| Variable | Default | Meaning |
| --- | --- | --- |
| `fprime_target_name` | `FPRIME` | COSMOS target name |
| `fprime_bridge_host` | `host.docker.internal` | Bridge host as seen from the COSMOS containers |
| `fprime_bridge_port` | `50001` | Bridge `--udp-fast-recv-port` (commands) |
| `fprime_cosmos_port` | `50000` | Bridge `--udp-fast-send-port` (telemetry) |

Commands reach the bridge from the COSMOS container addresses. When COSMOS is local, `fprime-cosmos` reads
those addresses from `docker ps`/`docker inspect` (plus the Docker bridge gateways) and starts the bridge with
`--udp-fast-bind-address <docker0 address> --udp-fast-allowed-source <addresses>`, i.e. the bridge listens only
on the address the containers know as `host.docker.internal` and only accepts datagrams from them (the bridge
command port is otherwise unauthenticated). When no `docker0` interface is visible on the host (for example rootless
Docker) the launcher binds `0.0.0.0` instead and relies on the allow-list alone. Restart the launcher if the containers
are recreated with new addresses. Setting either option yourself disables this defaulting, for example
`--udp-fast-bind-address 192.168.1.5 --udp-fast-allowed-source 192.168.1.20` for a remote COSMOS (and set
`--cosmos-variable fprime_bridge_host=192.168.1.5` so COSMOS sends commands to that address).

One plugin serves one COSMOS target: installing a dictionary for a target that another F Prime plugin already
serves upgrades that plugin in place (COSMOS forbids two plugins defining the same target). Use
`--target-name` to run several deployments side by side.

### Generating the plugin on its own

```bash
fprime-to-cosmos [--dictionary PATH | -d DEPLOYMENT] -o openc3-plugin [--target-name NAME] [--no-gem] [--install]
```

The dictionary is identified as for the launcher (detected from the F Prime project when neither option is
given). By default this only writes `openc3-plugin/openc3-cosmos-fprime-<deployment>/` and the corresponding `.gem`,
which can be installed through the COSMOS Admin tool or `openc3cli load`. With `--install` the gem is also
installed into COSMOS (same `--cosmos-*`/`--force-install` options as the launcher).

## F Prime applications hosted in cFS

[fprime_cfs](https://github.com/fprime-community/fprime_cfs) runs F Prime deployments as cFS applications. Their
packets travel on the cFS software bus as CCSDS space packets and reach the ground through the ordinary cFS
command ingest / telemetry output applications, so the ground side is the
[nasa/cfs-cosmos-plugin](https://github.com/nasa/cfs-cosmos-plugin), not the native plugin above.
`fprime-to-cosmos-cfs` keeps that plugin authoritative: it copies the plugin tree (a source checkout or the
directory its `make gem` stages) untouched, adds one command and one telemetry definition file per F Prime
application to its `CFS` target plus the Ruby subpacketizer those definitions use, and packages the combined tree
as a single `openc3-cosmos-cfs` gem. Nothing of the cFS plugin is regenerated or replaced.

```bash
fprime-to-cosmos-cfs --cfs-plugin path/to/cfs-cosmos-plugin \
    --dictionary build-artifacts/exe/Linux/fprime_app/dict/FPrimeDeploymentTopologyDictionary.json \
    [--dictionary another/AppTopologyDictionary.json ...] [--no-channelized] -o openc3-plugin [--install]
```

Per dictionary the tool derives, and refuses to proceed without:

- the command message id `0x1800 | ComCfg.Apid.FW_PACKET_COMMAND` (packet type command, secondary header flag),
- the telemetry message ids `0x0800 | ComCfg.Apid.FW_PACKET_TELEM` (channelized) and
  `0x0800 | ComCfg.Apid.FW_PACKET_PACKETIZED_TLM` (packetized),
- the command function code `ComCfg.FprimeCommandFunctionCode`.

Message ids must be unique across the F Prime applications and distinct from the cpu 1 base stream ids of the
cFS plugin's own message list (`targets/CFS/lib/cfs_cmd_tlm_list.rb`); collisions are reported with both owners.
`--no-channelized` omits the channelized (TlmChan) definitions for deployments that only downlink packetized
telemetry, such as `fprime_cfs_reference`, whose `FW_PACKET_TELEM` id `0x0801` is a cFE housekeeping packet.

The generated definitions carry the cFS plugin's CCSDS items (`CCSDS_STREAMID`, `CCSDS_SEQUENCE`, `CCSDS_LENGTH`,
`CCSDS_FC`, `CCSDS_CHECKSUM`; `SECONDS`, `SUBSECS`, `PACKET_TIME`) followed by the F Prime packet, so the CFS
interface's `ProtocolCcsdsChecksum` and `ProtocolCcsdsLength` write protocols and `LengthProtocol` read protocol
apply unchanged. Telemetry uses exactly the six byte secondary header `fprime_cfs` emits (no
`SPARE_FOR_64_ALIGN`). Packets are named `<Deployment>.<F Prime name>` (for example
`FPrimeDeployment.CfsCore.cmdDisp.CMD_NO_OP`) and no catch-all packet is added. Events remain out of scope.

Telemetry identifies on `CCSDS_STREAMID` alone, like every cFS packet, because COSMOS identifies a target's
packets by one shared set of ID items; a different set would switch the CFS target to per-packet matching, where
the cFS plugin's file packets (no ID items) swallow everything defined after them. Hence one parent packet per
message id — `<Deployment>.FPRIME_PACKETS` and `<Deployment>.FPRIME_CHANNELS` — carries the F Prime packet as a
block and `fprime_subpacketizer.rb` resolves it to the `SUBPACKET` for the F Prime packet
(`FPrimeDeployment.Health`) or channel.

`--install` uploads the gem like the native tool (same `--cosmos-*` options; `--target-name` defaults to `CFS-1`).
Without Ruby on the host the gem is still built, by the same pure-Python packager as the native plugin.
`tests/e2e/cfs_reference.py` exercises the result against a running `fprime_cfs_reference` through COSMOS.

## What is generated

All widths and constants come from the dictionary's `typeDefinitions` and `constants` (descriptor, opcode,
channel and packet identifier types, `FwSizeStoreType` string lengths, boolean encodings, time base enum).

- **Commands** – `FPRIME_DESCRIPTOR`, `FPRIME_OPCODE` and the serialized arguments. Enums and booleans get
  `STATE`s, strings are a length item plus a `VARIABLE_BIT_SIZE` string, structs are flattened to
  `name.member`, arrays of numbers become COSMOS arrays and other arrays are flattened to `name[i]`.
- **Channelized telemetry** – the `FPRIME_CHANNELS` parent packet carries the concatenated channel records and a
  `SUBPACKETIZER` (`fprime_subpacketizer.py`) splits it into one `SUBPACKET` per channel with the channel id,
  F Prime time (and `FPRIME_TIME`, that time as Unix time), the value, `FORMAT_STRING` and `LIMITS` from the
  dictionary. COSMOS stamps packets with the received time; rename `FPRIME_TIME` to `PACKET_TIME` in a copy of
  the plugin to stamp them with F Prime time instead (only meaningful for `TB_WORKSTATION_TIME`).
- **Packetized telemetry** – one packet per entry of the selected `telemetryPacketSets` set.
- `FPRIME_UNKNOWN` (last in `packets.txt`) catches packets the plugin does not model (events, files, data products).

Events are not generated in this version.

## Development

```bash
python -m venv venv && . venv/bin/activate
pip install -e .[dev]
pytest
ruff check src tests && ruff format --check src tests
python tests/update_golden.py   # after intentional emitter changes
```

## Attribution

`fprime_subpacketizer.py` and the plugin variable layout derive from
[openc3-cosmos-fprime](https://github.com/OpenC3/openc3-cosmos-fprime), Copyright 2026 OpenC3, Inc., MIT
License; see [NOTICE](NOTICE). fprime-cosmos itself is licensed under the Apache License 2.0 ([LICENSE](LICENSE)).
