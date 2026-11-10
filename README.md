# fprime-cosmos: Tools to Make Open C3 COSMOS Integration Easier

Runs an [F Prime](https://github.com/nasa/fprime) deployment against [OpenC3 COSMOS](https://openc3.com).

`fprime-cosmos` reads the F Prime JSON topology dictionary, generates a native COSMOS plugin (commands,
channelized telemetry and packetized telemetry), installs it into a running COSMOS through the plugin API,
and starts `fprime-comm-bridge` (from `fprime-gds`) to move packets between the flight software and COSMOS.
No Ruby toolchain, no hand-edited plugin files and no knowledge of COSMOS internals are required. It also ships the
`cosmos` fprime-gds data transport, so the GDS Integration Test API can run against the deployment through COSMOS
(see [COSMOS as the GDS data transport](#cosmos-as-the-gds-data-transport-integration-tests-through-cosmos)).

```text
F Prime deployment  <-- framing / transport -->  fprime-comm-bridge  <-- UDP, F Prime packets -->  COSMOS  <-- cmd-tlm-api -->  fprime-gds pipeline
                                                 (owns framing)                                    (this plugin)                 (`cosmos` data transport,
                                                                                                                                  e.g. the integration test API)
```

## Installation

Requires Python 3.10 or newer and COSMOS 6.10.0 or newer for the generated plugin (the gem declares
`openc3_cosmos_minimum_version`); the fprime-gds data transport below needs COSMOS 7.2.0 or newer. The local-Docker defaulting described under Options uses the `docker` and `ip`
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

Neither a release nor `devel` carries the `data_transport` plugin category yet; the `cosmos` GDS transport below needs
[nasa/fprime-gds#386](https://github.com/nasa/fprime-gds/pull/386) (install: see that section). The launcher and
generator do not need it.

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

### COSMOS as the GDS data transport (integration tests through COSMOS)

`fprime-cosmos` also ships a [fprime-gds `data-transport` plugin](https://github.com/nasa/fprime-gds/pull/386) named
`cosmos`, so the GDS Integration Test API (and any other `fprime-gds` pipeline user) can talk to the deployment
*through COSMOS* instead of the GDS comm layer. Commands go to the COSMOS cmd-tlm-api as structured JSON-RPC
`cmd_no_hazardous_check` calls (target, command name, `{PARAMETER: value}`; the hazardous-command confirmation is
bypassed because nobody is there to confirm it). Telemetry comes back from the COSMOS streaming websocket
(`/openc3-api/cable`, DECOM CONVERTED values): a packet carrying `FPRIME_CHANNEL_ID` is resolved by channel id,
otherwise a packet named after a dictionary channel (this generator's dedicated packets) by its name, any other
packet by the channel names its items spell, and the value is reassembled from the items the generator
flattened (primitives, enums, booleans, strings, arrays and structs) with the F Prime time carried by the
`FPRIME_TIME_*` items (COSMOS receive time, workstation time base, when a packet has none or its values are unusable
or out of range, or its time base is not one the loaded dictionary defines — reported once).

```bash
# COSMOS up and `fprime-cosmos` running (plugin installed, bridge + deployment started, see Quick start)
export OPENC3_API_PASSWORD=...        # password only from the environment; never on the command line
pytest test/int --dictionary <dictionary>.json \
    --data-transport-selection cosmos --cosmos-url http://localhost:2900 --cosmos-target FPRIME
```

| Argument | Meaning |
| --- | --- |
| `--data-transport-selection cosmos` | select the COSMOS transport (default is the GDS ZeroMQ transport) |
| `--cosmos-url` | cmd-tlm-api URL, `http://localhost:2900` by default; `https://` is honored (and recommended for a remote COSMOS: over plain `http://` the password and session token travel in clear text, which the transport warns about for non-loopback hosts) |
| `--cosmos-scope` | COSMOS scope, `DEFAULT` |
| `--cosmos-target` | COSMOS target of the deployment: the launcher's/generator's `--target-name` (default `FPRIME` in both) or the `fprime_target_name` plugin variable when it was overridden; with `--cosmos-layout cfs` the target name given to the cFS generator, e.g. `FPRIME_APP` |
| `--cosmos-layout` | `fprime` (this generator: per-channel packets named after the channel, subscribed selectively) or `cfs` (*experimental*: one-app-one-target cFS generator — every packet of the target is subscribed; per-channel packets carry the channel leaf item as here and are resolved by their `FPRIME_CHANNEL_ID` item only, packet-set packets such as `HEALTH` carry the upper-cased dotted names such as `CFSCORE.CMDDISP.COMMANDSDISPATCHED`; checked against that generator's source by unit tests only, not live) |

Requires an `fprime-gds` with the `data_transport` plugin category (`--data-transport-selection`;
[nasa/fprime-gds#386](https://github.com/nasa/fprime-gds/pull/386), not in a release or on `devel` at the time of
writing):

```bash
# nasa/fprime-gds#386 at the commit this transport was tested against; replace with a release once one contains #386
pip install "fprime-gds @ git+https://github.com/nasa/fprime-gds.git@d190fcb6d928a099d4f67e62ba5479b5e3073ad6"
```

On an fprime-gds without the category the entry point is loaded and ignored and `--data-transport-selection` does not
exist. The transport is tested against COSMOS 7.5.1 (JSON-RPC `cmd_no_hazardous_check` with structured parameters,
`/openc3-api/auth/verify` session tokens, the ActionCable `StreamingChannel` frame shape) and needs COSMOS 7.2.0 or
newer (password-based `/openc3-api/auth/verify` and token authentication of the `StreamingChannel` subscription;
6.10.x-7.1.x authenticate differently and are not supported by the transport, although the generated plugin itself
installs there). Command arguments are flattened like the generator's parameters (structs
to `NAME.MEMBER`, non-numeric arrays to `NAME[i]`, booleans and enums as state names). Events arrive through the
generated `FPRIME_EVENTS` packet (the opaque F Prime event record, decoded by the fprime-gds event decoder, so event
assertions see native events); files and data products are not modeled (a file uplink attempted through the
transport fails at once with an error instead of waiting for a handshake); the cFS layout has no events packet yet. A COSMOS command
rejection is raised to the caller (the test fails); a target without telemetry packets, a refused password or a
rejected first subscription fails `connect()` at once; afterwards, after three
consecutive subscription rejections (before or after COSMOS confirmed the subscription, a non-reconnectable
disconnect or a password the cmd-tlm-api refuses) without a delivered packet in between, the stream gives up with an error instead of re-sending the
password forever, and the next command raises that error; an unreachable COSMOS (connection errors, 5xx) is retried
with the same backoff without counting toward that limit. The
password is never placed in URLs, logs, exception messages or the GDS connection URI, and the authentication requests
do not follow redirects (nor do the JSON-RPC calls or the websocket handshake).

### Generating the plugin on its own

```bash
fprime-to-cosmos [--dictionary PATH | -d DEPLOYMENT] -o openc3-plugin [--target-name NAME] [--no-gem] [--install]
```

The dictionary is identified as for the launcher (detected from the F Prime project when neither option is
given). By default this only writes `openc3-plugin/openc3-cosmos-fprime-<deployment>/` and the corresponding `.gem`,
which can be installed through the COSMOS Admin tool or `openc3cli load`. With `--install` the gem is also
installed into COSMOS (same `--cosmos-*`/`--force-install` options as the launcher).

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

The plugin-discovery and GDS command-line tests in `tests/test_gds_transport.py` skip unless an `fprime-gds` with the
`data_transport` category (nasa/fprime-gds#386) is installed.

## Attribution

`fprime_subpacketizer.py` and the plugin variable layout derive from
[openc3-cosmos-fprime](https://github.com/OpenC3/openc3-cosmos-fprime), Copyright 2026 OpenC3, Inc., MIT
License; see [NOTICE](NOTICE). fprime-cosmos itself is licensed under the Apache License 2.0 ([LICENSE](LICENSE)).
