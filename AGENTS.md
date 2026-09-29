# pymadoka — Claude instructions

Fork of `mduran80/pymadoka`: a Daikin Madoka BRC1H **BLE ↔ MQTT bridge**
(`pymadoka-mqtt`) with Home Assistant auto-discovery. This fork exists because
upstream breaks on modern deps and lacks resilience; do **not** install upstream
from PyPI — always install from this fork.

## Repo / remote (read this before pushing)

- **Remote: `github.com/oriolrius/pymadoka` — GitHub, NOT Gitea.** Don't default
  to `git.oriolrius.cat` for this one.
- **Push as `oriolrius`.** The machine's default `gh` account is `i40sys`, which
  has **no write access** here (push → 403). Either `gh auth switch --user
  oriolrius`, or push over SSH (`git@github.com:oriolrius/pymadoka.git`) — SSH
  authenticates as oriolrius.
- Branch: `main`. Tags are **bare `X.Y.Z`** (no `v` prefix).

## Python / tests

- Use **`uv`** (never raw pip/python).
- Run tests: `uv run pytest tests/ -q` (from the repo root). MQTT bridge logic is
  covered in `tests/test_mqtt.py` — add/extend tests there for any `mqtt.py`
  change.

## Version & the metadata gotcha

- **Version source of truth: `setup.py` (`version='X.Y.Z'`).** Bump it manually
  (no commitizen here).
- ⚠️ **The installed dist-info version string is unreliable.** Early releases
  never bumped `setup.py`, so a `git+…@<tag>` install can report an old version
  (e.g. `0.2.14`) regardless of the tag. **Verify a deployment by code marker,
  not metadata** — e.g. `grep -c will_set …/site-packages/pymadoka/mqtt.py`, not
  `importlib.metadata` / `pymadoka-mqtt --version` of a stale install.

## Release flow

1. Make the change (conventional commits: `feat(mqtt): …`, `fix(mqtt): …`).
2. Bump `version=` in `setup.py`.
3. `uv run pytest tests/ -q` — must pass.
4. Commit, then `git tag X.Y.Z`.
5. Push as oriolrius: `git push origin main && git push origin X.Y.Z`
   (or `git push git@github.com:oriolrius/pymadoka.git main X.Y.Z`).
6. `gh release create X.Y.Z --verify-tag --title "X.Y.Z — <summary>" --notes …`
   — match the existing release title style `<version> — <short summary>`.

## Hard constraints (don't regress these)

- **`paho-mqtt >= 2.1.0`**, VERSION2 callback API (`build_client()` /
  `reason_is_success()`). The whole fork started to fix paho-mqtt 2.x.
- **ARMv6 target** (Pi Zero W): install via piwheels + `SKIP_CYTHON=1` for
  `dbus-fast` (no armv6 wheel on PyPI).
- **Linux-only** — shells out to `bluetoothctl`.
- MQTT topics/discovery payload in `mqtt.py` are the ground truth consumed by HA;
  changing topic shapes breaks existing HA entities.

## Deployment

This package runs as a systemd service on a dedicated Raspberry Pi Zero W bridge,
installed via `uv pip install git+…@<tag>` into a venv. The full, infra-specific
runbook (host, pairing, watchdog, Wi-Fi tuning, systemd unit) lives in the
private knowledge base at `~/infra-kb/sources/home-automation/madoka2mqtt/` — keep
host/MAC/network specifics there, not in this public repo.
