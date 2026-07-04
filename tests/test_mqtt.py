"""Tests for the MQTT bridge, focused on paho-mqtt 2.x compatibility.

These tests pin the regression that motivated the fork: the bridge must build a
paho-mqtt client and run its callbacks cleanly on paho-mqtt >= 2.0 (VERSION2
callback API), with no deprecation warnings and no ``callback_api_version``
TypeError.
"""

import asyncio
import warnings
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock

import pytest

import paho.mqtt.client as mqtt
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.packettypes import PacketTypes

from pymadoka.mqtt import (
    build_client,
    reason_is_success,
    set_power_state,
    set_operation_mode,
    MQTT,
)
from pymadoka.connection import ConnectionStatus
from pymadoka.features.operationmode import OperationModeEnum


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _connack(name: str) -> ReasonCode:
    return ReasonCode(PacketTypes.CONNACK, name)


def make_mqtt(root_topic="madoka", root_topic_only=False,
              address="AA:BB:CC:DD:EE:FF"):
    cfg = {
        "mqtt": {
            "host": "localhost", "port": 1883, "ssl": False,
            "root_topic": root_topic, "root_topic_only": root_topic_only,
        }
    }
    controller = SimpleNamespace(
        connection=SimpleNamespace(
            address=address,
            connection_status=ConnectionStatus.CONNECTED,
        )
    )
    return MQTT(loop=None, controller=controller, config=cfg)


# --------------------------------------------------------------------------- #
# paho-mqtt 2.x regression
# --------------------------------------------------------------------------- #

def test_build_client_raises_no_deprecation_warning():
    """The whole point of the fork: constructing the client must be clean on
    paho-mqtt >= 2.0 (no 'Callback API version 1 is deprecated' warning, no
    missing-callback_api_version TypeError)."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # turn any warning into a failure
        client = build_client("madoka_mqtt_test")
    assert isinstance(client, mqtt.Client)


def test_build_client_targets_paho_2():
    """Guard against silently running on an unsupported paho-mqtt 1.x:
    the modern callback-API enum only exists on paho-mqtt >= 2.0."""
    assert hasattr(mqtt, "CallbackAPIVersion")


@pytest.mark.parametrize("reason_code,expected", [
    (0, True),
    (_connack("Success"), True),
    (_connack("Server unavailable"), False),
    (_connack("Not authorized"), False),
])
def test_reason_is_success(reason_code, expected):
    assert reason_is_success(reason_code) is expected


# --------------------------------------------------------------------------- #
# Topic construction
# --------------------------------------------------------------------------- #

def test_get_device_topic_appends_normalized_mac():
    m = make_mqtt(root_topic="madoka", root_topic_only=False)
    assert m.get_device_topic() == "madoka/AA_BB_CC_DD_EE_FF"


def test_get_device_topic_root_only():
    m = make_mqtt(root_topic="madoka", root_topic_only=True)
    assert m.get_device_topic() == "madoka"


def test_discovery_message_builds_expected_topics():
    device_topic = "madoka/AA_BB_CC_DD_EE_FF"
    dm = MQTT.DiscoveryMessage(
        "AA:BB:CC:DD:EE:FF", "Living Room AC", device_topic, {}
    )
    assert dm.mode_command_topic == f"{device_topic}/operation_mode/set"
    assert dm.fan_mode_command_topic == f"{device_topic}/fan_speed/set"
    assert dm.power_command_topic == f"{device_topic}/power_state/set"
    assert dm.temperature_command_topic == f"{device_topic}/set_point/set"
    assert dm.current_temperature_topic == f"{device_topic}/state/get"
    assert dm.availability["topic"] == f"{device_topic}/available"
    assert dm.unique_id == "AA:BB:CC:DD:EE:FF"
    assert dm.name == "Living Room AC"
    assert set(dm.modes) >= {"auto", "off", "cool", "heat"}


# --------------------------------------------------------------------------- #
# VERSION2 callbacks
# --------------------------------------------------------------------------- #

def test_on_connect_success_marks_connected():
    m = make_mqtt()
    m.client = MagicMock()
    m.connect_future = None
    m.on_connect(m.client, None, None, _connack("Success"))
    assert m.connected is True
    m.client.subscribe.assert_called_once()  # start() ran


def test_on_connect_failure_marks_disconnected():
    m = make_mqtt()
    m.client = MagicMock()
    m.connect_future = None
    m.on_connect(m.client, None, None, _connack("Server unavailable"))
    assert m.connected is False
    m.client.subscribe.assert_not_called()


@pytest.mark.asyncio
async def test_on_disconnect_accepts_version2_signature():
    """on_disconnect must accept the 5-arg VERSION2 signature and schedule a
    reconnect (it calls asyncio.create_task, so it needs a running loop)."""
    m = make_mqtt()
    m.client = MagicMock()
    m.reconnect = AsyncMock()
    # disconnect_flags, reason_code, properties
    m.on_disconnect(m.client, None, None, _connack("Success"), None)
    await asyncio.sleep(0)  # let the scheduled task run
    m.reconnect.assert_called_once()


@pytest.mark.asyncio
async def test_on_disconnect_skips_when_already_reconnecting():
    """A disconnect that arrives while a reconnect loop is already running must
    NOT spawn a second competing loop."""
    m = make_mqtt()
    m.client = MagicMock()
    m.reconnect = AsyncMock()
    m._reconnecting = True
    m.on_disconnect(m.client, None, None, _connack("Success"), None)
    await asyncio.sleep(0)
    m.reconnect.assert_not_called()


@pytest.mark.asyncio
async def test_reconnect_is_single_flight():
    """reconnect() must return immediately if another loop already owns the
    reconnect (guarded by _reconnecting), without touching the client."""
    m = make_mqtt()
    m.client = MagicMock()
    m._reconnecting = True
    await m.reconnect()
    m.client.reconnect.assert_not_called()


@pytest.mark.asyncio
async def test_reconnect_reuses_client_and_stops_when_connected():
    """reconnect() must reuse the existing client via client.reconnect()
    (not build a new one) and exit once is_connected() is True."""
    m = make_mqtt()
    m.client = MagicMock()
    # Not connected on entry, connected after the first client.reconnect().
    m.client.is_connected.side_effect = [False, True, True]
    m.publish_discovery = AsyncMock()
    await m.reconnect()
    m.client.reconnect.assert_called_once()
    assert m._reconnecting is False  # guard released in finally


@pytest.mark.asyncio
async def test_reconnect_republishes_discovery():
    """After the socket is back, reconnect() MUST re-publish the HA discovery
    config. A broker restart drops all retained messages (incl. discovery), so
    without this the AC shows 'unavailable' in HA forever even though the
    bridge is up — the exact bug this fix targets."""
    m = make_mqtt()
    m.client = MagicMock()
    m.client.is_connected.side_effect = [False, True, True]
    m.publish_discovery = AsyncMock()
    await m.reconnect()
    m.publish_discovery.assert_awaited_once()


@pytest.mark.asyncio
async def test_publish_discovery_publishes_config_sensors_and_availability():
    """publish_discovery() must (re)send the climate discovery, the extra
    sensor discovery, and the availability=online flag together."""
    m = make_mqtt()  # controller.connection_status == CONNECTED
    m.discovery = MagicMock()
    m.discovery_sensors = MagicMock()
    m.available = MagicMock()
    await m.publish_discovery()
    m.discovery.assert_called_once()
    m.discovery_sensors.assert_called_once()
    m.available.assert_called_once_with(True)


def test_connect_sets_last_will():
    """connect() must register an LWT on the availability topic (retained, 0)
    BEFORE connecting, so the broker reports us unavailable on an ungraceful
    drop."""
    import pymadoka.mqtt as mqtt_mod
    fake_client = MagicMock()
    m = make_mqtt(root_topic="madoka", root_topic_only=False)
    loop = asyncio.new_event_loop()
    m.loop = loop

    async def _run():
        # build_client is module-level; patch it for the duration of connect().
        orig = mqtt_mod.build_client
        mqtt_mod.build_client = lambda _id: fake_client
        try:
            return m.connect()
        finally:
            mqtt_mod.build_client = orig

    try:
        loop.run_until_complete(_run())
    finally:
        loop.close()

    fake_client.will_set.assert_called_once()
    args, kwargs = fake_client.will_set.call_args
    assert args[0] == "madoka/AA_BB_CC_DD_EE_FF/available"
    assert kwargs.get("payload") == "0"
    assert kwargs.get("retain") is True
    # will must be set before connect() is invoked
    assert fake_client.connect.called


# --------------------------------------------------------------------------- #
# Command path decoding (broker -> BLE)
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_set_power_state_on():
    controller = SimpleNamespace(power_state=SimpleNamespace(update=AsyncMock()))
    await set_power_state(controller, b"ON")
    controller.power_state.update.assert_awaited_once()
    assert controller.power_state.update.await_args.args[0].turn_on is True


@pytest.mark.asyncio
async def test_set_power_state_off():
    controller = SimpleNamespace(power_state=SimpleNamespace(update=AsyncMock()))
    await set_power_state(controller, b"off")
    assert controller.power_state.update.await_args.args[0].turn_on is False


@pytest.mark.asyncio
async def test_set_operation_mode_heat():
    controller = SimpleNamespace(
        operation_mode=SimpleNamespace(update=AsyncMock()),
        power_state=SimpleNamespace(update=AsyncMock()),
    )
    await set_operation_mode(controller, b"HEAT")
    controller.operation_mode.update.assert_awaited_once()
    assert controller.operation_mode.update.await_args.args[0].operation_mode \
        == OperationModeEnum.HEAT
    # HEAT != OFF -> power on
    assert controller.power_state.update.await_args.args[0].turn_on is True


@pytest.mark.asyncio
async def test_set_operation_mode_off_only_powers_off():
    controller = SimpleNamespace(
        operation_mode=SimpleNamespace(update=AsyncMock()),
        power_state=SimpleNamespace(update=AsyncMock()),
    )
    await set_operation_mode(controller, b"OFF")
    controller.operation_mode.update.assert_not_awaited()
    assert controller.power_state.update.await_args.args[0].turn_on is False


# ---------------------------------------------------------------------------
# Layer-4 watchdog (persistent restart-spiral circuit breaker)
# ---------------------------------------------------------------------------

from pymadoka.mqtt import _BridgeState, _layer4_decide


def test_bridge_state_first_run_is_empty(tmp_path):
    s = _BridgeState(str(tmp_path / "state.json"))
    assert s.process_starts == []
    assert s.reboot_history == []


def test_bridge_state_register_start_persists(tmp_path):
    p = str(tmp_path / "state.json")
    s = _BridgeState(p)
    s.register_start(1000.0)
    s2 = _BridgeState(p)
    assert s2.process_starts == [1000.0]


def test_bridge_state_rotates_old_starts(tmp_path):
    s = _BridgeState(str(tmp_path / "state.json"))
    s.process_starts = [100.0, 200.0, 300.0]  # all "old"
    # Now is 2 hours later — all should be rotated out (>1h window).
    s.register_start(now=300.0 + 7200)
    assert s.process_starts == [300.0 + 7200]


def test_bridge_state_keeps_recent_starts(tmp_path):
    s = _BridgeState(str(tmp_path / "state.json"))
    # Three recent starts within the last hour, plus one old one.
    now = 10000.0
    s.process_starts = [now - 3700, now - 30, now - 10]  # 1 old + 2 recent
    s.register_start(now)
    assert sorted(s.process_starts) == [now - 30, now - 10, now]


def test_bridge_state_rotates_old_reboots(tmp_path):
    s = _BridgeState(str(tmp_path / "state.json"))
    s.reboot_history = [100.0]  # 49 h old
    # register_start rotates BOTH lists.
    s.register_start(now=100.0 + 49 * 3600)
    assert s.reboot_history == []


def test_bridge_state_corrupt_file_is_safe(tmp_path):
    p = tmp_path / "state.json"
    p.write_text("not json {{{")
    s = _BridgeState(str(p))
    # Corrupt file → empty state, no crash.
    assert s.process_starts == []
    assert s.reboot_history == []


def test_bridge_state_counts_in_window():
    s = _BridgeState.__new__(_BridgeState)  # bypass _load
    s.process_starts = [100.0, 200.0, 300.0, 400.0, 500.0]
    s.reboot_history = []
    # Window from 250 to 600: starts at 300,400,500 → 3.
    assert s.starts_in_last(600 - 250, 600) == 3
    # Full history: 5.
    assert s.starts_in_last(10000, 600) == 5


def test_layer4_normal_below_threshold(tmp_path):
    s = _BridgeState(str(tmp_path / "state.json"))
    # 4 starts in the last 30 min — under default threshold of 5.
    now = 100000.0
    for t in (now - 600, now - 500, now - 200, now - 50):
        s.process_starts.append(t)
    assert _layer4_decide(s, now) == "normal"


def test_layer4_reboot_when_spiral_and_breaker_open(tmp_path):
    s = _BridgeState(str(tmp_path / "state.json"))
    now = 100000.0
    # 5 starts in last 30 min → trigger.
    for t in (now - 1500, now - 1000, now - 500, now - 200, now - 30):
        s.process_starts.append(t)
    # No reboots in last 24 h → breaker open → reboot.
    assert _layer4_decide(s, now) == "reboot"


def test_layer4_degraded_when_breaker_tripped(tmp_path):
    s = _BridgeState(str(tmp_path / "state.json"))
    now = 100000.0
    for t in (now - 1500, now - 1000, now - 500, now - 200, now - 30):
        s.process_starts.append(t)
    # 2 reboots in last 24 h → at the default cap → no more reboots.
    s.reboot_history = [now - 7200, now - 3600]
    assert _layer4_decide(s, now) == "degraded"


def test_layer4_rotated_old_reboots_dont_block(tmp_path):
    s = _BridgeState(str(tmp_path / "state.json"))
    now = 100000.0
    for t in (now - 1500, now - 1000, now - 500, now - 200, now - 30):
        s.process_starts.append(t)
    # 2 reboots, both >24 h ago → don't count → breaker open → reboot.
    s.reboot_history = [now - 25 * 3600, now - 30 * 3600]
    assert _layer4_decide(s, now) == "reboot"
