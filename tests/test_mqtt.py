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
