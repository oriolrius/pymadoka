import asyncio
from asyncio.exceptions import CancelledError
from hashlib import new
import logging
import click
import json
import yaml

import paho.mqtt.client as mqtt
from paho.mqtt import MQTTException
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from functools import wraps
from dbus_fast.aio import MessageBus
from dbus_fast import BusType
from pymadoka.connection import Connection, ConnectionStatus, ConnectionException, discover_devices, force_device_disconnect
from pymadoka.controller import Controller
from pymadoka.features.fanspeed import FanSpeedEnum, FanSpeedStatus
from pymadoka.features.setpoint import SetPointStatus
from pymadoka.features.operationmode import OperationModeStatus, OperationModeEnum
from pymadoka.features.power import PowerStateStatus
from pymadoka.features.clean_filter import ResetCleanFilterTimerStatus
from pymadoka import Feature

logger = logging.getLogger(__name__)

# Taken from paho-mqtt examples to integrate with asyncio loop

def build_client(client_id: str) -> mqtt.Client:
    """Create a paho-mqtt client using the modern (paho-mqtt >= 2.0) callback API.

    paho-mqtt 2.0 made ``callback_api_version`` a mandatory argument and 3.0 will
    drop the legacy VERSION1 callbacks entirely, so we target VERSION2 explicitly.
    This keeps the bridge working (and deprecation-warning-free) on the latest
    paho-mqtt while the callbacks below use the VERSION2 signatures.
    """
    return mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
    )


def reason_is_success(reason_code) -> bool:
    """Return True if a paho-mqtt result represents success.

    paho-mqtt 2.x hands VERSION2 callbacks a ``ReasonCode`` object exposing an
    ``is_failure`` property; we also accept a plain ``int`` (0 == success) so the
    helper stays easy to unit-test.
    """
    is_failure = getattr(reason_code, "is_failure", None)
    if is_failure is not None:
        return not is_failure
    return reason_code == 0


class AsyncioHelper:
    def __init__(self, loop, client):
        self.loop = loop
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write

    def on_socket_open(self, client, userdata, sock):
        def cb():
            client.loop_read()

        self.loop.add_reader(sock, cb)
        self.misc = self.loop.create_task(self.misc_loop())

    def on_socket_close(self, client, userdata, sock):
        self.loop.remove_reader(sock)
        self.misc.cancel()

    def on_socket_register_write(self, client, userdata, sock):

        def cb():
            client.loop_write()

        self.loop.add_writer(sock, cb)

    def on_socket_unregister_write(self, client, userdata, sock):
        self.loop.remove_writer(sock)

    async def misc_loop(self):
        while self.client.loop_misc() == mqtt.MQTT_ERR_SUCCESS:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break


async def set_operation_mode(controller:Controller,payload:str):
    """
    Callback used to set the operation mode. It will issue a turn on/off command 
    depending on the value of the mode (OFF if 'OFF', ON otherwise)
    Args:
        controller (`Controller`): Device controller
        payload (str): The payload will be converted to the values accepted by the controller
    """
    try:
        value = payload.decode("utf-8").upper()
        if value != "OFF":
            status = OperationModeStatus(OperationModeEnum[value])
            await controller.operation_mode.update(status)
        await controller.power_state.update(PowerStateStatus(value != "OFF"))
    except CancelledError as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    except ConnectionException as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    except ConnectionAbortedError as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    except Exception as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    
async def set_fan_speed(controller:Controller,payload:str):
    """
    Callback used to set the fan speed
    Args:
        controller (`Controller`): Device controller
        payload (str): The payload will be converted to the values accepted by the controller
    """

    try:

        value = payload.decode("utf-8").upper()
        # .cooling_fan_speed / .heating_fan_speed are FanSpeedEnum members, not
        # strings — FanSpeedEnum[enum_member] raises KeyError, so extract .name.
        new_cooling_fan_speed = controller.fan_speed.status.cooling_fan_speed.name
        new_heating_fan_speed = controller.fan_speed.status.heating_fan_speed.name
        if (controller.operation_mode.status.operation_mode == OperationModeEnum.AUTO or
           controller.operation_mode.status.operation_mode == OperationModeEnum.DRY or
           controller.operation_mode.status.operation_mode == OperationModeEnum.FAN):
            new_cooling_fan_speed = value
            new_heating_fan_speed = value
        elif controller.operation_mode.status.operation_mode == OperationModeEnum.HEAT:
            new_heating_fan_speed = value
        elif controller.operation_mode.status.operation_mode == OperationModeEnum.COOL:
            new_cooling_fan_speed = value

        status = FanSpeedStatus(FanSpeedEnum[new_cooling_fan_speed],
                                FanSpeedEnum[new_heating_fan_speed])
        await controller.fan_speed.update(status)
    except CancelledError as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    except ConnectionException as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    except ConnectionAbortedError as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    except Exception as e:
        logging.error(f"Could not update operation mode: {str(e)}")

async def set_power_state(controller:Controller,payload:str):
    """
    Callback used to set the power state
    Args:
        controller (`Controller`): Device controller
        payload (str): The payload will be converted to the values accepted by the controller
    """
    try:
        status = PowerStateStatus(payload.decode("utf-8").upper()=="ON")
        await controller.power_state.update(status)
    except CancelledError as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    except ConnectionException as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    except ConnectionAbortedError as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    except Exception as e:
        logging.error(f"Could not update operation mode: {str(e)}")

async def set_set_point_state(controller:Controller,payload:str):
    """
    Callback used to set the set point (target temperature)
    Args:
        controller (`Controller`): Device controller
        payload (str): The payload will be converted to the values accepted by the controller
    """
    try:
        value = int(payload.decode("utf-8"))
        new_cooling_set_point = controller.set_point.status.cooling_set_point
        new_heating_set_point = controller.set_point.status.heating_set_point

        if (controller.operation_mode.status.operation_mode == OperationModeEnum.AUTO or
           controller.operation_mode.status.operation_mode == OperationModeEnum.DRY or
           controller.operation_mode.status.operation_mode == OperationModeEnum.FAN):
            new_cooling_set_point = value
            new_heating_set_point = value 
        elif controller.operation_mode.status.operation_mode == OperationModeEnum.HEAT:
            new_heating_set_point = value
        elif controller.operation_mode.status.operation_mode == OperationModeEnum.COOL:
            new_cooling_set_point = value
        await controller.set_point.update(SetPointStatus(new_cooling_set_point,new_heating_set_point))
    except CancelledError as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    except ConnectionException as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    except ConnectionAbortedError as e:
        logging.error(f"Could not update operation mode: {str(e)}")
    except Exception as e:
        logging.error(f"Could not update operation mode: {str(e)}")

class MQTT:

    """This class implements the MQTT bridge.
    
    Attributes:
        controller (Controller): Connection used to communicate with the device
        connected (bool): Feature used to control the fan speed
        client (Client): Feature used to control the fan speed
        mqtt_cfg (Dict[str,Any]): Feature used to control the fan speed
        loop (AsyncioLoop): Feature used to control the fan speed
    """
  
    ROOT_TOPIC = "/madoka"
    OPERATION_MODE_TOPIC = "operation_mode"
    POWER_STATE_TOPIC = "power_state"
    FAN_SPEED_TOPIC = "fan_speed"
    SET_POINT_TOPIC = "set_point"
    AVAILABLE_TOPIC = "available"
    STATE_TOPIC = "state"

    @dataclass
    class DiscoveryMessage:
        
        name:str
        unique_id:str
        current_temperature_topic: str
        fan_mode_command_topic: str
        fan_mode_state_topic: str
        mode_command_topic: str
        mode_state_topic: str
        power_command_topic: str
        temperature_state_topic: str
        temperature_command_topic: str 
        
        modes: List[str] = field(default_factory=list)
        fan_modes: List[str] = field(default_factory=list)
        temperature_command_template: str = "{{ int(value) }}"
        temperature_state_template: str = "{{ value_json.set_point['heating_set_point'] if value_json.operation_mode['operation_mode']=='HEAT' else value_json.set_point['cooling_set_point']}}"
        mode_state_template: str =  "{% set values = {None:None,'off':'off','HEAT':'heat','COOL':'cool','FAN':'fan_only', 'AUTO':'auto', 'DRY':'dry'} %} {{values[value_json.operation_mode['operation_mode']] if value_json.power_state['turn_on'] else 'off' }}"
        mode_command_template: str = "{% set values = { 'auto':'AUTO', 'heat':'HEAT', 'cool':'COOL', 'fan_only':'FAN','off':'AUTO','dry':'DRY'} %}{{ values[value] if value in values.keys() else 'AUTO' }}"
        fan_mode_state_template: str = "{% set values = { 'AUTO':'auto', 'LOW':'low', 'MEDIUM':'medium', 'HIGH':'high'} %} {{ values[value_json.fan_speed['heating_fan_speed']] if value_json.operation_mode['operation_mode']=='HEAT' else values[value_json.fan_speed['cooling_fan_speed']]}}"
        fan_mode_command_template: str = "{% set values = { 'auto':'AUTO', 'low':'LOW', 'medium':'MID', 'high':'HIGH'} %}{{ values[value] }}"
        current_temperature_template: str = "{{ value_json.temperatures['indoor'] }}"
        min_temp: int = 17
        max_temp: int =  31
        precision: int = 1
        temp_step: int = 1
        temperature_unit: str = "C"
        device: Dict[str, Any] = field(default_factory=dict)
        availability: Dict[str, Any] = field(default=dict)

        def __init__(self,device_name: str, device_friendly_name: str, device_topic: str, dev_info: Dict[str, Any]):
            self.modes = ["auto","off","cool","heat","dry","fan_only"]
            self.fan_modes = ["low","medium","high"]
            self.availability = {"payload_available": 1,
                                "payload_not_available": 0,
                                "topic": device_topic + "/available"
                                }
            self.current_temperature_topic = "/".join([device_topic,MQTT.STATE_TOPIC,"get"])
            self.fan_mode_command_topic = "/".join([device_topic,MQTT.FAN_SPEED_TOPIC,"set"])
            self.fan_mode_state_topic = "/".join([device_topic,MQTT.STATE_TOPIC,"get"])
            self.mode_command_topic = "/".join([device_topic,MQTT.OPERATION_MODE_TOPIC,"set"])
            self.mode_state_topic = "/".join([device_topic,MQTT.STATE_TOPIC,"get"])
            self.power_command_topic = "/".join([device_topic,MQTT.POWER_STATE_TOPIC,"set"])
            self.temperature_state_topic = "/".join([device_topic,MQTT.STATE_TOPIC,"get"])
            self.temperature_command_topic = "/".join([device_topic,MQTT.SET_POINT_TOPIC,"set"])

            # The class-level dataclass defaults below are NOT applied because this
            # custom __init__ overrides the dataclass-generated one, so they never
            # land in __dict__ / vars() and were missing from the published payload.
            # Assign them explicitly so HA receives the value templates and limits.
            self.min_temp = 17
            self.max_temp = 31
            self.precision = 1
            self.temp_step = 1
            self.temperature_unit = "C"
            self.temperature_command_template = "{{ value | int }}"
            self.temperature_state_template = "{{ value_json.set_point['heating_set_point'] if value_json.operation_mode['operation_mode']=='HEAT' else value_json.set_point['cooling_set_point'] }}"
            self.mode_state_template = "{% set values = {None:None,'off':'off','HEAT':'heat','COOL':'cool','FAN':'fan_only','AUTO':'auto','DRY':'dry'} %}{{ values[value_json.operation_mode['operation_mode']] if value_json.power_state['turn_on'] else 'off' }}"
            self.mode_command_template = "{% set values = {'auto':'AUTO','heat':'HEAT','cool':'COOL','fan_only':'FAN','off':'OFF','dry':'DRY'} %}{{ values[value] if value in values.keys() else 'AUTO' }}"
            self.fan_mode_state_template = "{% set values = {'AUTO':'auto','LOW':'low','MID':'medium','HIGH':'high'} %}{{ values[value_json.fan_speed['heating_fan_speed']] if value_json.operation_mode['operation_mode']=='HEAT' else values[value_json.fan_speed['cooling_fan_speed']] }}"
            self.fan_mode_command_template = "{% set values = {'auto':'AUTO','low':'LOW','medium':'MID','high':'HIGH'} %}{{ values[value] }}"
            self.current_temperature_template = "{{ value_json.temperatures['indoor'] }}"

            # These are usually set by HA, but we will enforce them
            self.unique_id = device_name
            self.name = device_friendly_name
            self.device = self.device_info(dev_info)
        
        def device_info(self, dev_info):
            """Return a device description for device registry."""

            model = (
                ("BRC1H" + dev_info["Model Number String"])
                if "Model Number String" in dev_info
                else ""
            )
            sw_version = (
                dev_info["Software Revision String"]
                if "Software Revision String" in dev_info
                else ""
            )
            return {
                # A list of strings — a Python set/tuple is not JSON-serializable
                # and previously leaked as a stringified set into the payload.
                "identifiers": [f"daikin_madoka_{self.unique_id}"],
                "name": self.name,
                "manufacturer": "DAIKIN",
                "model": model,
                "sw_version": sw_version,
            }

            
                
    def __init__(self,loop,controller: Controller,
                 config: Dict[str,Any]):

        """Initialize the MQTT bridge.
    
        Args:
            loop (AsyncioLoop): Asyncio loop to integrate the MQTT loop with
            controller (Controller): Controller used to manage the device
            mqtt_cfg (Dict[str,Any]): MQTT config 
            
        """
        self.controller:Controller = controller
        self.connected:bool = False
        self.client:mqtt.Client = None
        self.mqtt_cfg = config["mqtt"]
        self.loop = loop
        

    def connect(self):
        """ Connect to the MQTT broker. It returns a future to notify when the 
        connection has been finished.
        """

        id = "madoka_mqtt_" + self.controller.connection.address

        if "id" in self.mqtt_cfg:
            id = self.mqtt_cfg["id"]

        self.client:mqtt.Client = build_client(id)
        if "username" in self.mqtt_cfg:
            self.client.username_pw_set(username=self.mqtt_cfg["username"],password=self.mqtt_cfg["password"])
       
        if self.mqtt_cfg["ssl"]:
            # Use the system CA store and a modern TLS version. The previous code
            # referenced a non-existent ``mqtt.TLS_CERT_PATH`` which raised
            # AttributeError the moment SSL was enabled.
            self.client.tls_set(cert_reqs=mqtt.ssl.CERT_REQUIRED,
                                tls_version=mqtt.ssl.PROTOCOL_TLS_CLIENT)
            self.client.tls_insecure_set(False)
        
        try:

            aioh = AsyncioHelper(self.loop, self.client)
         
            self.client.on_connect = self.on_connect
            self.client.on_message = self.on_message
            self.client.on_disconnect = self.on_disconnect
            self.client.connect(self.mqtt_cfg["host"], port=self.mqtt_cfg["port"])
            self.connect_future = asyncio.get_event_loop().create_future()
            return self.connect_future

        except MQTTException as e:
            logger.error(f"Error in MQTT: {str(e)}")
    
    def start(self):
        """Start the MQTT bridge. Subscribe to the topics"""
        subscribe_topics = []
        for k,v in vars(self.controller).items():
            if isinstance(v,Feature):
                subscribe_topics.append(("/".join([self.get_device_topic(),k,"set"]),0))

        self.client.subscribe(subscribe_topics)

    def stop(self):
        """ Disconnect from the MQTT broker """
        if self.client:
            self.client.disconnect()
       
    
    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        """ Connection established callback (paho-mqtt VERSION2 API). """
        self.connected = reason_is_success(reason_code)
        if self.connect_future is not None and not self.connect_future.done():
            self.connect_future.set_result(self.connected)
        if self.connected:
            logger.info("Connected to MQTT broker")
            self.start()
            # NOTE: discovery() and discovery_sensors() are intentionally NOT
            # called here. on_connect runs synchronously inside the asyncio
            # reader callback — paho can queue publishes here, but the only way
            # to flush them is to yield the loop. wait_for_publish() would
            # block the loop and the PUBACK would never arrive. The caller in
            # run() handles discovery after awaiting connect, with explicit
            # asyncio.sleep() yields between batches so the socket writer
            # callback gets time to push the bytes out.
        self.available(self.controller.connection.connection_status == ConnectionStatus.CONNECTED)

    def on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None):
        """ Disconnection callback (paho-mqtt VERSION2 API). """
        logger.debug(f"Disconnected from MQTT broker ({reason_code})")
        asyncio.create_task(self.reconnect())

    async def reconnect(self):
        # We can't trust the client.is_connected() value here
        # as it is not updated
        is_connected = False        
        while not is_connected:
            try:
                logger.debug("Reconnecting in 60s...")
                await asyncio.sleep(60) 
                is_connected = await self.connect()
            except CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error in MQTT: {str(e)}")
                
    def normalize(self, address: str):
        normalized_name = address
        normalized_name = normalized_name.replace(" ","_")
        normalized_name = normalized_name.replace(":","_")
        normalized_name = normalized_name.replace("/","_")    
        return normalized_name
    
    def get_device_topic(self):
        """ Get the customized device topic using the device name and the root topic. 
        Returns:
            str: The device topic in the form /root_topic/device_name"""
        root_topic = self.ROOT_TOPIC 
        if "root_topic" in self.mqtt_cfg:
            root_topic = self.mqtt_cfg["root_topic"]
        if self.mqtt_cfg["root_topic_only"]: 
            return root_topic
        else: 
            normalized_name = self.controller.connection.address
            normalized_name = normalized_name.replace(" ","_")
            normalized_name = normalized_name.replace(":","_")
            normalized_name = normalized_name.replace("/","_")   
            return "/".join([root_topic, normalized_name]) 

    def available(self, status:bool):
        """
        Send the status to the availability topic
        Args:
            status (bool): True if available, False otherwise
        """
        if self.client is None or not self.client.is_connected():
            return
        device_topic = self.get_device_topic()
        topic = "/".join([device_topic, self.AVAILABLE_TOPIC])
        try:
            self.client.publish(topic,"0" if not status else "1",qos=1,retain=True)
        except Exception as e:
            logger.warning(f"available() publish failed: {e}")

    def update(self, status:str):
        """
        Send the status to the status topic (JSON payload)
        Args:
            status (str): New status
        """           
        if not self.client.is_connected():
            logger.debug("MQTT broker is not available. Skipping message...")
        else:
            device_topic = self.get_device_topic()
            topic = "/".join([device_topic, "state","get"])
            
            self.client.publish(topic,status)
    
    def discovery(self):
        """
        Send the discovery message to the config topic (JSON payload)
        Args:
            status (str): New status
        """           
        if not self.client.is_connected():
            logger.debug("MQTT broker is not available. Skipping message...")
        else:
            if "discovery_topic" in self.mqtt_cfg:
                discovery_topic: str = self.mqtt_cfg["discovery_topic"]
                device_topic = self.get_device_topic()
                if device_topic.startswith("/"):
                    device_topic = device_topic[1:]
                discovery_topic = discovery_topic.replace("<device_topic>", device_topic)
                discovery = MQTT.DiscoveryMessage(self.controller.connection.address,
                                            self.mqtt_cfg.get("friendly_name","Madoka friendly name"), 
                                            self.get_device_topic(),
                                            self.controller.info)
                self.client.publish(discovery_topic, json.dumps(vars(discovery), default=str), qos=1, retain=True)

    def discovery_sensors(self):
        """Publish HA MQTT discovery for extra sensors and binary_sensors.

        These cover all Madoka data fields that are NOT exposed by the main
        climate entity, plus BT link state (connected/paired/bonded/trusted/
        services_resolved). RSSI is included but will be null while connected.
        All use the same state/get topic with value_template to extract their
        field from the JSON blob.
        """
        if not self.client.is_connected():
            return
        state_topic = "/".join([self.get_device_topic(), "state", "get"])
        uid = self.controller.connection.address.replace(":", "_")
        friendly = self.mqtt_cfg.get("friendly_name", "Madoka")
        device = {
            "identifiers": [f"daikin_madoka_{self.controller.connection.address}"],
            "name": friendly,
            "manufacturer": "DAIKIN",
            "model": self.controller.info.get("Model Number String", "BRC1H"),
            "sw_version": self.controller.info.get("Software Revision String", ""),
        }

        sensors = [
            # Extra Madoka data ------------------------------------------------
            {
                "type": "sensor",
                "slug": "outdoor_temperature",
                "name": f"{friendly} Outdoor Temperature",
                "device_class": "temperature",
                "unit": "°C",
                "state_class": "measurement",
                "value_template": "{{ value_json.temperatures['outdoor'] }}",
            },
            {
                "type": "sensor",
                "slug": "heating_set_point",
                "name": f"{friendly} Heating Set Point",
                "device_class": "temperature",
                "unit": "°C",
                "state_class": "measurement",
                "value_template": "{{ value_json.set_point['heating_set_point'] }}",
            },
            {
                "type": "sensor",
                "slug": "heating_fan_speed",
                "name": f"{friendly} Heating Fan Speed",
                "icon": "mdi:fan",
                "value_template": "{{ value_json.fan_speed['heating_fan_speed'] }}",
            },
            {
                "type": "binary_sensor",
                "slug": "clean_filter",
                "name": f"{friendly} Clean Filter",
                "device_class": "problem",
                "value_template": "{{ value_json.clean_filter_indicator['clean_filter_indicator'] }}",
                "payload_on": "True",
                "payload_off": "False",
            },
            # BT link state ----------------------------------------------------
            {
                "type": "binary_sensor",
                "slug": "bt_connected",
                "name": f"{friendly} BT Connected",
                "device_class": "connectivity",
                "value_template": "{{ value_json.bt['connected'] }}",
                "payload_on": "True",
                "payload_off": "False",
            },
            {
                "type": "binary_sensor",
                "slug": "bt_paired",
                "name": f"{friendly} BT Paired",
                "device_class": "connectivity",
                "value_template": "{{ value_json.bt['paired'] }}",
                "payload_on": "True",
                "payload_off": "False",
            },
            {
                "type": "binary_sensor",
                "slug": "bt_bonded",
                "name": f"{friendly} BT Bonded",
                "device_class": "connectivity",
                "value_template": "{{ value_json.bt['bonded'] }}",
                "payload_on": "True",
                "payload_off": "False",
            },
            {
                "type": "binary_sensor",
                "slug": "bt_trusted",
                "name": f"{friendly} BT Trusted",
                "icon": "mdi:shield-check",
                "value_template": "{{ value_json.bt['trusted'] }}",
                "payload_on": "True",
                "payload_off": "False",
            },
            {
                "type": "binary_sensor",
                "slug": "bt_services_resolved",
                "name": f"{friendly} BT Services Resolved",
                "device_class": "connectivity",
                "value_template": "{{ value_json.bt['services_resolved'] }}",
                "payload_on": "True",
                "payload_off": "False",
            },
        ]
        # NOTE: RSSI is intentionally not published. BlueZ only exposes
        # org.bluez.Device1.RSSI while the device is advertising; a connected
        # LE central never gets a value, so the sensor would be permanently
        # unknown.

        for s in sensors:
            entity_type = s["type"]
            slug = s["slug"]
            config: Dict[str, Any] = {
                "name": s["name"],
                "unique_id": f"{uid}_{slug}",
                "state_topic": state_topic,
                "value_template": s["value_template"],
                "device": device,
                "availability": {
                    "topic": "/".join([self.get_device_topic(), self.AVAILABLE_TOPIC]),
                    "payload_available": "1",
                    "payload_not_available": "0",
                },
            }
            if "device_class" in s:
                config["device_class"] = s["device_class"]
            if "unit" in s:
                config["unit_of_measurement"] = s["unit"]
            if "state_class" in s:
                config["state_class"] = s["state_class"]
            if "icon" in s:
                config["icon"] = s["icon"]
            if entity_type == "binary_sensor":
                config["payload_on"] = s.get("payload_on", "True")
                config["payload_off"] = s.get("payload_off", "False")

            topic = f"homeassistant/{entity_type}/{uid}_{slug}/config"
            self.client.publish(topic, json.dumps(config), qos=1, retain=True)
            logger.debug(f"Published discovery for {entity_type} {slug}")

    def on_message(self,client, userdata, msg):
        """ Message received callback. See paho-mqtt docs for more details. """
        if msg.topic == "/".join([self.get_device_topic(), self.OPERATION_MODE_TOPIC,"set"]):
            asyncio.get_event_loop().create_task(set_operation_mode(self.controller,msg.payload))
        
        elif msg.topic == "/".join([self.get_device_topic(), self.FAN_SPEED_TOPIC,"set"]):
            asyncio.get_event_loop().create_task(set_fan_speed(self.controller,msg.payload))
        
        elif msg.topic == "/".join([self.get_device_topic(), self.POWER_STATE_TOPIC,"set"]):
            asyncio.get_event_loop().create_task(set_power_state(self.controller,msg.payload))
        
        elif msg.topic == "/".join([self.get_device_topic(), self.SET_POINT_TOPIC,"set"]):
            asyncio.get_event_loop().create_task(set_set_point_state(self.controller,msg.payload))
        
    
async def _get_bt_state(mac: str) -> dict:
    """Read Bluetooth link state for `mac` from BlueZ via D-Bus.

    Note on RSSI: BlueZ only populates `org.bluez.Device1.RSSI` while the
    device is advertising (i.e. during scanning); it is always None for a
    connected LE central, so we don't publish it — it would be a permanently
    unknown sensor.
    """
    path = "/org/bluez/hci0/dev_" + mac.upper().replace(":", "_")
    try:
        bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
        intr = await bus.introspect("org.bluez", path)
        obj = bus.get_proxy_object("org.bluez", path, intr)
        dev = obj.get_interface("org.bluez.Device1")
        state = {
            "connected":        await dev.get_connected(),
            "paired":           await dev.get_paired(),
            "bonded":           await dev.get_bonded(),
            "trusted":          await dev.get_trusted(),
            "services_resolved": await dev.get_services_resolved(),
        }
        bus.disconnect()
        return state
    except Exception as e:
        logger.warning(f"Could not read BT state from D-Bus: {e}")
        return {"connected": None, "paired": None, "bonded": None,
                "trusted": None, "services_resolved": None}


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return asyncio.run(f(*args, **kwargs))
        except KeyboardInterrupt:
            logger.info("User stopped program.")
        except Exception as e:
            logger.error("",e, stack_info=True)
        
    return wrapper

async def periodic_update(interval:int,controller:Controller,mqtt_service:MQTT):
    """ This routine is used to schedule the periodic update of the controller.

    Mirrors the upstream simple-loop model but never swallows CancelledError
    (that's what caused the original "Operation cancelled :" silent freeze)
    and always enriches the published status with BT link state.
    """
    reconnect = False
    while True:
        try:
            try:
                if reconnect:
                    await controller.start()
                    reconnect = False
                await controller.update()
                mqtt_service.available(True)
                status = controller.refresh_status()
                # Enrich with BT link state (connected/paired/bonded/trusted/
                # services_resolved/rssi). RSSI is null while connected — BlueZ
                # only populates it during advertising (BLE LE limitation).
                try:
                    status["bt"] = await _get_bt_state(controller.connection.address)
                except Exception as e:
                    logger.warning(f"BT state read failed: {e}")
                    status["bt"] = {"connected": None, "paired": None, "bonded": None,
                                    "trusted": None, "services_resolved": None, "rssi": None}
                mqtt_service.update(json.dumps(status, default=str))
            except ConnectionAbortedError as e:
                logger.warning(f"BLE link aborted: {e}")
                mqtt_service.available(False)
                reconnect = True
            except ConnectionException as e:
                logger.warning(f"BLE command failed: {e}")
                mqtt_service.available(False)
                reconnect = True
            except Exception as e:
                logger.error(f"Update cycle error (continuing): {type(e).__name__}: {e}")
            await asyncio.sleep(interval)
        except CancelledError:
            # Supervisor is shutting us down — propagate, never swallow.
            raise
        

@click.command()
@click.pass_context
@click.option('-a', '--address', required=True, type=str, help="Bluetooth MAC address of the thermostat")
@click.option('-c', '--config', required=True, type=click.Path(), help="MQTT config file")
@click.option('-d', '--adapter', required=False, type=str, default="hci0", show_default=True, help="Name of the Bluetooth adapter to be used for the connection")
@click.option('--force-disconnect/--not-force-disconnect', default="True", show_default=True, help="Should disconnect the device to ensure it is recognized (recommended)")
@click.option('-t', '--device-discovery-timeout', type=int, default=5, show_default=True, help = "Timeout for Bluetooth device scan in seconds")
@click.option('-o', '--log-output', required=False,type=click.Path(), help="Path to the log output file")
@click.option('--debug', is_flag=True, help="Enable debug logging")
@click.option('--verbose', is_flag=True, help="Enable versbose logging")
@click.version_option()
@coro
async def run(ctx,verbose,adapter,log_output,debug,address,force_disconnect, device_discovery_timeout, config):
    
    # We disable automatic reconnect on the controller so we
    # can handle it from here and send the available message
    # to the MQTT broker
    madoka = Controller(address, adapter = adapter, reconnect=False)
    
    ctx.obj = {}
    ctx.obj["madoka"] = madoka
    ctx.obj["loop"] = asyncio.get_event_loop()   
    ctx.obj["timeout"] = device_discovery_timeout
    ctx.obj["adapter"] = adapter
    ctx.obj["force_disconnect"] = force_disconnect
    with open(config, 'r') as stream:
        yml_config = yaml.safe_load(stream)
        ctx.obj["config"] = yml_config
    
    # Default to INFO so the journal shows lifecycle/state lines without --verbose.
    # --debug bumps it to DEBUG. The original code set level=None which dropped
    # everything below WARNING, making the bridge effectively silent in journalctl.
    if debug:
        logging_level = logging.DEBUG
    elif verbose:
        logging_level = logging.INFO
    else:
        logging_level = logging.INFO

    logging.basicConfig(level=logging_level,
                        filename=log_output,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    
    if force_disconnect:
        # First disconnect attempt — clears most "Connected" stale state.
        await force_device_disconnect(madoka.connection.address)

    # Resilient BLE start: if BlueZ is in InProgress / br-connection-canceled,
    # retry with a back-off and an extra disconnect between attempts. This is
    # the fragile path that used to require manual `bluetoothctl disconnect`.
    discovered_devices = []
    for attempt in range(1, 6):
        try:
            discovered_devices = await discover_devices(timeout=ctx.obj["timeout"], adapter=ctx.obj["adapter"])
            break
        except Exception as e:
            logger.warning(f"Device discovery attempt {attempt}/5 failed: {e}")
            if attempt < 5:
                await force_device_disconnect(madoka.connection.address)
                await asyncio.sleep(2 * attempt)
            else:
                logger.error("Device discovery failed after 5 attempts; continuing — periodic_update will retry")

    mqtt_service = MQTT(asyncio.get_event_loop(), madoka, yml_config)
    update_task = None
    try:
        # BLE start with retry — handles `org.bluez.Error.InProgress` after a
        # prior run left the link half-open. Up to 5 attempts with exponential
        # back-off and a disconnect between each.
        for attempt in range(1, 6):
            try:
                await madoka.start()
                break
            except Exception as e:
                logger.warning(f"BLE start attempt {attempt}/5 failed: {e}")
                if attempt < 5:
                    await force_device_disconnect(madoka.connection.address)
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise

        # read_info best-effort — missing model/sw strings shouldn't block start.
        try:
            await madoka.read_info()
        except Exception as e:
            logger.warning(f"read_info failed (continuing without device info): {e}")

        connect = await mqtt_service.connect()
        if not connect:
            logger.error("MQTT connect returned false; aborting")
            return

        # Publish discovery here (in async context), then yield to the loop so
        # paho-asyncio's socket writer callback runs and the bytes actually go
        # out before we move on. on_connect intentionally doesn't publish
        # discovery (it can't wait for flush without blocking the loop).
        mqtt_service.discovery()
        await asyncio.sleep(0.5)
        mqtt_service.discovery_sensors()
        await asyncio.sleep(1.0)
        mqtt_service.available(True)
        await asyncio.sleep(0.5)
        logger.info("Discovery published; entering periodic update loop")

        update_task = asyncio.create_task(periodic_update(yml_config["daemon"]["update_interval"], madoka, mqtt_service))
        await update_task

    except CancelledError:
        logger.info("Shutting down (cancelled).")
        raise
    except (ConnectionAbortedError, ConnectionRefusedError) as e:
        logger.error(f"Aborted: {e}")
    except Exception as e:
        logger.error(f"Fatal error in run(): {type(e).__name__}: {e}")
    finally:
        # Always signal unavailable + clean up so HA flips entities to
        # 'unavailable' instead of showing stale state.
        try:
            mqtt_service.available(False)
        except Exception:
            pass
        if update_task is not None and not update_task.done():
            update_task.cancel()
            try:
                await update_task
            except (CancelledError, Exception):
                pass
        try:
            mqtt_service.stop()
        except Exception:
            pass
        try:
            await madoka.stop()
        except Exception:
            pass


if __name__ == "__main__":  
      

    asyncio.run(run())        
    asyncio.get_event_loop().run_forever()

    
   
