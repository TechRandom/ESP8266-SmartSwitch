"""
Ping wall-switch ESP8266 nodes and publish ON/OFF to MQTT on state changes.

A few successful pings in a row -> ON
OFF only after a long window with zero successful pings.

A node stays ON if it answers at all during the off window, so brief Wi-Fi
or ARP dropouts do not flicker lights. Real offs are slower by design.

Edit nodes.json to add rooms. Each node needs a unique id and static IP.

  pip install paho-mqtt
"""

from __future__ import annotations

import json
import logging
import os
import platform
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.environ.get("NODES_CONFIG", SCRIPT_DIR / "nodes.json"))
IS_WINDOWS = platform.system().lower() == "windows"
AVAILABILITY_TOPIC = "wall_switch_bridge/status"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("wall-switch-bridge")


@dataclass
class MqttSettings:
    broker: str
    port: int = 1883
    user: str = ""
    password: str = ""
    client_id: str = "wall-switch-bridge"


@dataclass
class PingSettings:
    interval_s: float = 0.3
    timeout_ms: int = 250
    probes: int = 1
    hits_to_on: int = 3
    off_window_s: float = 5.0
    min_on_s: float = 2.0


@dataclass
class Node:
    node_id: str
    name: str
    host: str
    topic: str
    consecutive_ok: int = 0
    state: str | None = None
    last_on_at: float | None = None
    last_success_at: float | None = None
    first_seen_at: float | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)


def load_config(path: Path) -> tuple[MqttSettings, PingSettings, list[Node]]:
    if not path.is_file():
        raise FileNotFoundError(f"Config not found: {path}")

    with path.open(encoding="utf-8") as handle:
        raw: dict[str, Any] = json.load(handle)

    mqtt_raw = raw.get("mqtt", {})
    ping_raw = raw.get("ping", {})
    nodes_raw = raw.get("nodes", [])

    mqtt_settings = MqttSettings(
        broker=str(mqtt_raw.get("broker", "127.0.0.1")),
        port=int(mqtt_raw.get("port", 1883)),
        user=str(mqtt_raw.get("user", "")),
        password=str(mqtt_raw.get("password", "")),
        client_id=str(mqtt_raw.get("client_id", "wall-switch-bridge")),
    )
    legacy_hits = ping_raw.get("hits_to_switch")
    if "off_window_s" in ping_raw:
        off_window_s = float(ping_raw["off_window_s"])
    elif "off_confirm_s" in ping_raw:
        off_window_s = max(float(ping_raw["off_confirm_s"]), 5.0)
    else:
        off_window_s = 5.0
    ping_settings = PingSettings(
        interval_s=float(ping_raw.get("interval_s", 0.3)),
        timeout_ms=int(ping_raw.get("timeout_ms", 250)),
        probes=max(1, int(ping_raw.get("probes", 1))),
        hits_to_on=int(ping_raw.get("hits_to_on", legacy_hits if legacy_hits is not None else 3)),
        off_window_s=off_window_s,
        min_on_s=float(ping_raw.get("min_on_s", 2.0)),
    )

    nodes: list[Node] = []
    seen_ids: set[str] = set()
    seen_hosts: set[str] = set()
    for entry in nodes_raw:
        node_id = str(entry["id"]).strip()
        host = str(entry["host"]).strip()
        if not node_id or not host:
            raise ValueError(f"Node is missing id/host: {entry}")
        if node_id in seen_ids:
            raise ValueError(f"Duplicate node id: {node_id}")
        if host in seen_hosts:
            raise ValueError(f"Duplicate node host: {host}")
        seen_ids.add(node_id)
        seen_hosts.add(host)
        nodes.append(
            Node(
                node_id=node_id,
                name=str(entry.get("name", f"{node_id} Wall Switch")),
                host=host,
                topic=str(entry.get("topic", f"{node_id}/wall_switch/state")),
            )
        )

    if not nodes:
        raise ValueError("nodes.json must list at least one node")

    return mqtt_settings, ping_settings, nodes


def ping_once(host: str, timeout_ms: int) -> bool:
    timeout_s = max(timeout_ms / 1000.0, 0.05)
    if IS_WINDOWS:
        cmd = ["ping", "-n", "1", "-w", str(timeout_ms), host]
    else:
        cmd = ["ping", "-c", "1", "-W", f"{timeout_s:.3f}", host]

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout_s + 0.15,
            check=False,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError) as exc:
        log.debug("Ping error for %s: %s", host, exc)
        return False


def ping_check(host: str, timeout_ms: int, probes: int) -> bool:
    """A check succeeds if any probe gets a reply."""
    for _ in range(probes):
        if ping_once(host, timeout_ms):
            return True
    return False


def discovery_topic(node: Node) -> str:
    return f"homeassistant/binary_sensor/wall_switch_{node.node_id}/config"


def discovery_payload(node: Node) -> str:
    return json.dumps(
        {
            "name": node.name,
            "unique_id": f"wall_switch_{node.node_id}",
            "state_topic": node.topic,
            "payload_on": "ON",
            "payload_off": "OFF",
            "device_class": "power",
            "availability_topic": AVAILABILITY_TOPIC,
            "payload_available": "online",
            "payload_not_available": "offline",
            "device": {
                "identifiers": ["wall-switch-bridge"],
                "name": "Wall Switch Bridge",
                "manufacturer": "DIY",
                "model": "ESP8266 ping bridge",
            },
        }
    )


class Bridge:
    def __init__(self, mqtt_settings: MqttSettings, ping_settings: PingSettings, nodes: list[Node]):
        self.mqtt_settings = mqtt_settings
        self.ping_settings = ping_settings
        self.nodes = nodes
        self.stop_event = threading.Event()
        self.client = self._build_client()
        self.threads: list[threading.Thread] = []

    def _build_client(self) -> mqtt.Client:
        try:
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=self.mqtt_settings.client_id,
                protocol=mqtt.MQTTv311,
            )
        except AttributeError:
            client = mqtt.Client(
                client_id=self.mqtt_settings.client_id,
                protocol=mqtt.MQTTv311,
            )

        if self.mqtt_settings.user:
            client.username_pw_set(self.mqtt_settings.user, self.mqtt_settings.password)

        client.will_set(AVAILABILITY_TOPIC, "offline", qos=1, retain=True)
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        return client

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        rc = getattr(reason_code, "value", reason_code)
        if rc != 0:
            log.error("MQTT connect failed: %s", reason_code)
            return

        log.info("MQTT connected to %s:%s", self.mqtt_settings.broker, self.mqtt_settings.port)
        client.publish(AVAILABILITY_TOPIC, "online", qos=1, retain=True)
        for node in self.nodes:
            client.publish(discovery_topic(node), discovery_payload(node), qos=1, retain=True)
            if node.state:
                client.publish(node.topic, node.state, qos=1, retain=True)

    def _on_disconnect(self, client, userdata, *args):
        log.warning("MQTT disconnected; will retry")

    def publish_state(self, node: Node, state: str) -> None:
        info = self.client.publish(node.topic, state, qos=1, retain=True)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            log.error("MQTT publish failed (%s %s): rc=%s", node.node_id, state, info.rc)
            return
        log.info("%s -> %s", node.topic, state)

    def watch_node(self, node: Node) -> None:
        settings = self.ping_settings
        log.info(
            "Watching %s (%s) every %sms (on after %s hits, off %.0fs after last reply)",
            node.name,
            node.host,
            int(settings.interval_s * 1000),
            settings.hits_to_on,
            settings.off_window_s,
        )

        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                alive = ping_check(node.host, settings.timeout_ms, settings.probes)
            except Exception:
                log.exception("Unexpected ping failure for %s", node.node_id)
                alive = False

            now = time.monotonic()
            with node.lock:
                if node.first_seen_at is None:
                    node.first_seen_at = now
                if alive:
                    node.last_success_at = now
                    node.consecutive_ok += 1
                    if node.consecutive_ok >= settings.hits_to_on and node.state != "ON":
                        node.state = "ON"
                        node.last_on_at = now
                        self.publish_state(node, node.state)
                else:
                    node.consecutive_ok = 0

                reference = node.last_success_at if node.last_success_at is not None else node.first_seen_at
                held_on = node.last_on_at is not None and (now - node.last_on_at) < settings.min_on_s
                if (
                    node.state != "OFF"
                    and not alive
                    and not held_on
                    and reference is not None
                    and now - reference >= settings.off_window_s
                ):
                    node.state = "OFF"
                    self.publish_state(node, node.state)

            remaining = settings.interval_s - (time.monotonic() - started)
            if remaining > 0:
                self.stop_event.wait(remaining)

    def run(self) -> int:
        try:
            self.client.connect(self.mqtt_settings.broker, self.mqtt_settings.port, keepalive=30)
        except OSError as exc:
            log.error(
                "Could not reach MQTT broker %s:%s: %s",
                self.mqtt_settings.broker,
                self.mqtt_settings.port,
                exc,
            )
            return 1

        self.client.loop_start()

        for node in self.nodes:
            thread = threading.Thread(
                target=self.watch_node,
                args=(node,),
                name=f"ping-{node.node_id}",
                daemon=True,
            )
            thread.start()
            self.threads.append(thread)

        try:
            while not self.stop_event.is_set():
                self.stop_event.wait(0.5)
        except KeyboardInterrupt:
            log.info("Stopped")
        finally:
            self.stop_event.set()
            for thread in self.threads:
                thread.join(timeout=2)
            try:
                self.client.publish(AVAILABILITY_TOPIC, "offline", qos=1, retain=True)
            except Exception:
                pass
            self.client.loop_stop()
            self.client.disconnect()

        return 0

    def request_stop(self, *_args) -> None:
        self.stop_event.set()


def main() -> int:
    try:
        mqtt_settings, ping_settings, nodes = load_config(CONFIG_PATH)
    except (OSError, json.JSONDecodeError, KeyError, ValueError) as exc:
        log.error("Invalid config %s: %s", CONFIG_PATH, exc)
        return 1

    log.info("Loaded %s node(s) from %s", len(nodes), CONFIG_PATH)
    log.info(
        "Ping every %sms, timeout %sms, on after %s hits, off %.0fs after last reply",
        int(ping_settings.interval_s * 1000),
        ping_settings.timeout_ms,
        ping_settings.hits_to_on,
        ping_settings.off_window_s,
    )
    bridge = Bridge(mqtt_settings, ping_settings, nodes)
    signal.signal(signal.SIGINT, bridge.request_stop)
    signal.signal(signal.SIGTERM, bridge.request_stop)
    return bridge.run()


if __name__ == "__main__":
    sys.exit(main())
