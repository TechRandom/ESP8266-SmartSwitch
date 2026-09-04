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
import shutil
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import fcntl
except ImportError:
    fcntl = None

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
    use_arp: bool = True


@dataclass
class PingResult:
    ok: bool
    elapsed_ms: float
    reason: str
    detail: str = ""
    returncode: int | None = None


@dataclass
class Node:
    node_id: str
    name: str
    host: str
    topic: str
    consecutive_ok: int = 0
    consecutive_fail: int = 0
    state: str | None = None
    last_on_at: float | None = None
    last_success_at: float | None = None
    first_seen_at: float | None = None
    miss_started_at: float | None = None
    last_off_at: float | None = None
    last_ping: PingResult | None = None
    recent_reasons: deque[str] = field(default_factory=lambda: deque(maxlen=20))
    stats_ok: int = 0
    stats_arp: int = 0
    stats_miss: int = 0
    consecutive_arp: int = 0
    stats_started: float | None = None
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
        use_arp=bool(ping_raw.get("use_arp", True)),
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


def _ping_detail(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return ""
    for line in reversed(lines):
        lowered = line.lower()
        if "time=" in lowered or "packet" in lowered or "unreachable" in lowered or "timed out" in lowered:
            return line[:160]
    return lines[-1][:160]


def classify_ping(output: str, returncode: int | None, timed_out: bool) -> str:
    text = output.lower()
    if timed_out:
        return "proc_timeout"
    if "destination host unreachable" in text or "no route" in text:
        return "unreachable"
    if "unknown host" in text or "name or service not known" in text:
        return "dns"
    if "timed out" in text or "100% packet loss" in text or "0 received" in text:
        return "no_reply"
    if returncode == 0:
        return "reply"
    return f"rc={returncode}"


def ping_once(host: str, timeout_ms: int) -> PingResult:
    timeout_s = max(timeout_ms / 1000.0, 0.05)
    if IS_WINDOWS:
        cmd = ["ping", "-n", "1", "-w", str(timeout_ms), host]
    else:
        cmd = ["ping", "-c", "1", "-W", f"{timeout_s:.3f}", host]

    started = time.monotonic()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s + 0.15,
            check=False,
        )
        elapsed_ms = (time.monotonic() - started) * 1000
        output = (result.stdout or "") + (result.stderr or "")
        ok = result.returncode == 0
        return PingResult(
            ok=ok,
            elapsed_ms=elapsed_ms,
            reason="reply" if ok else classify_ping(output, result.returncode, False),
            detail=_ping_detail(output),
            returncode=result.returncode,
        )
    except subprocess.TimeoutExpired as exc:
        elapsed_ms = (time.monotonic() - started) * 1000
        output = ""
        if exc.stdout:
            output += exc.stdout if isinstance(exc.stdout, str) else exc.stdout.decode("utf-8", "replace")
        if exc.stderr:
            output += exc.stderr if isinstance(exc.stderr, str) else exc.stderr.decode("utf-8", "replace")
        return PingResult(
            ok=False,
            elapsed_ms=elapsed_ms,
            reason="proc_timeout",
            detail=_ping_detail(output) or f"killed after {elapsed_ms:.0f}ms",
            returncode=None,
        )
    except OSError as exc:
        elapsed_ms = (time.monotonic() - started) * 1000
        return PingResult(
            ok=False,
            elapsed_ms=elapsed_ms,
            reason="os_error",
            detail=str(exc)[:160],
        )


def ping_check(host: str, timeout_ms: int, probes: int) -> PingResult:
    """A check succeeds if any probe gets a reply."""
    last = PingResult(ok=False, elapsed_ms=0, reason="no_probe")
    for _ in range(probes):
        last = ping_once(host, timeout_ms)
        if last.ok:
            return last
    return last


_ARP_MISSING_LOGGED = False
_ARP_IFACE_LOGGED = False
_ARP_IFACE_CACHE: dict[str, tuple[str, bytes, bytes]] = {}
_VIRTUAL_IFACE_PREFIXES = ("docker", "br-", "veth", "virbr", "cni", "flannel", "tun", "tap", "wg", "lo")
SIOCGIFADDR = 0x8915
SIOCGIFNETMASK = 0x891B
ETH_P_ARP = 0x0806


def _mac_bytes(mac: str) -> bytes:
    return bytes(int(part, 16) for part in mac.split(":"))


def _ipv4_ifaces() -> list[tuple[str, str, str, bytes]]:
    """Return (name, ip, netmask, mac) for up IPv4 interfaces."""
    if fcntl is None or not hasattr(socket, "AF_PACKET"):
        return []
    found: list[tuple[str, str, str, bytes]] = []
    try:
        names = os.listdir("/sys/class/net")
    except OSError:
        return []
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for name in names:
            if name == "lo":
                continue
            try:
                req = struct.pack("256s", name.encode("utf-8")[:15])
                ip = socket.inet_ntoa(fcntl.ioctl(sock, SIOCGIFADDR, req)[20:24])
                mask = socket.inet_ntoa(fcntl.ioctl(sock, SIOCGIFNETMASK, req)[20:24])
                mac = Path(f"/sys/class/net/{name}/address").read_text(encoding="utf-8").strip()
                found.append((name, ip, mask, _mac_bytes(mac)))
            except (OSError, ValueError):
                continue
    finally:
        sock.close()
    return found


def _iface_for_host(host: str) -> tuple[str, bytes, bytes] | None:
    cached = _ARP_IFACE_CACHE.get(host)
    if cached:
        return cached
    try:
        target = struct.unpack("!I", socket.inet_aton(host))[0]
    except OSError:
        return None
    same_subnet: list[tuple[str, bytes, bytes]] = []
    others: list[tuple[str, bytes, bytes]] = []
    for name, ip, mask, mac in _ipv4_ifaces():
        ip_n = struct.unpack("!I", socket.inet_aton(ip))[0]
        mask_n = struct.unpack("!I", socket.inet_aton(mask))[0]
        entry = (name, mac, socket.inet_aton(ip))
        if (ip_n & mask_n) == (target & mask_n):
            same_subnet.append(entry)
        else:
            others.append(entry)

    def rank(item: tuple[str, bytes, bytes]) -> tuple[int, str]:
        name = item[0]
        virtual = name.startswith(_VIRTUAL_IFACE_PREFIXES)
        return (1 if virtual else 0, name)

    candidates = sorted(same_subnet, key=rank) or sorted(others, key=rank)
    if not candidates:
        return None
    chosen = candidates[0]
    _ARP_IFACE_CACHE[host] = chosen
    return chosen


def _linux_arp_once(host: str, timeout_ms: int) -> PingResult:
    """Ask for the node's MAC. ESP8266 often answers ARP when ICMP is blackholed."""
    global _ARP_IFACE_LOGGED
    if fcntl is None or not hasattr(socket, "AF_PACKET"):
        return PingResult(ok=False, elapsed_ms=0, reason="arp_unsupported")

    iface = _iface_for_host(host)
    if iface is None:
        return PingResult(ok=False, elapsed_ms=0, reason="arp_no_iface")

    ifname, src_mac, src_ip = iface
    if not _ARP_IFACE_LOGGED:
        log.info(
            "ARP probe via %s src %s",
            ifname,
            socket.inet_ntoa(src_ip),
        )
        _ARP_IFACE_LOGGED = True

    timeout_s = max(timeout_ms / 1000.0, 0.15)
    dst_ip = socket.inet_aton(host)
    packet = (
        b"\xff" * 6
        + src_mac
        + struct.pack("!H", ETH_P_ARP)
        + struct.pack(
            "!HHBBH6s4s6s4s",
            1,
            0x0800,
            6,
            4,
            1,
            src_mac,
            src_ip,
            b"\x00" * 6,
            dst_ip,
        )
    )
    started = time.monotonic()
    sock = None
    try:
        sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(ETH_P_ARP))
        sock.bind((ifname, 0))
        sock.send(packet)
        deadline = started + timeout_s
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return PingResult(
                    ok=False,
                    elapsed_ms=(time.monotonic() - started) * 1000,
                    reason="arp_fail",
                    detail=f"no ARP reply on {ifname}",
                )
            sock.settimeout(remaining)
            try:
                data = sock.recv(256)
            except (TimeoutError, socket.timeout):
                return PingResult(
                    ok=False,
                    elapsed_ms=(time.monotonic() - started) * 1000,
                    reason="arp_fail",
                    detail=f"no ARP reply on {ifname}",
                )
            if len(data) < 42:
                continue
            if struct.unpack("!H", data[12:14])[0] != ETH_P_ARP:
                continue
            spa = data[28:32]
            if spa == dst_ip:
                return PingResult(
                    ok=True,
                    elapsed_ms=(time.monotonic() - started) * 1000,
                    reason="arp_reply",
                    detail=f"{ifname} {host}",
                )
    except OSError as exc:
        _ARP_IFACE_CACHE.pop(host, None)
        return PingResult(ok=False, elapsed_ms=0, reason="arp_error", detail=str(exc)[:160])
    finally:
        if sock is not None:
            sock.close()


def arping_once(host: str, timeout_ms: int) -> PingResult:
    arping = shutil.which("arping")
    if not arping:
        return PingResult(ok=False, elapsed_ms=0, reason="arp_missing")

    timeout_s = max(timeout_ms / 1000.0, 0.2)
    deadline = max(1, int(round(timeout_s)))
    cmd = [arping, "-c", "1", "-w", str(deadline)]
    iface = _ARP_IFACE_CACHE.get(host)
    if iface:
        cmd.extend(["-I", iface[0]])
    cmd.append(host)
    started = time.monotonic()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=deadline + 0.3,
            check=False,
        )
        elapsed_ms = (time.monotonic() - started) * 1000
        output = (result.stdout or "") + (result.stderr or "")
        text = output.lower()
        ok = result.returncode == 0 or "unicast reply" in text or "bytes from" in text
        return PingResult(
            ok=ok,
            elapsed_ms=elapsed_ms,
            reason="arp_reply" if ok else "arp_fail",
            detail=_ping_detail(output) or output.strip()[:160],
            returncode=result.returncode,
        )
    except subprocess.TimeoutExpired:
        elapsed_ms = (time.monotonic() - started) * 1000
        return PingResult(ok=False, elapsed_ms=elapsed_ms, reason="arp_timeout")
    except OSError as exc:
        return PingResult(ok=False, elapsed_ms=0, reason="arp_error", detail=str(exc)[:160])


def arp_check(host: str, timeout_ms: int) -> PingResult:
    """ESP8266 often answers ARP when ICMP is blackholed."""
    global _ARP_MISSING_LOGGED
    if IS_WINDOWS:
        return PingResult(ok=False, elapsed_ms=0, reason="arp_skip")

    probe = _linux_arp_once(host, timeout_ms)
    if probe.reason not in {"arp_unsupported", "arp_no_iface", "arp_error"}:
        return probe

    fallback = arping_once(host, timeout_ms)
    if fallback.reason == "arp_missing" and not _ARP_MISSING_LOGGED:
        log.warning(
            "ARP probe failed (%s) and arping is not installed; "
            "recreate the container so iputils-arping is available",
            probe.reason,
        )
        _ARP_MISSING_LOGGED = True
    if fallback.ok:
        return fallback
    if probe.reason != "arp_unsupported":
        fallback.detail = f"{probe.reason}/{probe.detail or '-'} | {fallback.reason}".strip(" |")
    return fallback


def reach_check(host: str, timeout_ms: int, probes: int, use_arp: bool) -> PingResult:
    ping = ping_check(host, timeout_ms, probes)
    if ping.ok or not use_arp:
        return ping
    arp = arp_check(host, timeout_ms)
    if arp.ok:
        return arp
    if arp.reason not in {"arp_skip", "arp_missing"}:
        ping.detail = f"{ping.detail} | {arp.reason}".strip(" |")
    return ping


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

    def _reason_summary(self, node: Node) -> str:
        counts = Counter(node.recent_reasons)
        if not counts:
            return "none"
        return ", ".join(f"{reason} x{count}" for reason, count in counts.most_common())

    def _silent_for(self, node: Node, now: float) -> float:
        reference = node.last_success_at if node.last_success_at is not None else node.first_seen_at
        if reference is None:
            return 0.0
        return max(0.0, now - reference)

    def _log_stats_if_needed(self, node: Node, now: float) -> None:
        if node.stats_started is None:
            node.stats_started = now
            return
        if now - node.stats_started < 60:
            return
        total = node.stats_ok + node.stats_arp + node.stats_miss
        miss_pct = 100.0 * node.stats_miss / total if total else 0.0
        log.info(
            "%s 1m stats: %s icmp / %s arp / %s miss (%.1f%% down) reasons: %s",
            node.node_id,
            node.stats_ok,
            node.stats_arp,
            node.stats_miss,
            miss_pct,
            self._reason_summary(node),
        )
        node.stats_ok = 0
        node.stats_arp = 0
        node.stats_miss = 0
        node.stats_started = now

    def publish_state(self, node: Node, state: str) -> None:
        info = self.client.publish(node.topic, state, qos=1, retain=True)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            log.error("MQTT publish failed (%s %s): rc=%s", node.node_id, state, info.rc)
            return
        log.info("%s -> %s", node.topic, state)

    def watch_node(self, node: Node) -> None:
        settings = self.ping_settings
        log.info(
            "Watching %s (%s) every %sms (on after %s hits, off %.0fs after last ICMP/ARP, arp=%s)",
            node.name,
            node.host,
            int(settings.interval_s * 1000),
            settings.hits_to_on,
            settings.off_window_s,
            "on" if settings.use_arp else "off",
        )

        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                ping = reach_check(
                    node.host,
                    settings.timeout_ms,
                    settings.probes,
                    settings.use_arp,
                )
            except Exception:
                log.exception("Unexpected ping failure for %s", node.node_id)
                ping = PingResult(ok=False, elapsed_ms=0, reason="exception")

            now = time.monotonic()
            with node.lock:
                if node.first_seen_at is None:
                    node.first_seen_at = now
                if node.stats_started is None:
                    node.stats_started = now
                node.last_ping = ping
                node.recent_reasons.append(ping.reason)

                if ping.ok:
                    was_failing = node.consecutive_fail > 0
                    silent_for = self._silent_for(node, now)
                    if ping.reason == "arp_reply":
                        node.stats_arp += 1
                        node.consecutive_arp += 1
                        if node.consecutive_arp == 1 or node.consecutive_arp % 20 == 0:
                            log.info(
                                "%s ICMP blackout, ARP alive #%s %.0fms %s",
                                node.node_id,
                                node.consecutive_arp,
                                ping.elapsed_ms,
                                ping.detail or "-",
                            )
                    else:
                        node.stats_ok += 1
                        if node.consecutive_arp >= 3:
                            log.info(
                                "%s ICMP restored after %s ARP-only checks, rtt=%.0fms",
                                node.node_id,
                                node.consecutive_arp,
                                ping.elapsed_ms,
                            )
                        node.consecutive_arp = 0
                    if was_failing and (node.consecutive_fail >= 3 or silent_for >= 2.0):
                        log.info(
                            "%s recovered after %.1fs / %s misses (%s) rtt=%.0fms via %s",
                            node.node_id,
                            silent_for,
                            node.consecutive_fail,
                            self._reason_summary(node),
                            ping.elapsed_ms,
                            ping.reason,
                        )
                    node.last_success_at = now
                    node.consecutive_ok += 1
                    node.consecutive_fail = 0
                    node.miss_started_at = None
                    if node.consecutive_ok >= settings.hits_to_on and node.state != "ON":
                        off_for = (now - node.last_off_at) if node.last_off_at else 0.0
                        node.state = "ON"
                        node.last_on_at = now
                        log.info(
                            "%s publishing ON after %s hits, was off %.1fs, rtt=%.0fms",
                            node.node_id,
                            node.consecutive_ok,
                            off_for,
                            ping.elapsed_ms,
                        )
                        self.publish_state(node, node.state)
                else:
                    node.stats_miss += 1
                    node.consecutive_ok = 0
                    node.consecutive_fail += 1
                    if node.miss_started_at is None:
                        node.miss_started_at = now
                    silent_for = self._silent_for(node, now)
                    if node.state != "OFF" and (
                        node.consecutive_fail == 1
                        or node.consecutive_fail % 5 == 0
                        or silent_for >= settings.off_window_s - 0.35
                    ):
                        log.info(
                            "%s miss #%s silent=%.1fs/%ss %s %.0fms rc=%s %s",
                            node.node_id,
                            node.consecutive_fail,
                            silent_for,
                            settings.off_window_s,
                            ping.reason,
                            ping.elapsed_ms,
                            ping.returncode,
                            ping.detail or "-",
                        )

                reference = node.last_success_at if node.last_success_at is not None else node.first_seen_at
                held_on = node.last_on_at is not None and (now - node.last_on_at) < settings.min_on_s
                if (
                    node.state != "OFF"
                    and not ping.ok
                    and not held_on
                    and reference is not None
                    and now - reference >= settings.off_window_s
                ):
                    node.state = "OFF"
                    node.last_off_at = now
                    log.info(
                        "%s publishing OFF silent=%.1fs misses=%s held_on=%s reasons: %s last=%s",
                        node.node_id,
                        now - reference,
                        node.consecutive_fail,
                        held_on,
                        self._reason_summary(node),
                        ping.detail or ping.reason,
                    )
                    self.publish_state(node, node.state)

                self._log_stats_if_needed(node, now)

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
        "Ping every %sms, timeout %sms, on after %s hits, off %.0fs after last ICMP/ARP (arp %s)",
        int(ping_settings.interval_s * 1000),
        ping_settings.timeout_ms,
        ping_settings.hits_to_on,
        ping_settings.off_window_s,
        "on" if ping_settings.use_arp else "off",
    )
    bridge = Bridge(mqtt_settings, ping_settings, nodes)
    signal.signal(signal.SIGINT, bridge.request_stop)
    signal.signal(signal.SIGTERM, bridge.request_stop)
    return bridge.run()


if __name__ == "__main__":
    sys.exit(main())
