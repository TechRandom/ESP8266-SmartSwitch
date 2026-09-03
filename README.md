# ESP8266 Smart Switch

Turn a dumb wall switch into a Home Assistant sensor.

Plug an ESP8266 into an outlet controlled by a physical switch. When the switch is on, the board joins Wi-Fi at a static IP. A small companion service pings each board and publishes `ON` / `OFF` to MQTT. Home Assistant can then turn smart lights (or anything else) on and off.

```
Wall switch ON  -> ESP boots, joins Wi-Fi -> 3 pings succeed -> MQTT "ON"
Wall switch OFF -> ESP loses power       -> 3 pings fail    -> MQTT "OFF"
```

Off detection waits until **5 seconds have passed since the last successful ping**. A reply anywhere in that window keeps the switch ON, so brief Wi-Fi dropouts do not flicker lights. A real off should report in about 5 seconds.

## Hardware

- ESP8266 board (NodeMCU, Wemos D1 mini, or similar)
- USB power supply plugged into the **switched** outlet
- A 2.4 GHz Wi-Fi network (ESP8266 cannot join 5 GHz)
- An always-on machine on the same LAN to run the ping bridge (CasaOS, a NAS, a Pi, etc.)
- An MQTT broker (Mosquitto is typical)
- Home Assistant with the MQTT integration

Use one ESP8266 per room. Each board needs its own hostname and static IP.

## How the pieces fit

| Piece | Role |
| --- | --- |
| `ESP8266-SmartSwitch.ino` | Joins Wi-Fi at a static IP and stays reachable |
| `config.h` | Your Wi-Fi, hostname, and IP (not committed) |
| `ping_mqtt_bridge.py` | Pings every node and publishes MQTT on change |
| `nodes.json` | Broker settings and the list of rooms (not committed) |
| `docker-compose.yml` | Optional way to run the bridge with restart-on-boot |

The bridge does **not** run on the ESP. The ESP has no MQTT client. That keeps firmware simple and lets one service watch every room.

## 1. Flash the ESP8266

### Arduino IDE

1. Install [Arduino IDE](https://www.arduino.cc/en/software) and the [ESP8266 board package](https://github.com/esp8266/Arduino#installing-with-boards-manager) (core **2.7 or newer**).
2. Select your board (Generic ESP8266, NodeMCU 1.0, LOLIN D1 mini, etc.).
3. Copy `config.h.example` to `config.h` in this folder.
4. Edit `config.h`:

```cpp
const char* WIFI_SSID     = "YOUR_WIFI_SSID";
const char* WIFI_PASSWORD = "YOUR_WIFI_PASSWORD";
const char* HOSTNAME      = "office-wall-switch";

IPAddress STATIC_IP(192, 168, 1, 150);
IPAddress GATEWAY(192, 168, 1, 1);
IPAddress SUBNET(255, 255, 255, 0);
IPAddress DNS_SERVER(192, 168, 1, 1);
```

Pick an unused LAN address **outside** your DHCP pool if you can. `GATEWAY` and `DNS_SERVER` are usually your router.

5. Upload the sketch.
6. The onboard LED blinks while connecting and stays **on** (active low) when Wi-Fi is up.
7. From a PC on the same network: `ping 192.168.1.150`

If ping fails:

- Confirm the SSID is 2.4 GHz and the name matches exactly
- Confirm the static IP is on the same subnet as your PC (`ipconfig` / `ip addr`)
- Confirm the LED is solid, not blinking
- Confirm the outlet actually has power

The firmware caches the access point channel and BSSID after the first successful join so later power-ons skip the slow channel scan. If you change SSID, reflash; the cache is ignored when the SSID changes.

### Extra rooms

Flash each additional board with a **different** `HOSTNAME` and `STATIC_IP`, for example:

| Room | Hostname | IP |
| --- | --- | --- |
| Office | `office-wall-switch` | `192.168.1.150` |
| Bedroom | `bedroom-wall-switch` | `192.168.1.151` |
| Living room | `living-room-wall-switch` | `192.168.1.152` |

## 2. Configure the ping bridge

Copy `nodes.example.json` to `nodes.json` and edit it:

```json
{
  "mqtt": {
    "broker": "192.168.1.10",
    "port": 1883,
    "user": "",
    "password": "",
    "client_id": "wall-switch-bridge"
  },
  "ping": {
    "interval_s": 0.3,
    "timeout_ms": 250,
    "probes": 1,
    "hits_to_on": 3,
    "off_window_s": 5,
    "min_on_s": 2
  },
  "nodes": [
    {
      "id": "office",
      "name": "Office Wall Switch",
      "host": "192.168.1.150"
    }
  ]
}
```

- `broker` is the MQTT host (often the same machine as Home Assistant).
- Leave `user` / `password` empty if the broker allows anonymous connections.
- `host` must match that room’s ESP static IP.
- MQTT topic defaults to `{id}/wall_switch/state` (override with `"topic"` if you want).
- Three successful checks in a row publish `ON`. `OFF` is published **5 seconds after the last successful ping** (`off_window_s`). One reply in that window keeps the node ON.

`config.h` and `nodes.json` are gitignored so you do not publish credentials.

## 3. Run the bridge

The service must run on a machine that is **always on** and can ping the ESPs and reach MQTT. Do not run it only on a desktop that sleeps.

### Docker (recommended)

```bash
cp nodes.example.json nodes.json
# edit nodes.json
docker compose up -d
docker logs -f wall-switch-bridge
```

`restart: unless-stopped` brings it back after reboot. `network_mode: host` and `NET_RAW` are required so ICMP ping works.

### CasaOS / ZimaOS

1. Copy this repo (or at least `ping_mqtt_bridge.py`, `nodes.json`, `requirements.txt`, and `docker-compose.yml`) to a folder such as `/DATA/AppData/wall-switch-bridge`.
2. If you use CasaOS app data, set the compose volume to that folder instead of `./`.
3. Start the stack from **Install a customized app** or:

```bash
cd /DATA/AppData/wall-switch-bridge
docker compose up -d
```

After editing `nodes.json`:

```bash
docker restart wall-switch-bridge
```

### Python directly

```bash
python -m pip install -r requirements.txt
python ping_mqtt_bridge.py
```

Use systemd, Task Scheduler, or equivalent if you are not using Docker.

Healthy logs look like:

```text
Loaded 1 node(s) from .../nodes.json
MQTT connected to 192.168.1.10:1883
Watching Office Wall Switch (192.168.1.150) every 100ms
office/wall_switch/state -> ON
```

## 4. Home Assistant

1. Run an MQTT broker if you do not already have one (Mosquitto add-on, CasaOS Mosquitto app, or Docker).
2. **Settings → Devices & services → Add integration → MQTT**
   - Broker: your MQTT host
   - Port: `1883`
   - Username / password only if the broker requires them
3. The bridge publishes Home Assistant MQTT discovery. After it is running you should get a binary sensor per node, for example `binary_sensor.office_wall_switch`.

Example automations:

```yaml
automation:
  - alias: Office lights follow wall switch on
    trigger:
      - platform: state
        entity_id: binary_sensor.office_wall_switch
        to: "on"
    action:
      - action: light.turn_on
        target:
          entity_id: light.office

  - alias: Office lights follow wall switch off
    trigger:
      - platform: state
        entity_id: binary_sensor.office_wall_switch
        to: "off"
    action:
      - action: light.turn_off
        target:
          entity_id: light.office
```

You can also trigger on the MQTT topic directly:

```yaml
trigger:
  - platform: mqtt
    topic: office/wall_switch/state
    payload: "ON"
```

The bridge publishes an availability topic `wall_switch_bridge/status` (`online` / `offline`) so Home Assistant can tell if the watcher itself is down.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| ESP LED keeps blinking | Wrong SSID/password, or the network is 5 GHz only |
| `ping` from a PC fails | ESP is offline, wrong subnet, or Wi-Fi client isolation |
| HA sensor stays off / unavailable | Bridge not running, MQTT not connected, or ESP not pingable |
| Lights flicker without touching the switch | Raise `off_window_s` (try `8`) |
| Works, then drops after a Wi-Fi rename | Reflash with the new SSID |
| Docker cannot ping but the host can | Need `network_mode: host` and `cap_add: NET_RAW` |
| Duplicate HA entities | Remove an old YAML MQTT sensor if discovery also created one |

Confirm in this order: ESP LED → `ping` the static IP from a PC → bridge logs → MQTT topic → Home Assistant entity.

## License

MIT. See [LICENSE](LICENSE).
