/*
 * ESP8266 wall-switch sensor
 *
 * Stay on Wi-Fi at a static IP so ping_mqtt_bridge.py can detect power.
 * Remembers the AP channel + BSSID so later power-ons skip the slow scan.
 *
 * Copy config.h.example to config.h and set Wi-Fi, hostname, and static IP.
 * Each room needs its own hostname and IP.
 *
 * Board: generic ESP8266 (NodeMCU, Wemos D1 mini, etc.)
 * Needs ESP8266 Arduino core 2.7 or newer.
 */

#include <EEPROM.h>
#include <ESP8266WiFi.h>
#include <ESP8266WiFiGratuitous.h>

extern "C" {
#include "user_interface.h"
}

#include "config.h"

const uint32_t AP_CACHE_MAGIC = 0xA5C0FFF1;
const uint32_t FAST_CONNECT_TIMEOUT_MS = 2500;

struct ApCache {
  uint32_t magic;
  uint32_t ssid_hash;
  int32_t channel;
  uint8_t bssid[6];
};

void preinit() {
  // Runs before RF init. 3 = skip calibration for a faster power-on.
  system_phy_set_powerup_option(3);
}

uint32_t ssidHash(const char* ssid) {
  uint32_t hash = 2166136261u;
  while (*ssid) {
    hash ^= static_cast<uint8_t>(*ssid++);
    hash *= 16777619u;
  }
  return hash;
}

bool waitForWifi(uint32_t timeoutMs) {
  const uint32_t start = millis();
  while (WiFi.status() != WL_CONNECTED && (millis() - start) < timeoutMs) {
    digitalWrite(LED_BUILTIN, !digitalRead(LED_BUILTIN));
    delay(1);
  }
  return WiFi.status() == WL_CONNECTED;
}

ApCache loadApCache() {
  ApCache cache{};
  EEPROM.begin(sizeof(ApCache));
  EEPROM.get(0, cache);
  if (cache.magic != AP_CACHE_MAGIC || cache.ssid_hash != ssidHash(WIFI_SSID) ||
      cache.channel < 1 || cache.channel > 13) {
    cache.magic = 0;
  }
  return cache;
}

void saveApCache() {
  ApCache next{};
  next.magic = AP_CACHE_MAGIC;
  next.ssid_hash = ssidHash(WIFI_SSID);
  next.channel = WiFi.channel();
  memcpy(next.bssid, WiFi.BSSID(), 6);

  ApCache current = loadApCache();
  if (current.magic == next.magic && current.ssid_hash == next.ssid_hash &&
      current.channel == next.channel && memcmp(current.bssid, next.bssid, 6) == 0) {
    return;
  }

  EEPROM.put(0, next);
  EEPROM.commit();
}

void connectWifi() {
  system_update_cpu_freq(SYS_CPU_160MHZ);

  WiFi.persistent(false);
  WiFi.setAutoConnect(false);
  WiFi.mode(WIFI_STA);
  WiFi.hostname(HOSTNAME);
  WiFi.setSleepMode(WIFI_NONE_SLEEP);
  WiFi.setAutoReconnect(true);
  WiFi.config(STATIC_IP, GATEWAY, SUBNET, DNS_SERVER);

  ApCache cache = loadApCache();
  bool connected = false;

  if (cache.magic == AP_CACHE_MAGIC) {
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD, cache.channel, cache.bssid);
    connected = waitForWifi(FAST_CONNECT_TIMEOUT_MS);
  }

  while (!connected) {
    WiFi.disconnect();
    delay(10);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    connected = waitForWifi(15000);
  }

  saveApCache();
  experimental::ESP8266WiFiGratuitous::stationKeepAliveSetIntervalMs(5000);
  digitalWrite(LED_BUILTIN, LOW);
}

void setup() {
  pinMode(LED_BUILTIN, OUTPUT);
  digitalWrite(LED_BUILTIN, HIGH);
  connectWifi();
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) {
    digitalWrite(LED_BUILTIN, HIGH);
    connectWifi();
  }
}
