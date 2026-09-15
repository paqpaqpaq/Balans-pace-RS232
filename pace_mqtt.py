#!/usr/bin/env python3

import json
import os
import sqlite3
import signal
from pathlib import Path
import serial
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import paho.mqtt.client as mqtt

from flask import (
    Flask,
    abort,
    jsonify,
    redirect,
    render_template_string,
    request,
    url_for,
)


# ============================================================
# CONFIG
# ============================================================

# Configureer vaste USB-poorten via /etc/pace-bms.env.
SERIAL_PORTS = {
    "BMS1": os.environ.get("PACE_SERIAL_BMS1", "/dev/serial/by-path/CHANGE-ME-BMS1"),
    "BMS2": os.environ.get("PACE_SERIAL_BMS2", "/dev/serial/by-path/CHANGE-ME-BMS2"),
    "BMS3": os.environ.get("PACE_SERIAL_BMS3", "/dev/serial/by-path/CHANGE-ME-BMS3"),
}
BAUD = 9600

LIVE_POLL_INTERVAL = 1.0
MQTT_PUBLISH_INTERVAL = 5.0

MQTT_HOST = os.environ.get("PACE_MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("PACE_MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("PACE_MQTT_USER", "")
MQTT_PASSWORD = os.environ.get("PACE_MQTT_PASSWORD", "")

BASE_TOPIC = os.environ.get("PACE_MQTT_BASE_TOPIC", "bmsrs232").strip("/")

GLOBAL_AVAILABILITY = (
    f"{BASE_TOPIC}/availability"
)

BMS_ADDRESSES = {
    1: "BMS1",
    2: "BMS2",
    3: "BMS3",
}

WEB_HOST = os.environ.get("PACE_WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.environ.get("PACE_WEB_PORT", "8080"))

PARAM_ADDRESS = 0x00


# ============================================================
# GLOBALS
# ============================================================

app = Flask(__name__)

serial_locks = {bms: threading.RLock() for bms in BMS_ADDRESSES.values()}
serial_connections = {bms: None for bms in BMS_ADDRESSES.values()}
connection_health = {bms: {"path": SERIAL_PORTS[bms], "verified": False,
                          "error": "Nog niet verbonden"} for bms in BMS_ADDRESSES.values()}
io_context = threading.local()

def current_bms():
    # No implicit BMS1 fallback for writes or worker bugs.
    bms = getattr(io_context, "bms", None)
    if bms not in SERIAL_PORTS:
        raise RuntimeError("Geen BMS geselecteerd voor seriële opdracht")
    return bms

@app.before_request
def select_parameter_target():
    io_context.bms = None
    if request.path.startswith("/parameters/"):
        bms = request.form.get("bms")
        if bms not in SERIAL_PORTS:
            abort(400, description="Selecteer BMS1, BMS2 of BMS3 via het eigen parameterpaneel.")
        io_context.bms = bms

cache_lock = threading.RLock()

mqtt_client = None

live_cache = {
    "BMS1": None,
    "BMS2": None,
    "BMS3": None,
}

capacity_cache = {}
capacity_errors = {}

parameter_cache = {bms: None for bms in BMS_ADDRESSES.values()}

last_parameter_error = {bms: None for bms in BMS_ADDRESSES.values()}
last_action_message = {bms: None for bms in BMS_ADDRESSES.values()}

last_mqtt_publish = {
    "BMS1": 0.0,
    "BMS2": 0.0,
    "BMS3": 0.0,
}

# Laatst geziene RAW44 per pack.
# We loggen alleen opnieuw zodra iets in het frame verandert.
last_raw44 = {
    "BMS1": None,
    "BMS2": None,
    "BMS3": None,
}


# ============================================================
# PACE FRAME HELPERS
# ============================================================

def pace_checksum_ascii(frame_without_checksum):
    raw = (
        frame_without_checksum[1:]
        .encode("ascii")
    )

    checksum = (
        (~sum(raw) + 1)
        & 0xFFFF
    )

    return f"{checksum:04X}"


def make_lenid(data_ascii_length):
    length = (
        data_ascii_length
        & 0x0FFF
    )

    n1 = (length >> 8) & 0x0F
    n2 = (length >> 4) & 0x0F
    n3 = length & 0x0F

    nibble_checksum = (
        (~(n1 + n2 + n3) + 1)
        & 0x0F
    )

    return (
        (nibble_checksum << 12)
        | length
    )


def make_param_frame(cid2, payload=b""):
    payload_ascii = (
        payload
        .hex()
        .upper()
    )

    lenid = make_lenid(
        len(payload_ascii)
    )

    base = (
        f"~25"
        f"{PARAM_ADDRESS:02X}"
        f"46"
        f"{cid2:02X}"
        f"{lenid:04X}"
        f"{payload_ascii}"
    )

    checksum = (
        pace_checksum_ascii(base)
    )

    return (
        base
        + checksum
        + "\r"
    ).encode("ascii")


def build_request(address, cid2, selector=1):
    base = (
        f"~25"
        f"{address:02X}"
        f"46"
        f"{cid2:02X}"
        f"E002"
        f"{selector:02X}"
    )

    checksum = (
        pace_checksum_ascii(base)
    )

    return (
        base
        + checksum
        + "\r"
    ).encode("ascii")


# ============================================================
# SERIAL
# ============================================================

def decode_frame(reply):
    text = reply.decode("ascii", errors="strict").strip()
    if not text.startswith("~") or len(text) < 17:
        raise ValueError("Ongeldig of te kort PACE frame")
    body = text[1:]
    if len(body) % 2 or any(c not in "0123456789abcdefABCDEF" for c in body):
        raise ValueError("Ongeldige hex in PACE frame")
    if pace_checksum_ascii(text[:-4]).upper() != text[-4:].upper():
        raise ValueError("PACE checksum mismatch")
    raw = bytes.fromhex(body)
    if raw[0] != 0x25 or raw[2] != 0x46:
        raise ValueError("Onverwachte PACE versie of CID1")
    length = int.from_bytes(raw[4:6], "big")
    if length != make_lenid((len(raw)-8)*2):
        raise ValueError("PACE antwoordlengte/LENID mismatch")
    if raw[3] != 0:
        raise ValueError(f"BMS retourcode 0x{raw[3]:02X}")
    return raw[6:-2]


def exchange(connection, request_bytes, wait, expected_address):
    connection.reset_input_buffer()
    connection.write(request_bytes)
    connection.flush()
    if stop_event.wait(wait):
        raise RuntimeError("Service stopt")
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and not stop_event.is_set():
        reply = connection.read_until(b"\r")
        if not reply:
            continue
        if reply.strip() == request_bytes.strip():
            continue  # Adapter echo is nooit een geldig BMS-antwoord.
        decode_frame(reply)
        address = int(reply.decode("ascii").strip()[3:5], 16)
        if address != expected_address:
            raise ValueError(f"Verkeerde BMS aan kabel: antwoordadres {address}, verwacht {expected_address}")
        return reply
    raise RuntimeError("Geen geldig BMS-antwoord (timeout of alleen echo)")


def resolve_serial_path(bms):
    # Both aliases refer to the SAME physical hub socket. Resolve again on reconnect;
    # ttyUSB numbers may change after unplugging or rebooting.
    primary = SERIAL_PORTS[bms]
    alias = primary.replace(".usb-usb-", ".usb-usbv2-")
    for candidate in dict.fromkeys((primary, alias)):
        if os.path.exists(candidate):
            return candidate
    raise RuntimeError(f"{bms}: USB-kabel niet aanwezig op de ingestelde hubpoort; opnieuw verbinden volgt automatisch")


def transact(request, wait=0.18):
    bms = current_bms()
    expected = next(a for a, name in BMS_ADDRESSES.items() if name == bms)
    with serial_locks[bms]:
        if stop_event.is_set():
            raise RuntimeError("Service stopt")
        connection = serial_connections[bms]
        try:
            if connection is None:
                active_path = resolve_serial_path(bms)
                connection = serial.Serial(active_path, baudrate=BAUD, bytesize=8,
                                           parity="N", stopbits=1, timeout=0.5, write_timeout=2)
                serial_connections[bms] = connection
                # Local read identifies the physical pack BEFORE any parameter write.
                identity = exchange(connection, make_param_frame(0xA6), 0.30, expected)
                if len(decode_frame(identity)) != 6:
                    raise ValueError("Lokale A6-identificatie heeft geen 6 capaciteitbytes")
                with cache_lock:
                    connection_health[bms].update(verified=True, error=None, active_path=active_path)
                print(f"{bms}: eigen RS232 geverifieerd · {active_path}", flush=True)
            reply = exchange(connection, request, wait, expected)
            with cache_lock:
                connection_health[bms].update(error=None, last_response=time.time())
            return reply
        except Exception as exc:
            # Do not retry a write: after a lost ACK its outcome is unknown.
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
            serial_connections[bms] = None
            with cache_lock:
                connection_health[bms].update(verified=False, error=str(exc))
            raise


def param_request(cid2):
    return decode_frame(
        transact(
            make_param_frame(cid2),
            wait=0.30,
        )
    )


def param_write(cid2, payload):
    return decode_frame(
        transact(
            make_param_frame(
                cid2,
                payload,
            ),
            wait=0.35,
        )
    )


def u16(data, pos):
    return int.from_bytes(
        data[pos:pos + 2],
        "big",
        signed=False,
    )


def s16(data, pos):
    return int.from_bytes(
        data[pos:pos + 2],
        "big",
        signed=True,
    )


# ============================================================
# ANALOG 0x42
# ============================================================

def parse_analog(payload):
    pos = 0

    # marker
    pos += 1

    address = payload[pos]
    pos += 1

    cell_count = payload[pos]
    pos += 1

    cells = []

    for _ in range(cell_count):

        cells.append(
            u16(
                payload,
                pos
            )
            / 1000.0
        )

        pos += 2

    temp_count = payload[pos]
    pos += 1

    temperatures = []

    for _ in range(temp_count):

        raw = u16(
            payload,
            pos
        )

        pos += 2

        temperatures.append(
            raw / 10.0
            - 273.15
        )

    current = (
        s16(
            payload,
            pos
        )
        / 100.0
    )

    pos += 2

    voltage = (
        u16(
            payload,
            pos
        )
        / 1000.0
    )

    pos += 2

    remaining_capacity_ah = (
        u16(
            payload,
            pos
        )
        * 10
        / 1000.0
    )

    pos += 2

    user_defined = payload[pos]
    pos += 1

    full_capacity_ah = (
        u16(
            payload,
            pos
        )
        * 10
        / 1000.0
    )

    pos += 2

    cycle_count = u16(
        payload,
        pos
    )

    pos += 2

    design_capacity_ah = (
        u16(
            payload,
            pos
        )
        * 10
        / 1000.0
    )

    if full_capacity_ah > 0:

        soc = (
            remaining_capacity_ah
            / full_capacity_ah
            * 100.0
        )

    else:

        soc = 0.0

    if design_capacity_ah > 0:

        soh = (
            full_capacity_ah
            / design_capacity_ah
            * 100.0
        )

    else:

        soh = 0.0

    return {
        "address":
            address,

        "cells":
            cells,

        "temperatures":
            temperatures,

        "current":
            current,

        "voltage":
            voltage,

        "power":
            voltage * current,

        "remaining_capacity_ah":
            remaining_capacity_ah,

        "full_capacity_ah":
            full_capacity_ah,

        "design_capacity_ah":
            design_capacity_ah,

        "cycle_count":
            cycle_count,

        "soc":
            soc,

        "soh":
            soh,

        "user_defined":
            user_defined,
    }


# ============================================================
# STATUS 0x44
#
# PACE A-Series layout, passend bij onze 16S / 6-temp frames:
#   29 Protection state 1
#   30 Protection state 2   (bit7 = Fully)
#   31 Instructions state   (bit1 CFET, bit2 DFET, bit5 ACin)
#   32 Control state
#   33 Fault state
#   34 Balance state 1      (C01..C08)
#   35 Balance state 2      (C09..C16)
#   36 Warning state 1
#   37 Warning state 2
#   38 Firmware-extra / onbekend
# ============================================================

def parse_status(payload):
    if len(payload)<4 or not 1<=payload[2]<=16:
        raise ValueError('Ongeldig statusframe/celaantal')
    temp_offset=3+payload[2]
    if temp_offset>=len(payload) or not 1<=payload[temp_offset]<=16:
        raise ValueError('Ongeldig statusframe/temperatuuraantal')
    if len(payload)<temp_offset+1+payload[temp_offset]+12:
        raise ValueError('Afgekapt statusframe')
    pos = 0

    marker_offset = pos
    marker = payload[pos]
    pos += 1

    address_offset = pos
    address = payload[pos]
    pos += 1

    cell_count_offset = pos
    cell_count = payload[pos]
    pos += 1

    cell_warning_offset = pos
    cell_warnings = list(payload[pos:pos + cell_count])
    pos += cell_count

    temp_count_offset = pos
    temp_count = payload[pos]
    pos += 1

    temp_warning_offset = pos
    temp_warnings = list(payload[pos:pos + temp_count])
    pos += temp_count

    charge_current_warning_offset = pos
    charge_current_warning = payload[pos]
    pos += 1

    total_voltage_warning_offset = pos
    total_voltage_warning = payload[pos]
    pos += 1

    discharge_current_warning_offset = pos
    discharge_current_warning = payload[pos]
    pos += 1

    protection1_offset = pos
    protection1 = payload[pos]
    pos += 1

    protection2_offset = pos
    protection2 = payload[pos]
    pos += 1

    instructions_offset = pos
    instructions = payload[pos]
    pos += 1

    control_offset = pos
    control = payload[pos]
    pos += 1

    fault_offset = pos
    fault = payload[pos]
    pos += 1

    balance1_offset = pos
    balance1 = payload[pos]
    pos += 1

    balance2_offset = pos
    balance2 = payload[pos]
    pos += 1

    warning1_offset = pos
    warning1 = payload[pos]
    pos += 1

    warning2_offset = pos
    warning2 = payload[pos]
    pos += 1

    extra_offset = pos
    extra = payload[pos:]

    return {
        "address": address,
        "marker": marker,
        "cell_count": cell_count,
        "temp_count": temp_count,
        "cell_warnings": cell_warnings,
        "temp_warnings": temp_warnings,
        "charge_current_warning": charge_current_warning,
        "total_voltage_warning": total_voltage_warning,
        "discharge_current_warning": discharge_current_warning,
        "protection1": protection1,
        "protection2": protection2,
        "instructions": instructions,
        "system": instructions,  # compatibiliteit met bestaande code / MQTT
        "control": control,
        "fault_state": fault,
        "balance1": balance1,
        "balance2": balance2,
        "warning1": warning1,
        "warning2": warning2,
        "extra": extra,
        "raw_payload": bytes(payload),
        "offsets": {
            "marker": marker_offset,
            "address": address_offset,
            "cell_count": cell_count_offset,
            "cell_warnings": cell_warning_offset,
            "temp_count": temp_count_offset,
            "temp_warnings": temp_warning_offset,
            "charge_current_warning": charge_current_warning_offset,
            "total_voltage_warning": total_voltage_warning_offset,
            "discharge_current_warning": discharge_current_warning_offset,
            "protection1": protection1_offset,
            "protection2": protection2_offset,
            "instructions": instructions_offset,
            "control": control_offset,
            "fault_state": fault_offset,
            "balance1": balance1_offset,
            "balance2": balance2_offset,
            "warning1": warning1_offset,
            "warning2": warning2_offset,
            "extra": extra_offset,
        },
    }

# ============================================================
# RAW 0x44 DEBUG
# ============================================================

def raw_hex(payload):
    return (
        payload
        .hex(" ")
        .upper()
    )


def raw_indexed(payload):
    return " ".join(
        f"{i:02d}:{b:02X}"
        for i, b in enumerate(payload)
    )


def log_raw44_change(bms, payload, status):
    # Full payload remains available through API and MQTT, without console flood.
    return


# ============================================================
# TEXT DECODERS
# ============================================================

def warning_value(v):
    if v == 0x00:
        return None

    if v == 0x01:
        return "LOW"

    if v == 0x02:
        return "HIGH"

    if v == 0xF0:
        return "overige fout"

    return (
        f"onbekend 0x{v:02X}"
    )


def balancing_cells(status):
    cells = []

    for bit in range(8):
        if status["balance1"] & (1 << bit):
            cells.append(bit + 1)

    for bit in range(8):
        if status["balance2"] & (1 << bit):
            cells.append(bit + 9)

    return cells


def balancing_text(status):
    cells = balancing_cells(status)

    if not cells:
        return "geen"

    return " | ".join(
        f"C{cell:02d}"
        for cell in cells
    )


def individual_alarms_text(s):
    items = []

    for i, raw in enumerate(
        s["cell_warnings"],
        start=1,
    ):

        value = warning_value(raw)

        if value:

            items.append(
                f"Cel {i:02d} "
                f"{value}"
            )

    for i, raw in enumerate(
        s["temp_warnings"],
        start=1,
    ):

        value = warning_value(raw)

        if value:

            items.append(
                f"Temperatuur {i} "
                f"{value}"
            )

    value = warning_value(
        s[
            "charge_current_warning"
        ]
    )

    if value:

        items.append(
            f"Laadstroom {value}"
        )

    value = warning_value(
        s[
            "discharge_current_warning"
        ]
    )

    if value:

        items.append(
            f"Ontlaadstroom {value}"
        )

    value = warning_value(
        s[
            "total_voltage_warning"
        ]
    )

    if value:

        items.append(
            f"Packspanning {value}"
        )

    return (
        " | ".join(items)
        if items
        else "geen"
    )


# A-Series tables A.24/A.25 and nkinnan V25 agree on these warning bits.
# Sources: https://github.com/nkinnan/esphome-pace-bms/blob/main/components/pace_bms/pace_bms_protocol_v25.h
# https://www.scribd.com/document/881727973/A-Series-RS232commuciation-Protocal-PACE-RS232-TY16S-20180705
WARNING1_BITS = {0:'Cel OV',1:'Cel UV',2:'Pack OV',3:'Pack UV',4:'Laad-overstroom',5:'Ontlaad-overstroom'}
WARNING2_BITS = {0:'Laden te warm',1:'Ontladen te warm',2:'Laden te koud',3:'Ontladen te koud',
                 4:'Omgeving te warm',5:'Omgeving te koud',6:'MOSFET te warm',7:'Lage SOC'}

def decode_bits(value, mapping):
    items=[label for bit,label in mapping.items() if value & (1<<bit)]
    unknown=value & ~sum(1<<bit for bit in mapping)
    if unknown: items.append(f'Onbekende bits 0x{unknown:02X}')
    return items

def alarms_text(status):
    individual=individual_alarms_text(status)
    parts=[] if individual=='geen' else [individual]
    for field,mapping in [('warning1',WARNING1_BITS),('warning2',WARNING2_BITS)]:
        decoded=decode_bits(status[field],mapping)
        if decoded: parts.append(field+': '+', '.join(decoded))
    return ' | '.join(parts) if parts else 'geen'

def decoded_status(status):
    return {
        'warning1':decode_bits(status['warning1'],WARNING1_BITS),
        'warning2':decode_bits(status['warning2'],WARNING2_BITS),
        'protection':protection_text(status),'faults':faults_text(status),
        'balancing_cells':balancing_cells(status),'fully_charged':bool(status['protection2'] & 0x80),
        'charge_state_0x20':bool(status['instructions'] & 0x20),
        'reverse_indication':bool(status['instructions'] & 0x10),
        'buzzer_enabled':bool(status['control'] & 0x01),
        'limiter_control_bit4':bool(status['control'] & 0x10),
        'limiter_instruction_bit0':bool(status['instructions'] & 0x01),
        'limiter_gear_configuration':'low' if status['control'] & 0x08 else 'high',
        'limiter_note':'PACE v25: control bit4=limiter geconfigureerd, bit3=low gear; instruction bit0=limiter runtime uit. Geen numerieke CAN CCL.',
        'instructions_other_raw':f"0x{status['instructions'] & 0xC9:02X}",
        'control_other_raw':f"0x{status['control'] & 0xE6:02X}",
        'extra_raw':raw_hex(status['extra'])}

def protection_text(s):
    items = []

    p1 = [
        (0x80, "P1 bit7: ongedocumenteerd voor A-Series"),
        (0x40, "Kortsluiting"),
        (0x20, "Ontlaad-overstroom"),
        (0x10, "Laad-overstroom"),
        (0x08, "Pack UVP"),
        (0x04, "Pack OVP"),
        (0x02, "Cell UVP"),
        (0x01, "Cell OVP"),
    ]

    # In de A-Series layout is bit7 van Protection State 2 'Fully'.
    # De overige temperatuurbits blijven volgens dezelfde PACE-familie.
    p2 = [
        (0x40, "Omgeving te koud"),
        (0x20, "Omgeving te warm"),
        (0x10, "MOSFET te warm"),
        (0x08, "Ontladen te koud"),
        (0x04, "Laden te koud"),
        (0x02, "Ontladen te warm"),
        (0x01, "Laden te warm"),
    ]

    for bit, name in p1:
        if s["protection1"] & bit:
            items.append(name)

    for bit, name in p2:
        if s["protection2"] & bit:
            items.append(name)

    return " | ".join(items) if items else "geen"


def faults_text(s):
    items = []
    fault = s["fault_state"]

    # PACE A-Series Fault State.
    mapping = [
        (0x20, "Sampling fault"),
        (0x10, "Cell fault"),
        (0x04, "NTC fault"),
        (0x02, "Ontlaad-MOSFET fault"),
        (0x01, "Laad-MOSFET fault"),
    ]

    for bit, name in mapping:
        if fault & bit:
            items.append(name)

    # Onbekende bits verliezen we niet.
    known_mask = 0x37
    unknown = fault & ~known_mask
    if unknown:
        items.append(f"Onbekende fault bits 0x{unknown:02X}")

    return " | ".join(items) if items else "geen"


def full_text(s):
    return "Ja" if (s["protection2"] & 0x80) else "Nee"


def charge_limiter_text(s):
    enabled = bool(s['control'] & 0x10)
    gear = 'Low' if s['control'] & 0x08 else 'High'
    runtime_off = bool(s['instructions'] & 0x01)
    return (f"Lokale limiter {'Aan' if enabled else 'Uit'} · gear {gear}"
            f" · runtime-off {'Ja' if runtime_off else 'Nee'} · geen numerieke CAN CCL")



def raw_status_text(s):
    return (
        f"p1={s['protection1']:02X} "
        f"p2={s['protection2']:02X} "
        f"instr={s['instructions']:02X} "
        f"ctrl={s['control']:02X} "
        f"fault={s['fault_state']:02X} "
        f"bal1={s['balance1']:02X} "
        f"bal2={s['balance2']:02X} "
        f"warn1={s['warning1']:02X} "
        f"warn2={s['warning2']:02X}"
    )


def extra_bytes_text(s):
    if not s["extra"]:
        return "geen"
    return raw_hex(s["extra"])


def diagnostic_text(analog, status):
    items = [
        f"P1=0x{status['protection1']:02X}",
        f"P2=0x{status['protection2']:02X}",
        f"Instr=0x{status['instructions']:02X}",
        f"Ctrl=0x{status['control']:02X}",
        f"Fault=0x{status['fault_state']:02X}",
        f"Bal1=0x{status['balance1']:02X}",
        f"Bal2=0x{status['balance2']:02X}",
        f"Warn1=0x{status['warning1']:02X}",
        f"Warn2=0x{status['warning2']:02X}",
    ]

    balance = balancing_text(status)
    if balance != "geen":
        items.append(f"Balanceren: {balance}")

    alarm = alarms_text(status)
    if alarm != "geen":
        items.append(f"Alarm: {alarm}")

    protect = protection_text(status)
    if protect != "geen":
        items.append(f"Protect: {protect}")

    faults = faults_text(status)
    if faults != "geen":
        items.append(f"Fault: {faults}")

    if status["protection2"] & 0x80:
        items.append("Fully")

    return " | ".join(items)


# ============================================================
# MQTT HELPERS
# ============================================================

def bms_node(bms):
    return (
        f"{BASE_TOPIC}_"
        f"{bms.lower()}"
    )


def state_topic(
    bms,
    field,
):
    return (
        f"{BASE_TOPIC}/"
        f"{bms.lower()}/"
        f"{field}"
    )


def pack_availability_topic(bms):
    return (
        f"{BASE_TOPIC}/"
        f"{bms.lower()}/"
        f"availability"
    )


def discovery_topic(
    component,
    bms,
    object_id,
):
    return (
        f"homeassistant/"
        f"{component}/"
        f"{bms_node(bms)}/"
        f"{object_id}/config"
    )


def availability_config(bms):
    return [
        {
            "topic":
                GLOBAL_AVAILABILITY,

            "payload_available":
                "online",

            "payload_not_available":
                "offline",
        },
        {
            "topic":
                pack_availability_topic(
                    bms
                ),

            "payload_available":
                "online",

            "payload_not_available":
                "offline",
        },
    ]


# ============================================================
# MQTT DISCOVERY
# ============================================================

def publish_discovery(
    client,
    bms,
):
    device = {
        "identifiers": [
            bms_node(bms)
        ],

        "name":
            f"BMSRS232 {bms}",

        "manufacturer":
            "PACE",

        "model":
            "P16S200A",
    }

    normal_entities = {
        "alarmen":
            "Alarmen",

        "beveiliging":
            "Beveiliging",

        "storingen":
            "Hardwarefouten",

        "balancing":
            "Balancing",

        "laad_mosfet":
            "Laad-MOSFET",

        "ontlaad_mosfet":
            "Ontlaad-MOSFET",

        "volledig_geladen":
            "Volledig geladen",

        "diagnostiek":
            "Diagnostiek",

        # NIEUW:
        "raw_44":
            "RAW status 44",
    }

    for (
        object_id,
        label,
    ) in normal_entities.items():

        payload = {
            "name":
                label,

            "unique_id":
                f"{BASE_TOPIC}_"
                f"v2_"
                f"{bms.lower()}_"
                f"{object_id}",

            "state_topic":
                state_topic(
                    bms,
                    object_id,
                ),

            "availability":
                availability_config(
                    bms
                ),

            "availability_mode":
                "all",

            "device":
                device,
        }

        client.publish(
            discovery_topic(
                "sensor",
                bms,
                object_id,
            ),
            json.dumps(payload),
            retain=True,
        )

    payload = {
        "name":
            "Laatste update",

        "unique_id":
            f"{BASE_TOPIC}_"
            f"v2_"
            f"{bms.lower()}_"
            f"last_update",

        "state_topic":
            state_topic(
                bms,
                "last_update",
            ),

        "device_class":
            "timestamp",

        "availability":
            availability_config(
                bms
            ),

        "availability_mode":
            "all",

        "device":
            device,
    }

    client.publish(
        discovery_topic(
            "sensor",
            bms,
            "last_update",
        ),
        json.dumps(payload),
        retain=True,
    )

    diagnostic_entities = {
        "status35": "Balance state 2 raw (C09-C16)",
        "laadstatus": "Instructions bit5 / ACin",
        "raw_status":
            "Raw status",

        "extra_bytes":
            "Extra bytes",
        "status_decoded": "Statusvelden (gedocumenteerd/raw)",
    }

    for (
        object_id,
        label,
    ) in diagnostic_entities.items():

        payload = {
            "name":
                label,

            "unique_id":
                f"{BASE_TOPIC}_"
                f"v2_"
                f"{bms.lower()}_"
                f"{object_id}",

            "state_topic":
                state_topic(
                    bms,
                    object_id,
                ),

            "availability":
                availability_config(
                    bms
                ),

            "availability_mode":
                "all",

            "entity_category":
                "diagnostic",

            "device":
                device,
        }

        client.publish(
            discovery_topic(
                "sensor",
                bms,
                object_id,
            ),
            json.dumps(payload),
            retain=True,
        )

    payload = {
        "name":
            "Live",

        "unique_id":
            f"{BASE_TOPIC}_"
            f"v2_"
            f"{bms.lower()}_"
            f"live",

        "state_topic":
            state_topic(
                bms,
                "live",
            ),

        "payload_on":
            "ON",

        "payload_off":
            "OFF",

        "device_class":
            "connectivity",

        "availability":
            availability_config(
                bms
            ),

        "availability_mode":
            "all",

        "device":
            device,
    }

    client.publish(
        discovery_topic(
            "binary_sensor",
            bms,
            "live",
        ),
        json.dumps(payload),
        retain=True,
    )


# ============================================================
# MQTT PUBLISH
# ============================================================

def publish_pack(
    client,
    bms,
    analog,
    status,
):
    now = (
        datetime.now()
        .astimezone()
        .isoformat(
            timespec="seconds"
        )
    )

    states = {
        "status_decoded": json.dumps(decoded_status(status), ensure_ascii=False),
        "status35": f"0x{status['balance2']:02X} (balance C09-C16)",
        "laadstatus": "Aan" if status["instructions"] & 0x20 else "Uit",
        "alarmen":
            alarms_text(status),

        "beveiliging":
            protection_text(status),

        "storingen":
            faults_text(status),

        "balancing":
            balancing_text(status),

        "laad_mosfet":
            (
                "Aan"
                if (
                    status["instructions"]
                    & 0x02
                )
                else
                "Uit"
            ),

        "ontlaad_mosfet":
            (
                "Aan"
                if (
                    status["instructions"]
                    & 0x04
                )
                else
                "Uit"
            ),

        "volledig_geladen":
            full_text(status),

        "diagnostiek":
            diagnostic_text(
                analog,
                status,
            ),

        "last_update":
            now,

        "raw_status":
            raw_status_text(
                status
            ),

        "extra_bytes":
            extra_bytes_text(
                status
            ),

        # VOLLEDIGE STATUS PAYLOAD
        "raw_44":
            raw_hex(
                status[
                    "raw_payload"
                ]
            ),

        "live":
            "ON",
    }

    client.publish(
        pack_availability_topic(
            bms
        ),
        "online",
        retain=True,
    )

    for (
        field,
        value,
    ) in states.items():

        client.publish(
            state_topic(
                bms,
                field,
            ),
            value,
            retain=True,
        )


# ============================================================
# READ PACK
# ============================================================

def read_pack(address):
    # Header address identifies the physical BMS (verified by transact).
    # Payload address is the local slot: 1 on each dedicated RS232 cable.
    with serial_locks[current_bms()]:
        analog_payload = decode_frame(transact(build_request(address, 0x42)))
        if len(analog_payload) < 3:
            raise ValueError("Geen lokale analoge meetgegevens")
        analog = parse_analog(analog_payload)
        if analog["address"] != 1:
            raise ValueError(f"Onverwachte lokale analog selector: {analog['address']}")
        if not analog["cells"] or not any(v > 0 for v in analog["cells"]) or analog["voltage"] <= 0:
            raise ValueError("Ongeldig leeg/nul-meetframe; niet gepubliceerd of opgeslagen")
        time.sleep(0.05)
        status_payload = decode_frame(transact(build_request(address, 0x44)))
        status = parse_status(status_payload)
        if status["address"] != 1:
            raise ValueError(f"Onverwachte lokale status selector: {status['address']}")
    # Preserve physical identity for delta parameters, MQTT and the portal.
    # Raw payload stays untouched for diagnostics.
    analog["local_selector"] = analog["address"]
    status["local_selector"] = status["address"]
    analog["address"] = address
    status["address"] = address
    return analog, status


# ============================================================
# SNAPSHOT
# ============================================================

def make_snapshot(
    analog,
    status,
):
    cells = (
        analog["cells"]
    )

    min_v = None
    max_v = None

    min_cell = None
    max_cell = None

    delta_mv = None

    if cells:

        min_v = min(cells)
        max_v = max(cells)

        min_cell = (
            cells.index(min_v)
            + 1
        )

        max_cell = (
            cells.index(max_v)
            + 1
        )

        delta_mv = round(
            (
                max_v
                - min_v
            )
            * 1000
        )

    high_warning_cells = [
        i + 1
        for i, warning
        in enumerate(
            status[
                "cell_warnings"
            ]
        )
        if warning == 0x02
    ]

    balance_cells = balancing_cells(status)

    cell_ovp_active = bool(
        status[
            "protection1"
        ]
        & 0x01
    )

    ovp_cells = []

    if cell_ovp_active:

        if high_warning_cells:

            ovp_cells = list(
                high_warning_cells
            )

        elif max_cell is not None:

            ovp_cells = [
                max_cell
            ]

    now = datetime.now()

    return {
        "timestamp": now.timestamp(),
        "charging": bool(status["instructions"] & 0x20),
        "decoded_status": decoded_status(status),
        "status35": f"0x{status['balance2']:02X} (balance C09-C16)",
        "delta_threshold_mv": delta_threshold(analog['address'])[0],
        "delta_threshold_source": delta_threshold(analog['address'])[1],

        "time":
            now.strftime(
                "%H:%M:%S"
            ),

        "soc":
            analog["soc"],

        "remaining_capacity_ah":
            analog["remaining_capacity_ah"],

        "full_capacity_ah":
            analog["full_capacity_ah"],

        "design_capacity_ah":
            analog["design_capacity_ah"],

        "soh":
            analog["soh"],

        "cycle_count":
            analog["cycle_count"],

        "voltage":
            analog["voltage"],

        "current":
            analog["current"],

        "power":
            analog["power"],

        "cells":
            list(cells),

        "temperatures":
            list(
                analog[
                    "temperatures"
                ]
            ),

        "min_v":
            min_v,

        "max_v":
            max_v,

        "min_cell":
            min_cell,

        "max_cell":
            max_cell,

        "delta_mv":
            delta_mv,

        "high_warning_cells":
            high_warning_cells,

        "balancing_cells":
            balance_cells,

        "cell_ovp_active":
            cell_ovp_active,

        "ovp_cells":
            ovp_cells,

        "alarm":
            alarms_text(status),

        "protection":
            protection_text(status),

        "fault":
            faults_text(status),

        "balancing":
            balancing_text(status),

        "charge_limiter":
            charge_limiter_text(status),

        "charge_mosfet":
            (
                "Aan"
                if (
                    status["instructions"]
                    & 0x02
                )
                else
                "Uit"
            ),

        "discharge_mosfet":
            (
                "Aan"
                if (
                    status["instructions"]
                    & 0x04
                )
                else
                "Uit"
            ),

        "fully_charged":
            full_text(status),

        "raw_status":
            raw_status_text(status),

        "protection1_raw": f"0x{status['protection1']:02X}",
        "protection2_raw": f"0x{status['protection2']:02X}",
        "instructions_raw": f"0x{status['instructions']:02X}",
        "control_raw": f"0x{status['control']:02X}",
        "fault_raw": f"0x{status['fault_state']:02X}",
        "balance1_raw": f"0x{status['balance1']:02X}",
        "balance2_raw": f"0x{status['balance2']:02X}",
        "warning1_raw": f"0x{status['warning1']:02X}",
        "warning2_raw": f"0x{status['warning2']:02X}",

        "raw44":
            raw_hex(
                status[
                    "raw_payload"
                ]
            ),

        "raw44_indexed":
            raw_indexed(
                status[
                    "raw_payload"
                ]
            ),
    }


# ============================================================
# PARAMETER PARSERS
# ============================================================

def parse_ov(payload):
    if len(payload) != 8:

        raise ValueError(
            "OV payloadlengte "
            f"{len(payload)}, "
            "verwacht 8"
        )

    return {
        "marker":
            payload[0],

        "alarm_mv":
            int.from_bytes(
                payload[1:3],
                "big",
            ),

        "protection_mv":
            int.from_bytes(
                payload[3:5],
                "big",
            ),

        "release_mv":
            int.from_bytes(
                payload[5:7],
                "big",
            ),

        "delay_100ms":
            payload[7],
    }


def parse_balancing(payload):
    if len(payload) != 4:

        raise ValueError(
            "Balancing payloadlengte "
            f"{len(payload)}, "
            "verwacht 4"
        )

    return {
        "threshold_mv":
            int.from_bytes(
                payload[0:2],
                "big",
            ),

        "delta_mv":
            int.from_bytes(
                payload[2:4],
                "big",
            ),
    }


def parse_sleep(payload):
    if len(payload) != 4:

        raise ValueError(
            "Sleep payloadlengte "
            f"{len(payload)}, "
            "verwacht 4"
        )

    return {
        "voltage_mv":
            int.from_bytes(
                payload[0:2],
                "big",
            ),

        "reserved":
            payload[2],

        "delay_min":
            payload[3],
    }


def parse_full_charge(payload):
    if len(payload) != 5:

        raise ValueError(
            "Full Charge payloadlengte "
            f"{len(payload)}, "
            "verwacht 5"
        )

    return {
        "voltage_mv":
            int.from_bytes(
                payload[0:2],
                "big",
            ),

        "current_ma":
            int.from_bytes(
                payload[2:4],
                "big",
            ),

        "low_soc":
            payload[4],
    }


def parse_charge_overcurrent(payload):
    if len(payload) != 6 or payload[0] != 0x01:
        raise ValueError("CHG OC: onbekende payload-layout")
    return {
        "warning_a": int.from_bytes(payload[1:3], "big"),
        "protection_a": int.from_bytes(payload[3:5], "big"),
        "delay_100ms": payload[5],
    }


def parse_limiter_start(payload):
    if len(payload) != 2:
        raise ValueError("Limiter-startstroom: verwacht 2 bytes")
    return {
        "payload_address": payload[0],
        "start_current_a": payload[1],
    }


CAN_PROTOCOL_NAMES = {
    0x00: "PACE",
    0x01: "Pylon / Deye-familie",
    0x02: "Growatt",
    0x03: "Victron",
    0x04: "Schneider / SE / SMA",
    0x05: "LuxPower",
    0x06: "SoroTec / SRD",
    0x07: "SMA / Studer",
    0x08: "GoodWe",
    0x09: "Studer",
    0x0A: "Sofar",
    0x0B: "Must / PV",
    0x0C: "Solis / Jinlang",
    0x0D: "DIDU / TBB",
    0x0E: "Senergy / Aifu",
    0x0F: "TBB",
    0x10: "Pylon V2.02",
    0x11: "Growatt V1.09",
    0x12: "Must V2.02",
    0x13: "Afore",
    0x14: "INVT / YWT",
    0x15: "FUJI",
    0x16: "Sofar V2.1003",
    0xFF: "Uit / leeg",
}

RS485_PROTOCOL_NAMES = {
    0x00: "PACE Modbus",
    0x01: "Pylon / Deye / Bentterson",
    0x02: "Growatt",
    0x03: "Voltronic",
    0x04: "Schneider / SE",
    0x05: "PHOCOS",
    0x06: "LuxPower",
    0x07: "Solar",
    0x08: "Lithium / SMARK",
    0x09: "EP / MSL",
    0x0A: "RTU04",
    0x0B: "LuxPower V0.1",
    0x0C: "LuxPower V0.3",
    0x0D: "SRNE / WOW",
    0x0E: "LEOCH",
    0x0F: "Pylon F",
    0x10: "Afore",
    0x11: "UPS AGXN",
    0x12: "Orex / Sunpolo",
    0x13: "XIONGTAO",
    0x14: "RONGKE",
    0x15: "XINRUI",
    0x16: "ELTEK",
    0x17: "GT",
    0x18: "Leoch V1.06",
    0xFF: "Uit / leeg",
}


def protocol_name(mapping, value, kind):
    if value == 0x29 and kind == "CAN":
        return "DEYE (gecontroleerd in PBMS Tools)"
    if value == 0x01 and kind == "RS485":
        return "PYLON (gecontroleerd in PBMS Tools)"
    return mapping.get(value, "onbekende firmwarecode")


def parse_protocols(payload):
    if len(payload) != 3:
        raise ValueError("Communicatieprotocollen: verwacht 3 bytes")
    can_raw, rs485_raw, selection_raw = payload
    return {
        "can_raw": can_raw,
        "can_hex": f"0x{can_raw:02X}",
        "can_name": protocol_name(CAN_PROTOCOL_NAMES, can_raw, "CAN"),
        "rs485_raw": rs485_raw,
        "rs485_hex": f"0x{rs485_raw:02X}",
        "rs485_name": protocol_name(RS485_PROTOCOL_NAMES, rs485_raw, "RS485"),
        "selection_raw": selection_raw,
        "selection_name": {0x00: "Auto", 0x01: "Manual", 0xFF: "Leeg"}.get(
            selection_raw, "Onbekend"
        ),
    }


def limiter_status_from_snapshot(snapshot):
    if not snapshot:
        return {
            "available": False,
            "enabled": None,
            "gear": "onbekend",
            "runtime_off": None,
            "control_raw": None,
            "instructions_raw": None,
        }
    control = int(snapshot["control_raw"], 16)
    instructions = int(snapshot["instructions_raw"], 16)
    return {
        "available": True,
        "enabled": bool(control & 0x10),
        "gear": "low" if control & 0x08 else "high",
        "runtime_off": bool(instructions & 0x01),
        "control_raw": snapshot["control_raw"],
        "instructions_raw": snapshot["instructions_raw"],
    }


# ============================================================
# PARAMETERS READ
# ============================================================

def parse_capacity(payload):
    if len(payload) != 6:
        raise ValueError(f"Capaciteit: verwacht 6 bytes, ontvangen {len(payload)}")
    remaining, full, design = (u16(payload, offset) for offset in (0, 2, 4))
    if full == 0 or design == 0:
        raise ValueError("Ongeldige nulcapaciteit")
    return {"remaining_ah": remaining / 100, "fcc_ah": full / 100,
            "design_ah": design / 100}


def read_capacity():
    bms = current_bms()
    try:
        with serial_locks[bms]:
            data = parse_capacity(param_request(0xA6))
        data["read_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with cache_lock:
            capacity_cache[bms] = data
            capacity_errors[bms] = None
        return data
    except Exception as exc:
        with cache_lock:
            capacity_errors[bms] = str(exc)
        raise


@app.route("/parameters/capacity-read", methods=["POST"])
def web_read_capacity():
    try:
        read_capacity()
        last_action_message[current_bms()] = "Capaciteit opnieuw gelezen"
    except Exception as exc:
        last_action_message[current_bms()] = f"Capaciteit lezen mislukt: {exc}"
    return redirect(url_for("index"))


TEMP_GROUPS = {
 "charge_discharge_ot": (0xDD,0xDC,"Hoge temperatuur laden / ontladen",("Laden","Ontladen"),False),
 "charge_discharge_ut": (0xDF,0xDE,"Lage temperatuur laden / ontladen",("Laden","Ontladen"),True),
 "mosfet_ot": (0xE1,0xE0,"Hoge MOSFET-temperatuur",("MOSFET",),False),
 "environment_ut_ot": (0xE7,0xE6,"Omgevingstemperatuur",("Laag","Hoog"),None),
}
extra_settings = {b: {"temps":{},"errors":{},"clock":None} for b in BMS_ADDRESSES.values()}

def parse_temp_settings(payload, group):
    count = 3*len(TEMP_GROUPS[group][3])
    if len(payload)!=1+2*count or payload[0]!=1:
        raise ValueError("Onbekende temperatuur-layout")
    return [round((int.from_bytes(payload[i:i+2],"big")-2730)/10,1) for i in range(1,len(payload),2)]

def parse_bms_clock(payload):
    if len(payload)!=6:raise ValueError("Datum/tijd: verwacht 6 bytes")
    return datetime(2000+payload[0],*payload[1:])

def read_extra_settings():
    bms=current_bms()
    with serial_locks[bms]:
        for group,(read_cmd,_,_,_,_) in TEMP_GROUPS.items():
            try:
                values=parse_temp_settings(param_request(read_cmd),group)
                with cache_lock:
                    extra_settings[bms]["temps"][group]=values
                    extra_settings[bms]["errors"].pop(group,None)
            except Exception as exc:
                with cache_lock:extra_settings[bms]["errors"][group]=str(exc)
        try:
            clock=parse_bms_clock(param_request(0xB1))
            with cache_lock:
                extra_settings[bms]["clock"]=clock.isoformat(timespec="seconds")
                extra_settings[bms]["clock_read_at"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                extra_settings[bms]["errors"].pop("clock",None)
        except Exception as exc:
            with cache_lock:extra_settings[bms]["errors"]["clock"]=str(exc)

def log_setting_write(bms, kind, before, after):
    try:
        with history_lock:
            with history_db:
                event_insert(bms,"setting_write",{"setting":kind,"before":before,"after":after,
                    "note":"Portalopdracht, bevestigd door teruglezing"})
    except Exception as exc:
        print(f"Instellinglog: {exc}",flush=True)

@app.route("/parameters/temperature",methods=["POST"])
def web_write_temperature():
    bms=current_bms()
    try:
        group=request.form["group"]
        if group not in TEMP_GROUPS:raise ValueError("Onbekende temperatuurgroep")
        read_cmd,write_cmd,title,labels,under=TEMP_GROUPS[group]
        values=[float(request.form[f"t{i}"]) for i in range(3*len(labels))]
        if not all(-40<=v<=125 and v.is_integer() for v in values):
            raise ValueError("Gebruik hele graden tussen -40 en 125 °C")
        for i in range(len(labels)):
            alarm,protect,release=values[3*i:3*i+3]
            is_under=under if under is not None else i==0
            if not ((protect<alarm and protect<release) if is_under else (protect>alarm and protect>release)):
                raise ValueError("Controleer volgorde waarschuwing, beveiliging en herstel")
        with serial_locks[bms]:
            before=parse_temp_settings(param_request(read_cmd),group)
            payload=b"\x01"+b"".join((round(v*10)+2730).to_bytes(2,"big") for v in values)
            param_write(write_cmd,payload)
            time.sleep(0.4)
            after=parse_temp_settings(param_request(read_cmd),group)
            if after!=values:raise RuntimeError(f"Teruglezing wijkt af: {after}")
            with cache_lock:
                extra_settings[bms]["temps"][group]=after
                extra_settings[bms]["errors"].pop(group,None)
            log_setting_write(bms,group,before,after)
        last_action_message[bms]=f"{title} WRITE OK"
    except Exception as exc:
        last_action_message[bms]=f"Temperatuur WRITE FOUT: {exc}"
    return redirect(url_for("index"))

@app.route("/parameters/clock",methods=["POST"])
def web_write_clock():
    bms=current_bms()
    try:
        with serial_locks[bms]:
            before=parse_bms_clock(param_request(0xB1))
            if request.form.get("mode")=="sync":
                target=datetime.now(EXPORT_TIMEZONE).replace(tzinfo=None,microsecond=0)
            elif request.form.get("mode")=="manual":
                target=datetime.fromisoformat(request.form["clock"])
                if target.tzinfo is not None:raise ValueError("Gebruik lokale tijd zonder tijdzone-offset")
                target=target.replace(microsecond=0)
            else:raise ValueError("Onbekende tijdactie")
            if not 2000<=target.year<=2099:raise ValueError("Jaar moet tussen 2000 en 2099 liggen")
            payload=bytes([target.year-2000,target.month,target.day,target.hour,target.minute,target.second])
            started=time.monotonic()
            param_write(0xB2,payload)
            time.sleep(0.4)
            after=parse_bms_clock(param_request(0xB1))
            delta=(after-target).total_seconds()
            if not 0<=delta<=time.monotonic()-started+2:
                raise RuntimeError(f"Teruglezing wijkt af: {after.isoformat()}")
            with cache_lock:
                extra_settings[bms]["clock"]=after.isoformat(timespec="seconds")
                extra_settings[bms]["clock_read_at"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                extra_settings[bms]["errors"].pop("clock",None)
            log_setting_write(bms,"clock",before.isoformat(),after.isoformat())
        last_action_message[bms]=f"BMS-klok WRITE OK · {after:%Y-%m-%d %H:%M:%S}"
    except Exception as exc:
        last_action_message[bms]=f"BMS-klok WRITE FOUT: {exc}"
    return redirect(url_for("index"))


def read_parameters():

    try:

        with serial_locks[current_bms()]:

            cell_ov = parse_ov(
                param_request(
                    0xD1
                )
            )

            time.sleep(0.08)

            pack_ov = parse_ov(
                param_request(
                    0xD5
                )
            )

            time.sleep(0.08)

            cell_uv = parse_ov(param_request(0xD3))
            pack_uv = parse_ov(param_request(0xD7))

            balancing = (
                parse_balancing(
                    param_request(
                        0xB6
                    )
                )
            )

            time.sleep(0.08)

            sleep_cfg = (
                parse_sleep(
                    param_request(
                        0xA0
                    )
                )
            )

            time.sleep(0.08)

            full_charge = (
                parse_full_charge(
                    param_request(
                        0xAF
                    )
                )
            )

            time.sleep(0.08)

            charge_overcurrent = parse_charge_overcurrent(
                param_request(0xD9)
            )

            time.sleep(0.08)

            limiter_start = parse_limiter_start(
                param_request(0xED)
            )

            time.sleep(0.08)

            protocols = parse_protocols(
                param_request(0xEB)
            )

        with cache_lock:
            limiter_status = limiter_status_from_snapshot(
                live_cache.get(current_bms())
            )

        data = {
            "read_at":
                datetime.now()
                .strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),

            "cell_uv": cell_uv,
            "pack_uv": pack_uv,
            "cell_ov":
                cell_ov,

            "pack_ov":
                pack_ov,

            "balancing":
                balancing,

            "sleep":
                sleep_cfg,

            "full_charge":
                full_charge,

            "charge_overcurrent": charge_overcurrent,

            "limiter_start": limiter_start,

            "limiter_status": limiter_status,

            "protocols": protocols,
        }

        with cache_lock:

            parameter_cache[current_bms()] = data
            last_parameter_error[current_bms()] = None

        record_parameter_event(data)
        read_extra_settings()
        try:
            read_capacity()
        except Exception as exc:
            print(f"{current_bms()} capaciteit lezen: {exc}", flush=True)
        return data

    except Exception as exc:

        with cache_lock:

            last_parameter_error[current_bms()] = (
                str(exc)
            )

        raise


# ============================================================
# PARAMETER WRITE
# ============================================================

def write_full_charge(
    voltage_mv,
    current_ma,
    low_soc,
):
    payload = (
        int(
            voltage_mv
        ).to_bytes(
            2,
            "big",
        )
        +
        int(
            current_ma
        ).to_bytes(
            2,
            "big",
        )
        +
        bytes([
            int(low_soc)
        ])
    )

    param_write(
        0xAE,
        payload,
    )


def write_charge_overcurrent(warning_a, protection_a, delay_100ms):
    payload = (
        b"\x01"
        + int(warning_a).to_bytes(2, "big")
        + int(protection_a).to_bytes(2, "big")
        + bytes([int(delay_100ms)])
    )
    param_write(0xD8, payload)


def write_limiter_start(start_current_a, payload_address):
    param_write(0xEE, bytes([int(payload_address), int(start_current_a)]))


def write_limiter_switch(command):
    # PBmsTools/PACE v25 wire values: enable=0B, disable=0A,
    # high gear=08, low gear=09. This changes local limiter configuration,
    # not a numeric CAN CCL value.
    if command not in (0x08, 0x09, 0x0A, 0x0B):
        raise ValueError("Onbekende limiteropdracht")
    param_write(0x99, bytes([command]))


def write_balancing(
    threshold_mv,
    delta_mv,
):
    payload = (
        int(
            threshold_mv
        ).to_bytes(
            2,
            "big",
        )
        +
        int(
            delta_mv
        ).to_bytes(
            2,
            "big",
        )
    )

    param_write(
        0xB5,
        payload,
    )


def write_sleep(
    voltage_mv,
    delay_min,
):
    payload = (
        int(
            voltage_mv
        ).to_bytes(
            2,
            "big",
        )
        +
        bytes([
            0x00
        ])
        +
        bytes([
            int(delay_min)
        ])
    )

    param_write(
        0xA8,
        payload,
    )


def write_cell_ov(
    alarm_mv,
    protection_mv,
    release_mv,
    delay_100ms,
):
    payload = (
        bytes([
            0x01
        ])
        +
        int(
            alarm_mv
        ).to_bytes(
            2,
            "big",
        )
        +
        int(
            protection_mv
        ).to_bytes(
            2,
            "big",
        )
        +
        int(
            release_mv
        ).to_bytes(
            2,
            "big",
        )
        +
        bytes([
            int(
                delay_100ms
            )
        ])
    )

    param_write(
        0xD0,
        payload,
    )


def write_pack_ov(
    alarm_mv,
    protection_mv,
    release_mv,
    delay_100ms,
):
    payload = (
        bytes([
            0x01
        ])
        +
        int(
            alarm_mv
        ).to_bytes(
            2,
            "big",
        )
        +
        int(
            protection_mv
        ).to_bytes(
            2,
            "big",
        )
        +
        int(
            release_mv
        ).to_bytes(
            2,
            "big",
        )
        +
        bytes([
            int(
                delay_100ms
            )
        ])
    )

    param_write(
        0xD4,
        payload,
    )


# ============================================================
# LIVE POLLING
# ============================================================

def polling_loop(address, bms):
    io_context.bms = bms
    global live_cache
    global last_mqtt_publish

    while not stop_event.is_set():

        cycle_start = (
            time.monotonic()
        )

        for (
            address,
            bms,
        ) in [(address, bms)]:

            if stop_event.is_set():
                break
            try:

                analog, status = (
                    read_pack(
                        address
                    )
                )

                # ZEER BELANGRIJK:
                # log volledige raw status zodra ook maar één byte verandert.
                log_raw44_change(
                    bms,
                    status[
                        "raw_payload"
                    ],
                    status,
                )

                snapshot = (
                    make_snapshot(
                        analog,
                        status,
                    )
                )

                with cache_lock:

                    live_cache[
                        bms
                    ] = snapshot

                record_status_event(bms, status, snapshot)
                record_capacity_event(bms, status, snapshot)
                save_history(bms, snapshot)

                now_mono = (
                    time.monotonic()
                )

                if (
                    now_mono
                    -
                    last_mqtt_publish[
                        bms
                    ]
                    >=
                    MQTT_PUBLISH_INTERVAL
                ):

                    publish_pack(
                        mqtt_client,
                        bms,
                        analog,
                        status,
                    )

                    last_mqtt_publish[
                        bms
                    ] = (
                        now_mono
                    )



            except Exception as exc:

                print(
                    f"{datetime.now():%H:%M:%S} "
                    f"{bms} FOUT: "
                    f"{exc}",
                    flush=True,
                )

                try:

                    mqtt_client.publish(
                        state_topic(
                            bms,
                            "live",
                        ),
                        "OFF",
                        retain=True,
                    )

                    mqtt_client.publish(
                        pack_availability_topic(
                            bms
                        ),
                        "offline",
                        retain=True,
                    )

                except Exception:
                    pass

            time.sleep(0.03)

        elapsed = (
            time.monotonic()
            - cycle_start
        )

        remaining = (
            LIVE_POLL_INTERVAL
            - elapsed
        )

        if remaining > 0:

            stop_event.wait(remaining)


# ============================================================
# WEB PAGE
# ============================================================

PAGE = r"""
<!doctype html>

<html lang="nl">

<head>

<meta charset="utf-8">

<meta
    name="viewport"
    content="width=device-width,initial-scale=1"
>

<title>PACE BMS Monitor · V2.1</title>



<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;

    font-family:
        system-ui,
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        sans-serif;

    background: #111318;
    color: #eceff4;

    font-size: 12px;
}

main {
    max-width: 1900px;
    margin: auto;
    padding: 8px;
}

h1 {
    margin: 0;
    font-size: 18px;
}

h2 {
    margin: 0;
    font-size: 15px;
}

h3 {
    margin: 0 0 6px 0;
    font-size: 13px;
}

.muted {
    color: #969ca6;
}

.ok {
    color: #65d48b;
}

.warn {
    color: #ffb74d;
}

.bad {
    color: #ff6b6b;
}

.page-top {
    display: flex;
    justify-content: space-between;
    align-items: end;
    gap: 10px;

    margin-bottom: 7px;
}


/* ========================================================
   GRAFIEKEN
   ======================================================== */

.chart-grid {
    display: grid;

    grid-template-columns:
        repeat(
            3,
            minmax(0, 1fr)
        );

    gap: 6px;

    margin-bottom: 6px;
}

.chart-card {
    background: #1b1e25;

    border:
        1px solid #30343d;

    border-radius: 8px;

    padding: 7px;

    min-width: 0;
}

.chart-title {
    display: flex;
    justify-content: space-between;
    align-items: center;

    margin-bottom: 3px;
}

.chart-title strong {
    font-size: 13px;
}

.chart-live {
    display: flex;
    gap: 10px;

    font-size: 11px;

    margin-bottom: 3px;
}

.legend-b1 {
    color: #65d48b;
}

.legend-b2 {
    color: #56a4ff;
}

.legend-b3 {
    color: #ffab40;
}

canvas {
    display: block;

    width: 100%;
    height: 150px;

    background: #16191f;

    border-radius: 5px;
}


/* ========================================================
   BMS LIVE
   ======================================================== */

.bms-grid {
    display: grid;

    grid-template-columns:
        repeat(
            3,
            minmax(0, 1fr)
        );

    gap: 6px;
}

.bms-column {
    min-width: 0;
}

.bms-panel {
    min-width: 0;

    background: #1b1e25;

    border:
        1px solid #30343d;

    border-radius: 8px;

    padding: 7px;
}

.bms-title {
    display: flex;

    justify-content:
        space-between;

    align-items: center;

    gap: 6px;

    margin-bottom: 6px;
}

.bms-title strong {
    font-size: 14px;
}

.live-time {
    font-size: 10px;
    color: #65d48b;
}

.metrics {
    display: grid;

    grid-template-columns:
        repeat(
            5,
            minmax(0, 1fr)
        );

    gap: 3px;

    margin-bottom: 5px;
}

.metric {
    min-width: 0;

    background: #12151a;

    border-radius: 5px;

    padding: 5px;
}

.metric-label {
    color: #969ca6;
    font-size: 9px;
}

.metric-value {
    margin-top: 1px;

    font-size: 13px;
    font-weight: 700;

    white-space: nowrap;
}


/* ========================================================
   CELLEN
   ======================================================== */

.cells {
    display: grid;

    grid-template-columns:
        repeat(
            4,
            minmax(0, 1fr)
        );

    /* Iets meer lucht tussen de cellen: hierdoor worden de vakjes
       zelf smaller en hoort het voltage visueel duidelijk bij Cxx. */
    column-gap: 11px;
    row-gap: 3px;
}

.cell {
    min-width: 0;

    background: #12151a;

    border:
        1px solid #20242c;

    border-radius: 4px;

    padding: 3px 5px;

    min-height: 26px;

    /* Vaste interne volgorde: celnaam | BL | voltage */
    display: grid;
    grid-template-columns: 24px 19px minmax(0, 1fr);
    align-items: center;
    column-gap: 2px;

    overflow: hidden;
}


/* Cell OV / HIGH warning */
.cell.cell-ov {
    background: #4c3109;

    border-color: #d98b1d;
}

.cell.cell-ov .cell-value {
    color: #ffc05b;
}


/* Cell OVP / protection */
.cell.cell-ovp {
    border-color: #ff5151;

    background:
        repeating-linear-gradient(
            135deg,
            rgba(170, 25, 25, 0.72) 0px,
            rgba(170, 25, 25, 0.72) 5px,
            rgba(83, 13, 13, 0.92) 5px,
            rgba(83, 13, 13, 0.92) 10px
        );
}

.cell.cell-ovp .cell-number,
.cell.cell-ovp .cell-value {
    color: #ffffff;
    font-weight: 800;
}


.cell-number {
    color: #7f8691;
    font-size: 9px;
    white-space: nowrap;
}

.cell-value {
    font-size: 10px;
    font-weight: 600;
    text-align: right;
    white-space: nowrap;
}


/* Balancing badge: altijd de middelste kolom tussen Cxx en voltage. */
.bl-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;

    width: 18px;
    height: 13px;

    padding: 0;

    border-radius: 3px;

    background: #1879d9;

    color: white;

    font-size: 8px;

    font-weight: 800;

    line-height: 13px;

    box-shadow:
        0 0 0 1px
        rgba(255,255,255,0.15);

    visibility: hidden;
    opacity: 0;
}

.bl-badge.active {
    visibility: visible;
    opacity: 1;
}


.cell-summary {
    margin-top: 5px;

    font-size: 10px;

    color: #c7cbd1;
}

.status-list {
    margin-top: 6px;
}

.status-row {
    display: grid;

    grid-template-columns:
        76px minmax(0, 1fr);

    gap: 4px;

    padding: 3px 0;

    border-top:
        1px solid #282c34;

    font-size: 10px;
}

.status-name {
    color: #969ca6;
}

.status-value {
    overflow-wrap: anywhere;
}

.temp-line {
    margin-top: 5px;

    padding-top: 4px;

    border-top:
        1px solid #282c34;

    font-size: 10px;

    color: #c7cbd1;
}

.info-line {
    margin-top: 4px;

    font-size: 10px;

    color: #aeb3bb;
}


/* ========================================================
   PARAMETERS BMS1
   ======================================================== */

.parameter-area {
    margin-top: 6px;

    background: #1b1e25;

    border:
        1px solid #30343d;

    border-radius: 8px;

    padding: 7px;
}

.parameter-heading {
    display: flex;

    align-items: center;

    justify-content:
        space-between;

    gap: 8px;

    margin-bottom: 6px;
}

.parameter-stack {
    display: grid;

    grid-template-columns:
        repeat(
            2,
            minmax(0, 1fr)
        );

    gap: 5px;
}

.param-card {
    background: #15181e;

    border:
        1px solid #30343d;

    border-radius: 6px;

    padding: 6px;
}

.param-card.full {
    grid-column: span 2;
}

label {
    display: block;

    color: #adb2ba;

    font-size: 10px;

    margin-bottom: 4px;
}

input {
    display: block;

    width: 100%;

    min-height: 27px;

    margin-top: 2px;

    padding: 3px 5px;

    border-radius: 4px;

    border:
        1px solid #454a54;

    background: #111318;

    color: white;

    font-size: 12px;
}

button {
    min-height: 27px;

    border: 0;

    border-radius: 4px;

    padding: 4px 8px;

    font-size: 10px;

    font-weight: 700;

    cursor: pointer;
}

.read {
    background: #4d8fe8;
    color: white;
}

.write {
    width: 100%;

    margin-top: 2px;

    background: #d99b3a;
    color: #111;
}

.message {
    margin-bottom: 6px;

    padding: 6px 8px;

    background: #1b1e25;

    border-left:
        3px solid #4d8fe8;

    border-radius: 4px;
}


/* ========================================================
   RESPONSIVE
   ======================================================== */

@media (max-width: 1150px) {

    .chart-grid {
        grid-template-columns: 1fr;
    }

    .bms-grid {
        grid-template-columns: 1fr;
    }

    .parameter-stack {
        grid-template-columns:
            repeat(
                2,
                minmax(0, 1fr)
            );
    }
}

@media (max-width: 600px) {

    .metrics {
        grid-template-columns:
            repeat(
                2,
                minmax(0, 1fr)
            );
    }

    .parameter-stack {
        grid-template-columns: 1fr;
    }

    .param-card.full {
        grid-column: span 1;
    }
}


/* Original layout, with only the requested readability changes. */
.chart-card canvas {height:245px}
.chart-live {display:grid;grid-template-columns:repeat(3,minmax(0,1fr));font-size:17px;font-weight:700;gap:16px;margin:8px 0 12px}
.chart-reading {display:flex;flex-direction:column;gap:4px;min-width:0}
.chart-label {font-size:12px;font-weight:600;opacity:.85}
.chart-number {font-size:23px;line-height:1.2;white-space:nowrap}
.chart-title strong {font-size:15px}
.metric-value {font-size:clamp(13px,1.08vw,20px);white-space:nowrap}
.metric-label {font-size:11px}
.pack-state {margin:6px 0;color:#969ca6;font-size:12px}
.diagnostics {margin-top:8px;color:#969ca6;font-size:11px}
.diagnostics summary {cursor:pointer}
.diagnostics div {font-family:monospace;overflow-wrap:anywhere;margin:5px 0}
#connection-status {font-size:11px;color:#969ca6;margin-bottom:6px}
@media(max-width:900px){.chart-card canvas{height:230px}.metric-value{font-size:18px}}
.decoded-status{white-space:pre-wrap;overflow-wrap:anywhere;font-size:11px}
#event-log{margin-top:10px;padding:9px;border:1px solid #414752;border-radius:6px}
#event-log summary{cursor:pointer}#event-list>details{padding:7px 0;border-bottom:1px solid #343943}
#event-list pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:11px;max-height:280px;overflow:auto}
#event-log a{color:#56a4ff}
.parameter-progress[hidden]{display:none}
.parameter-progress{
    position:fixed;inset:0;z-index:10000;display:flex;align-items:center;justify-content:center;
    padding:20px;background:rgba(6,8,12,.82);backdrop-filter:blur(3px);cursor:wait
}
.parameter-progress-card{
    width:min(460px,100%);padding:24px;border:1px solid #596170;border-radius:10px;
    background:#1b1e25;box-shadow:0 18px 60px rgba(0,0,0,.55);text-align:center
}
.parameter-progress-spinner{
    width:38px;height:38px;margin:0 auto 15px;border:4px solid #414752;
    border-top-color:#d99b3a;border-radius:50%;animation:parameter-spin .8s linear infinite
}
.parameter-progress-title{font-size:20px;font-weight:800;color:#fff;margin-bottom:8px}
.parameter-progress-text{font-size:14px;line-height:1.45;color:#d5d9df}
.parameter-progress-warning{margin-top:12px;font-size:13px;font-weight:700;color:#ffbe55}
body.parameter-busy main{pointer-events:none;user-select:none}
@keyframes parameter-spin{to{transform:rotate(360deg)}}
@media(prefers-reduced-motion:reduce){.parameter-progress-spinner{animation:none;border-top-color:#d99b3a}}

/* V2.0: alleen het instellingengedeelte; monitoring blijft ongewijzigd. */
#settings-v2 {margin:12px 0;background:#1b1e25;border:1px solid #30343d;border-radius:9px;padding:14px;}
#settings-v2 .settings-title h2{font-size:19px;}
#settings-v2 .settings-title p{margin:5px 0 12px;}
#settings-v2 .version-badge{font-size:11px;font-weight:500;background:#303743;border-radius:4px;padding:3px 6px;vertical-align:middle;}
#settings-v2 .settings-nav{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px;}
#settings-v2 .settings-nav a{color:#dde3ec;text-decoration:none;border:1px solid #39414d;border-radius:5px;padding:9px 15px;background:#141820;}
#settings-v2 .settings-nav a:hover,#settings-v2 .settings-nav a:focus-visible{border-color:#dba13b;color:#ffc66a;}
#settings-v2 .settings-read-row,#settings-v2 .settings-comparison{display:grid;grid-template-columns:minmax(155px,.65fr) repeat(3,minmax(0,1fr));gap:12px;}
#settings-v2 .settings-read-row{padding:8px 12px 14px;align-items:center;}
#settings-v2 .settings-group{border:1px solid #343b46;border-radius:6px;margin-top:8px;background:#161a21;scroll-margin-top:12px;}
#settings-v2 .settings-group>summary{cursor:pointer;padding:12px 14px;font-size:14px;font-weight:650;}
#settings-v2 .settings-group[open]>summary{border-bottom:1px solid #303743;}
#settings-v2 .settings-comparison{padding:12px;}
#settings-v2 .settings-description{padding:8px 4px;}
#settings-v2 .settings-description p{line-height:1.6;}
#settings-v2 .settings-pack{min-width:0;}
#settings-v2 .pack-heading{padding:8px 10px;background:#202730;border-radius:4px;}
#settings-v2 .bms1>.pack-heading,#settings-v2 .bms1>strong{color:#65d48b;}
#settings-v2 .bms2>.pack-heading,#settings-v2 .bms2>strong{color:#4fa2ff;}
#settings-v2 .bms3>.pack-heading,#settings-v2 .bms3>strong{color:#ffb74d;}
#settings-v2 .param-card{padding:10px;border:1px solid #2b333f;border-radius:5px;margin-bottom:8px;background:#191e26;}
#settings-v2 .param-card.full{grid-column:auto;}
#settings-v2 label{display:grid;grid-template-columns:minmax(0,1fr) minmax(80px,.8fr);align-items:center;gap:10px;min-height:40px;margin:6px 0;color:#bec6d2;font-size:12px;}
#settings-vol .param-card>h3,#settings-balans .param-card>h3{display:none;}
#settings-v2 input:not([type=hidden]),#settings-v2 select{width:100%;min-width:0;margin-top:0;padding:8px 10px;border:1px solid #3d4654;border-radius:5px;background:#10151c;color:#f1f5fa;font:inherit;font-size:13px;}
#settings-v2 button{cursor:pointer;}
#settings-v2 .write{width:100%;margin-top:8px;padding:9px 6px;font-size:12px;border-radius:5px;}
#settings-v2 .read{padding:7px 10px;}
#settings-v2 .protocol-readable dl{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin:8px 0 14px;}
#settings-v2 .protocol-readable dt{color:#a9b4c4;}#settings-v2 .protocol-readable dd{margin:0;overflow-wrap:anywhere;}
#settings-v2 .settings-pack>p.bad{overflow-wrap:anywhere;}
@media(max-width:950px){ #settings-v2 .settings-comparison,#settings-v2 .settings-read-row{grid-template-columns:repeat(3,minmax(0,1fr));}#settings-v2 .settings-description,#settings-v2 .settings-read-row>div:first-child{grid-column:1/-1;}}
@media(max-width:600px){ #settings-v2{padding:8px;}#settings-v2 .settings-comparison,#settings-v2 .settings-read-row{grid-template-columns:1fr;}#settings-v2 .settings-nav a{padding:8px;font-size:11px;}}


/* Compacte, permanent zichtbare instellingen. */
#settings-v2{padding:10px;}
#settings-v2 .settings-comparison,#settings-v2 .settings-read-row{grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;padding:6px 8px;}
#settings-v2 .settings-read-row>div:first-child{display:none;}
#settings-v2 .settings-title p{margin:3px 0 6px;}
#settings-v2 .settings-read-row .settings-pack{display:flex;align-items:center;gap:10px;flex-wrap:wrap;}
#settings-v2 .settings-read-row p{margin:0;font-size:10px;}
#settings-v2 .group-title{font-size:13px;margin:0;padding:7px 10px;border-bottom:1px solid #303743;background:#20252e;}
#settings-v2 .settings-group{margin-top:7px;}
#settings-v2 .pack-heading{font-size:11px;margin:0 0 3px;padding:3px 6px;background:transparent;}
#settings-v2 .param-card{padding:5px 8px;margin:0 0 4px;border:0;border-radius:0;background:transparent;}
#settings-v2 label{min-height:29px;margin:3px 0;gap:8px;grid-template-columns:minmax(0,1fr) minmax(90px,.65fr);}
#settings-v2 input:not([type=hidden]),#settings-v2 select{padding:5px 8px;font-size:12px;}
#settings-v2 .write{padding:6px;margin-top:4px;font-size:11px;}
#settings-v2 .read{padding:5px 8px;font-size:11px;}
#settings-v2 .param-card p{margin:4px 0;line-height:1.35;}
#settings-v2 .protocol-readable dl{gap:5px;margin:4px 0;}
#settings-v2 .protocol-readable h3{display:none;}
#settings-v2 form[action^="/parameters/limiter-"]{display:grid;grid-template-columns:minmax(0,1fr) 132px;gap:8px;align-items:center;margin:4px 0;}
#settings-v2 form[action^="/parameters/limiter-"] label{grid-template-columns:minmax(0,1fr) 80px;}
#settings-v2 form[action^="/parameters/limiter-"] button{margin:0;font-size:10px;}
#settings-v2 .param-card[style]{margin:0 0 5px!important;}
@media(max-width:600px){ #settings-v2 .settings-comparison,#settings-v2 .settings-read-row{grid-template-columns:1fr;} }

/* Volgorde van de instellingen: lager cijfer staat hoger. */
#settings-v2 {
    display: flex;
    flex-direction: column;
}

#settings-stroom      { order: 4; }
#settings-vol         { order: 2; }
#settings-balans      { order: 3; }
#settings-beveiliging { order: 1; }
#settings-overig      { order: 5; }
#settings-communicatie { order: 6; }

</style>

</head>


<body>

<div id="parameter-progress" class="parameter-progress" hidden role="alertdialog" aria-modal="true" aria-labelledby="parameter-progress-title">
    <div class="parameter-progress-card">
        <div class="parameter-progress-spinner" aria-hidden="true"></div>
        <div id="parameter-progress-title" class="parameter-progress-title">Bezig met BMS-instelling…</div>
        <div id="parameter-progress-text" class="parameter-progress-text">De waarde wordt geschreven en daarna opnieuw uitgelezen.</div>
        <div class="parameter-progress-warning">Wacht tot deze melding vanzelf verdwijnt. Wijzig of verstuur ondertussen niets.</div>
    </div>
</div>

<main>


<div class="page-top">

    <div>

        <h1>
            PACE BMS Monitor
        </h1>

        <div class="muted">
            RS232 multistack · live web · MQTT 5 s · historie 2 uur
        </div>

    </div>

    <div
        id="api-status"
        class="muted"
    >
        Web live…
    </div>

</div>


{% for target, message in all_messages.items() %}
{% if message %}<div class="message">{{ target }}: {{ message }}</div>{% endif %}
{% endfor %}


<!-- ======================================================
     GRAFIEKEN
     ====================================================== -->

<div id="connection-status">Verbinding controleren…</div><div class="chart-grid">


<div class="chart-card">

    <div class="chart-title">

        <strong>
            SOC
        </strong>

        <span class="muted">
            %
        </span>

    </div>

    <div class="chart-live" id="headline_soc">Wachten…</div>

    <canvas id="chart_soc"></canvas>

</div>


<div class="chart-card">

    <div class="chart-title">

        <strong>
            Stroom
        </strong>

        <span class="muted">
            A
        </span>

    </div>

    <div class="chart-live" id="headline_current">Wachten…</div>

    <canvas id="chart_current"></canvas>

</div>


<div class="chart-card">

    <div class="chart-title">

        <strong>
            Spanning
        </strong>

        <span class="muted">
            V
        </span>

    </div>

    <div class="chart-live" id="headline_voltage">Wachten…</div>

    <canvas id="chart_voltage"></canvas>

</div>


</div>


<!-- ======================================================
     BMS KOLOMMEN
     ====================================================== -->

<div class="bms-grid">


{% for bms in ["BMS1", "BMS2", "BMS3"] %}

{% set key = bms.lower() %}

<div class="bms-column">


<div
    class="bms-panel"
    id="{{ key }}_panel"
>

    <div class="bms-title">

        <strong>

            {{ bms }}

            {% if bms == "BMS1" %}
                · master
            {% endif %}

        </strong>

        <span
            class="live-time"
            id="{{ key }}_time"
        >
            wachten…
        </span>

    </div>


    <div class="metrics">


        <div class="metric">

            <div class="metric-label">
                SOC
            </div>

            <div
                class="metric-value"
                id="{{ key }}_soc"
            >
                –
            </div>

        </div>


        <div class="metric">

            <div class="metric-label">
                Spanning
            </div>

            <div
                class="metric-value"
                id="{{ key }}_voltage"
            >
                –
            </div>

        </div>


        <div class="metric">

            <div class="metric-label">
                Stroom
            </div>

            <div
                class="metric-value"
                id="{{ key }}_current"
            >
                –
            </div>

        </div>


        <div class="metric">

            <div class="metric-label">
                Vermogen
            </div>

            <div
                class="metric-value"
                id="{{ key }}_power"
            >
                –
            </div>

        </div>


        <div class="metric">

            <div class="metric-label">
                Delta
            </div>

            <div
                class="metric-value"
                id="{{ key }}_delta"
            >
                –
            </div>

        </div>


    </div>


    <div class="cells">

        {% for i in range(1, 17) %}

        <div
            class="cell"
            id="{{ key }}_cellbox_{{ i }}"
        >

            <span class="cell-number">
                C{{ "%02d"|format(i) }}
            </span>

            <span
                class="bl-badge"
                id="{{ key }}_bl_{{ i }}"
            >BL</span>

            <span
                class="cell-value"
                id="{{ key }}_cell_{{ i }}"
            >
                –
            </span>

        </div>

        {% endfor %}

    </div>


    


    <div class="pack-state" id="{{ key }}_status">Wachten…</div>
<div class="status-list">


        <div class="status-row">

            <div class="status-name">
                Meldingen
            </div>

            <div
                class="status-value"
                id="{{ key }}_messages"
            >
                –
            </div>

        </div>


        <div class="status-row">

            <div class="status-name">
                Balanceren
            </div>

            <div
                class="status-value"
                id="{{ key }}_balancing"
            >
                –
            </div>

        </div>


        <div class="status-row">

            <div class="status-name">
                MOSFET
            </div>

            <div
                class="status-value"
                id="{{ key }}_mosfet"
            >
                –
            </div>

        </div>


        <div class="status-row">

            <div class="status-name">
                Volmelding
            </div>

            <div
                class="status-value"
                id="{{ key }}_full"
            >
                –
            </div>

        </div>


    </div>


    <div class="status-row">
        <div class="status-name" title="Volle capaciteit volgens het BMS">FCC / cycli</div>
        <div class="status-value" id="{{ key }}_info">–</div>
    </div>
    <details class="diagnostics"><summary>Diagnostiek</summary>
<p>FCC: volle capaciteit volgens het BMS. Een volmelding en een FCC-wijziging zijn afzonderlijke gebeurtenissen.</p>
<p>CAN CCL: niet uitgelezen. De ruwe limiterbits hieronder geven geen numerieke laadstroomlimiet aan.</p>
<div id="{{ key }}_charge_limiter"></div>

<pre class="decoded-status" id="{{ key }}_decoded"></pre><div id="{{ key }}_status35"></div>
<div id="{{ key }}_raw_status"></div>
<div id="{{ key }}_raw44"></div>
<div id="{{ key }}_raw44_indexed"></div>
<p>0x44 volgens PACE A-Series layout: P1, P2/Fully, Instructions, Control, Fault, Balance C01-C08, Balance C09-C16, Warning1, Warning2, extra firmwarebyte.</p></details>
    <div
        class="temp-line"
        id="{{ key }}_temps"
    >
        T: –
    </div>





</div>





</div>

{% endfor %}


</div>


<section id="settings-v2" aria-label="BMS instellingen">
<header class="settings-title"><div><h2>Instellingen <span class="version-badge">V2.1</span></h2><p class="muted">Vergelijk de drie accu’s en sla wijzigingen per BMS op.</p></div></header>
<div class="settings-read-row"><div class="muted">Laatst uitgelezen</div>{% for bms in ["BMS1","BMS2","BMS3"] %}
<div class="settings-pack {{ bms|lower }}"><strong>{{ bms }}{% if bms == 'BMS1' %} · master{% endif %}</strong>
<p class="muted">{{ all_params[bms].read_at if all_params.get(bms) else 'Nog niet gelezen' }}</p>
<form method="post" action="/parameters/read"><input type="hidden" name="bms" value="{{ bms }}"><button class="read" type="submit">Opnieuw lezen</button></form>
{% if all_errors.get(bms) %}<p class="bad">{{ all_errors[bms] }}</p>{% endif %}</div>{% endfor %}</div><section class="settings-group" id="settings-stroom"><h3 class="group-title">Laadstroom</h3><div class="settings-comparison">{% for bms in ["BMS1","BMS2","BMS3"] %}{% set params = all_params.get(bms) %}
<div class="settings-pack {{ bms|lower }}"><h3 class="pack-heading">{{ bms }}{% if bms == 'BMS1' %} · master{% endif %}</h3>{% if params %}<div class="param-card">
<h3>Laad-overstroom (CHG OC)</h3>
<p class="muted">Waarschuwing en beveiliging bij te hoge laadstroom.</p>
<form method="post" action="/parameters/charge-overcurrent">
<input type="hidden" name="bms" value="{{ bms }}">
<label>Waarschuwing (A)
<input name="warning_a" type="number" step="1" min="1" max="220"
 value="{{ params.charge_overcurrent.warning_a }}" required></label>
<label>Protect (A)
<input name="protection_a" type="number" step="1" min="1" max="220"
 value="{{ params.charge_overcurrent.protection_a }}" required></label>
<label>Delay (ms)
<input name="delay_ms" type="number" step="100" min="500" max="25000"
 value="{{ params.charge_overcurrent.delay_100ms * 100 }}" required></label>
<button class="write" type="submit">Opslaan en controleren</button>
</form>
</div>{% else %}<p class="muted">Nog geen instellingen beschikbaar. Gebruik Opnieuw lezen.</p>{% endif %}{% if params %}<div class="param-card">
<h3>Charge Current Limiter</h3>
<p class="muted">Limiterinstellingen van het BMS. De CAN-laadstroomlimiet wordt niet uitgelezen.</p>
<form method="post" action="/parameters/limiter-start">
<input type="hidden" name="bms" value="{{ bms }}">
<input type="hidden" name="payload_address" value="{{ params.limiter_start.payload_address }}">
<label>Inschakelstroom (A)
<input name="start_current_a" type="number" step="1" min="5" max="255"
 value="{{ params.limiter_start.start_current_a }}" required></label>

<button class="write" type="submit">Opslaan en controleren</button>
</form>
{% set ls = params.limiter_status %}
{% if ls.available %}
<p>Configuratie: <strong>{{ 'Aan' if ls.enabled else 'Uit' }}</strong> · gear <strong>{{ ls.gear|upper }}</strong></p>

<form method="post" action="/parameters/limiter-switch">
<input type="hidden" name="bms" value="{{ bms }}">
<label>Limiter aan/uit
<select name="enabled"><option value="1" {% if ls.enabled %}selected{% endif %}>Aan</option><option value="0" {% if not ls.enabled %}selected{% endif %}>Uit</option></select></label>
<button class="write" type="submit">Opslaan en controleren</button>
</form>
<form method="post" action="/parameters/limiter-gear">
<input type="hidden" name="bms" value="{{ bms }}">
<label>Gear
<select name="gear"><option value="high" {% if ls.gear == 'high' %}selected{% endif %}>High</option><option value="low" {% if ls.gear == 'low' %}selected{% endif %}>Low</option></select></label>
<button class="write" type="submit">Opslaan en controleren</button>
</form>
{% else %}<p>Status nog niet beschikbaar; wacht op een geldige live-uitlezing.</p>{% endif %}
</div>{% else %}<p class="muted">Nog geen instellingen beschikbaar. Gebruik Opnieuw lezen.</p>{% endif %}</div>{% endfor %}</div></section><section class="settings-group" id="settings-vol"><h3 class="group-title">Volmelding en lage SOC</h3><div class="settings-comparison">{% for bms in ["BMS1","BMS2","BMS3"] %}{% set params = all_params.get(bms) %}
<div class="settings-pack {{ bms|lower }}"><h3 class="pack-heading">{{ bms }}{% if bms == 'BMS1' %} · master{% endif %}</h3>{% if params %}<div class="param-card full">

<h3>
    Volmelding
</h3>

<form
    method="post"
    action="/parameters/full-charge"
>
<input type="hidden" name="bms" value="{{ bms }}">

<label>
Spanning voor volmelding (V)

<input
    name="voltage_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.full_charge.voltage_mv / 1000) }}"
    required
>

</label>


<label>
Stroomdrempel voor volmelding (mA)

<input
    name="current_ma"
    type="number"
    step="1"
    value="{{ params.full_charge.current_ma }}"
    required
>

</label>


<label>
Lage-SOC-waarschuwing (%)

<input
    name="low_soc"
    type="number"
    step="1"
    value="{{ params.full_charge.low_soc }}"
    required
>

</label>


<button
    class="write"
    type="submit"
>
    Opslaan en controleren
</button>

</form>
</div>{% else %}<p class="muted">Nog geen instellingen beschikbaar. Gebruik Opnieuw lezen.</p>{% endif %}</div>{% endfor %}</div></section><section class="settings-group" id="settings-balans"><h3 class="group-title">Balanceren</h3><div class="settings-comparison">{% for bms in ["BMS1","BMS2","BMS3"] %}{% set params = all_params.get(bms) %}
<div class="settings-pack {{ bms|lower }}"><h3 class="pack-heading">{{ bms }}{% if bms == 'BMS1' %} · master{% endif %}</h3>{% if params %}<div class="param-card">

<h3>
    Balanceren
</h3>

<form
    method="post"
    action="/parameters/balancing"
>
<input type="hidden" name="bms" value="{{ bms }}">

<label>
Startspanning (V)

<input
    name="threshold_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.balancing.threshold_mv / 1000) }}"
    required
>

</label>


<label>
Delta (mV)

<input
    name="delta_mv"
    type="number"
    step="1"
    value="{{ params.balancing.delta_mv }}"
    required
>

</label>


<button
    class="write"
    type="submit"
>
    Opslaan en controleren
</button>

</form>

</div>{% else %}<p class="muted">Nog geen instellingen beschikbaar. Gebruik Opnieuw lezen.</p>{% endif %}</div>{% endfor %}</div></section><section class="settings-group" id="settings-beveiliging"><h3 class="group-title">Spanningsgrenzen · waarschuwingen en beveiliging</h3><div class="settings-comparison">{% for bms in ["BMS1","BMS2","BMS3"] %}{% set params = all_params.get(bms) %}
<div class="settings-pack {{ bms|lower }}"><h3 class="pack-heading">{{ bms }}{% if bms == 'BMS1' %} · master{% endif %}</h3>{% if params %}
<div class="param-card">

<h3>
    Accu · bovengrens
</h3>

<form
    method="post"
    action="/parameters/pack-ov"
>
<input type="hidden" name="bms" value="{{ bms }}">

<label>
Waarschuwing (V) / Pack OV

<input
    name="alarm_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.pack_ov.alarm_mv / 1000) }}"
    required
>

</label>


<label>
Beveiliging (V) / Pack OVP

<input
    name="protection_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.pack_ov.protection_mv / 1000) }}"
    required
>

</label>


<label>
Herstel (V)

<input
    name="release_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.pack_ov.release_mv / 1000) }}"
    required
>

</label>


<label>
Vertraging (s)

<input
    name="delay_s"
    type="number"
    step="0.1"
    value="{{ '%.1f'|format(params.pack_ov.delay_100ms / 10) }}"
    required
>

</label>


<button
    class="write"
    type="submit"
>
    Opslaan en controleren
</button>

</form>

</div>
<div class="param-card">

<h3>
    Cel · bovengrens
</h3>

<form
    method="post"
    action="/parameters/cell-ov"
>
<input type="hidden" name="bms" value="{{ bms }}">

<label>
Waarschuwing (V) / Cel OV

<input
    name="alarm_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.cell_ov.alarm_mv / 1000) }}"
    required
>

</label>


<label>
Beveiliging (V) / Cel OVP

<input
    name="protection_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.cell_ov.protection_mv / 1000) }}"
    required
>

</label>


<label>
Herstel (V)

<input
    name="release_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.cell_ov.release_mv / 1000) }}"
    required
>

</label>


<label>
Vertraging (s)

<input
    name="delay_s"
    type="number"
    step="0.1"
    value="{{ '%.1f'|format(params.cell_ov.delay_100ms / 10) }}"
    required
>

</label>


<button
    class="write"
    type="submit"
>
    Opslaan en controleren
</button>

</form>

</div>
<div class="param-card">

<h3>
    Accu · ondergrens
</h3>

<form
    method="post"
    action="/parameters/pack-uv"
>
<input type="hidden" name="bms" value="{{ bms }}">

<label>
Waarschuwing (V)

<input
    name="alarm_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.pack_uv.alarm_mv / 1000) }}"
    required
>

</label>


<label>
Beveiliging (V)

<input
    name="protection_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.pack_uv.protection_mv / 1000) }}"
    required
>

</label>


<label>
Herstel (V)

<input
    name="release_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.pack_uv.release_mv / 1000) }}"
    required
>

</label>


<label>
Vertraging (s)

<input
    name="delay_s"
    type="number"
    step="0.1"
    value="{{ '%.1f'|format(params.pack_uv.delay_100ms / 10) }}"
    required
>

</label>


<button
    class="write"
    type="submit"
>
    Opslaan en controleren
</button>

</form>

</div>
<div class="param-card">

<h3>
    Cel · ondergrens
</h3>

<form
    method="post"
    action="/parameters/cell-uv"
>
<input type="hidden" name="bms" value="{{ bms }}">

<label>
Waarschuwing (V)

<input
    name="alarm_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.cell_uv.alarm_mv / 1000) }}"
    required
>

</label>


<label>
Beveiliging (V)

<input
    name="protection_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.cell_uv.protection_mv / 1000) }}"
    required
>

</label>


<label>
Herstel (V)

<input
    name="release_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.cell_uv.release_mv / 1000) }}"
    required
>

</label>


<label>
Vertraging (s)

<input
    name="delay_s"
    type="number"
    step="0.1"
    value="{{ '%.1f'|format(params.cell_uv.delay_100ms / 10) }}"
    required
>

</label>


<button
    class="write"
    type="submit"
>
    Opslaan en controleren
</button>

</form>

</div>
{% else %}<p class="muted">Nog geen instellingen beschikbaar. Gebruik Opnieuw lezen.</p>{% endif %}</div>{% endfor %}</div></section>
<section class="settings-group" id="settings-overig"><h3 class="group-title">Temperatuur, energiestand en klok</h3><div class="settings-comparison">{% for bms in ["BMS1","BMS2","BMS3"] %}{% set params = all_params.get(bms) %}
<div class="settings-pack {{ bms|lower }}"><h3 class="pack-heading">{{ bms }}{% if bms == 'BMS1' %} · master{% endif %}</h3>{% if params %}<div class="param-card">

<h3>
    Energiestand (Sleep)
</h3>

<form
    method="post"
    action="/parameters/sleep"
>
<input type="hidden" name="bms" value="{{ bms }}">

<label>
Spanningsdrempel (V)

<input
    name="voltage_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.sleep.voltage_mv / 1000) }}"
    required
>

</label>


<label>
Vertraging (min)

<input
    name="delay_min"
    type="number"
    step="1"
    value="{{ params.sleep.delay_min }}"
    required
>

</label>


<button
    class="write"
    type="submit"
>
    Opslaan en controleren
</button>

</form>

</div>{% else %}<p class="muted">Nog geen instellingen beschikbaar. Gebruik Opnieuw lezen.</p>{% endif %}<section class="param-card" style="margin:8px 0"><h3 class="group-title">Temperatuurinstellingen</h3>
{% set ex = extra[bms] %}
{% for group, spec in temp_groups.items() %}
<h3>{{ spec[2] }}</h3>
{% if ex.errors.get(group) %}<p class="bad">{{ ex.errors[group] }}</p>{% endif %}
{% if group in ex.temps %}
<form method="post" action="/parameters/temperature">
<input type="hidden" name="bms" value="{{ bms }}"><input type="hidden" name="group" value="{{ group }}">
{% for label in spec[3] %}{% set row = loop.index0 %}
{% for field in ['Waarschuwing','Beveiliging','Herstel'] %}
<label>{{ label }} · {{ field }} (°C)<input type="number" name="t{{ row*3+loop.index0 }}" step="1" min="-40" max="125" value="{{ ex.temps[group][row*3+loop.index0] }}" required></label>
{% endfor %}{% endfor %}
<button class="write" type="submit">Opslaan en controleren</button></form>
{% else %}<p>Nog niet gelezen; gebruik Opnieuw lezen.</p>{% endif %}
{% endfor %}</section>
<section class="param-card" style="margin:8px 0"><h3 class="group-title">Datum en tijd BMS</h3>
{% set ex = extra[bms] %}
<p>BMS-klok: {{ ex.clock or 'Nog niet gelezen' }}</p>
<p class="muted">Momentopname · gelezen {{ ex.get('clock_read_at','–') }}. Synchroniseren gebruikt Nederlandse tijd van de Pi.</p>
{% if ex.errors.get('clock') %}<p class="bad">{{ ex.errors.clock }}</p>{% endif %}
<form method="post" action="/parameters/clock">
<input type="hidden" name="bms" value="{{ bms }}"><input type="hidden" name="mode" value="manual">
<label>Datum en tijd<input type="datetime-local" name="clock" step="1" min="2000-01-01T00:00" max="2099-12-31T23:59:59" value="{{ ex.clock or '' }}" required></label>
<button class="write" type="submit">Opslaan en controleren</button></form>
<form method="post" action="/parameters/clock">
<input type="hidden" name="bms" value="{{ bms }}"><input type="hidden" name="mode" value="sync">
<button class="write" type="submit">Synchroniseer met Pi-tijd</button></form>
</section>

</div>{% endfor %}</div></section><section class="settings-group" id="settings-communicatie"><h3 class="group-title">Communicatie</h3><div class="settings-comparison">{% for bms in ["BMS1","BMS2","BMS3"] %}{% set params = all_params.get(bms) %}
<div class="settings-pack {{ bms|lower }}"><h3 class="pack-heading">{{ bms }}{% if bms == 'BMS1' %} · master{% endif %}</h3>{% if params %}<div class="param-card protocol-readable">
<h3>Communicatie</h3>
<dl><dt>Omvormer (CAN)</dt><dd>{{ params.protocols.can_name.split(' (')[0] }}</dd>
<dt>RS485</dt><dd>{{ params.protocols.rs485_name.split(' (')[0] }}</dd>
<dt>Uitlezen</dt><dd>RS232 via USB</dd>
<dt>Protocolkeuze</dt><dd>{{ 'Handmatig' if params.protocols.selection_raw == 1 else params.protocols.selection_name }}</dd></dl>
<span class="muted">Alleen uitlezen</span></div>{% else %}<p class="muted">Nog geen instellingen beschikbaar. Gebruik Opnieuw lezen.</p>{% endif %}</div>{% endfor %}</div></section></section>

<details id="event-log"><summary>Logboek en JSON-export</summary>
<p>Volmeldingen, waarschuwingen, instellingen en FCC-wijzigingen. Balansstatus wordt bij de metingen bewaard.</p>
<form action="/api/events/export" method="get" style="display:flex;flex-wrap:wrap;align-items:end;gap:10px">
<label>Periode<br><select name="period" id="export-period" style="padding:8px;background:#12151b;color:#e5e7eb;border:1px solid #555;border-radius:5px">
<option value="all">Alles wat bewaard is</option>
<option value="1h" selected>Afgelopen 1 uur</option><option value="6h">Afgelopen 6 uur</option><option value="12h">Afgelopen 12 uur</option>
<option value="24h">Afgelopen 24 uur</option><option value="today">Vandaag</option>
<option value="yesterday">Gisteren</option><option value="custom">Zelf kiezen</option>
</select></label>
<span id="export-custom" hidden>
<label>Vanaf <input type="datetime-local" name="start" disabled></label>
<label>Tot (exclusief) <input type="datetime-local" name="end" disabled></label>
</span>
<button class="read" type="submit">Download JSON</button>
</form>
<p class="muted">Metingen elke 5 seconden · 72 uur bewaard. Gebeurtenissen: 30 dagen, maximaal 2.000. FCC/cycli: 1 jaar, maximaal 3.000. Export bevat alleen wat nog bewaard is.</p>
<p id="event-log-state">Log laden…</p><div id="event-list"></div></details></main>


<script>

(function () {

"use strict";

// Scrollpositie na opslaan/lezen; alle instellingen zijn altijd zichtbaar.
const settingsStateKey='pace-settings-v21';
try{const y=sessionStorage.getItem(settingsStateKey);if(y!==null){sessionStorage.removeItem(settingsStateKey);requestAnimationFrame(()=>window.scrollTo(0,Number(y)));}}catch(e){}
function saveSettingsView(){try{sessionStorage.setItem(settingsStateKey,String(window.scrollY));}catch(e){}}
const parameterProgress=document.getElementById('parameter-progress');
let parameterSubmitBusy=false;

function clearParameterProgress(){
    parameterSubmitBusy=false;
    document.body.classList.remove('parameter-busy');
    document.body.removeAttribute('aria-busy');
    parameterProgress.hidden=true;
    document.querySelector('main').inert=false;
}

function showParameterProgress(form){
    if(parameterSubmitBusy)return false;
    parameterSubmitBusy=true;
    const bms=form.querySelector('input[name="bms"]')?.value || 'BMS';
    const submitter=document.activeElement && document.activeElement.form===form ? document.activeElement : null;
    const isRead=form.action.endsWith('/parameters/read');
    const action=isRead?'Parameters lezen en scherm vernieuwen':'Instelling schrijven, controleren en scherm vernieuwen';
    document.getElementById('parameter-progress-title').textContent=bms+' · bezig…';
    document.getElementById('parameter-progress-text').textContent=action+'. Dit kan enkele seconden duren.';
    if(submitter && submitter.tagName==='BUTTON')submitter.textContent='Bezig…';
    document.body.classList.add('parameter-busy');
    document.body.setAttribute('aria-busy','true');
    parameterProgress.hidden=false;
    document.querySelector('main').inert=true;
    return true;
}

for(const form of document.querySelectorAll('form[method="post"][action^="/parameters/"]')){
    form.addEventListener('submit',event=>{
        if(!showParameterProgress(form))event.preventDefault();
        else saveSettingsView(true);
    });
}

// A page restored with the browser Back button must never retain the old busy screen.
window.addEventListener('pageshow',clearParameterProgress);

const exportPeriod=document.getElementById('export-period');
exportPeriod.addEventListener('change',()=>{
    const custom=exportPeriod.value==='custom';
    const fields=document.getElementById('export-custom');fields.hidden=!custom;
    for(const input of fields.querySelectorAll('input')){input.disabled=!custom;input.required=custom;}
});


function el(id) {

    return document.getElementById(
        id
    );

}


function setText(
    id,
    value
) {

    const node = el(id);

    if (node) {

        node.textContent = value;

    }

}


function fmt(
    value,
    decimals
) {

    if (
        value === null
        ||
        value === undefined
    ) {

        return "–";

    }

    return Number(
        value
    ).toFixed(
        decimals
    );

}


function hasCell(
    array,
    number
) {

    return (
        Array.isArray(array)
        &&
        array.includes(number)
    );

}


function updateCells(
    key,
    data
) {

    if (
        !Array.isArray(
            data.cells
        )
    ) {

        return;

    }


    data.cells.forEach(
        function (
            voltage,
            index
        ) {

            const cell = (
                index + 1
            );

            setText(
                key
                + "_cell_"
                + cell,

                fmt(
                    voltage,
                    3
                )
            );


            const box = el(
                key
                + "_cellbox_"
                + cell
            );

            const bl = el(
                key
                + "_bl_"
                + cell
            );


            if (box) {
                const label = box.querySelector('span');
                label.style.color = data.max_v !== data.min_v && voltage === data.max_v ? '#ff6b6b' :
                    data.max_v !== data.min_v && voltage === data.min_v ? '#65d48b' : '';


                box.classList.remove(
                    "cell-ov",
                    "cell-ovp"
                );


                if (
                    hasCell(
                        data.ovp_cells,
                        cell
                    )
                ) {

                    box.classList.add(
                        "cell-ovp"
                    );

                }
                else if (
                    hasCell(
                        data.high_warning_cells,
                        cell
                    )
                ) {

                    box.classList.add(
                        "cell-ov"
                    );

                }

            }


            if (bl) {
                if (hasCell(data.balancing_cells, cell)) {
                    bl.classList.add("active");
                    /* BL heeft een vaste middenkolom; geen extra padding nodig. */
                }
                else {
                    bl.classList.remove("active");
                    /* vaste BL-kolom blijft gereserveerd */
                }
            }

        }
    );

}


function updateBms(
    name,
    data
) {

    if (!data) {

        return;

    }

    const key = (
        name.toLowerCase()
    );


    setText(
        key + "_time",
        "LIVE "
        + data.time
    );


    setText(
        key + "_soc",
        fmt(
            data.soc,
            1
        )
        + "%"
    );


    setText(
        key + "_voltage",
        fmt(
            data.voltage,
            2
        )
        + " V"
    );


    setText(
        key + "_current",
        fmt(
            data.current,
            1
        )
        + " A"
    );


    setText(
        key + "_power",
        fmt(
            data.power,
            0
        )
        + " W"
    );


    setText(
        key + "_delta",
        data.delta_mv
        + " mV"
    );


    const deltaEl = el(key + '_delta');
    deltaEl.style.color = data.delta_mv > data.delta_threshold_mv ? '#ffb74d' : '#65d48b';
    deltaEl.title = 'Oranje bij meer dan ' + data.delta_threshold_mv + ' mV · ' + data.delta_threshold_source;
    setText(key + '_status', (data.current > 0.1 ? 'Laden' : data.current < -0.1 ? 'Ontladen' : 'Rust'));
    setText(key + '_full', data.fully_charged);
    setText(key + '_temps', 'T: ' + data.temperatures.map(t=>Number(t).toFixed(1)+' °C').join(' · '));
    setText(key + '_status35', 'Balance state 2 (C09-C16): ' + data.status35);
    setText(key + '_raw_status', data.raw_status);
    setText(key + '_decoded', JSON.stringify(data.decoded_status,null,2));
    if (Date.now()/1000 - data.timestamp > 15) setText(key + '_time', 'VEROUDERD · ' + data.time);
    updateCells(
        key,
        data
    );


    const messages=[];
    if(data.fault !== 'geen') messages.push('STORING: '+data.fault);
    if(data.protection !== 'geen') messages.push('BEVEILIGING: '+data.protection);
    if(data.alarm !== 'geen') messages.push('WAARSCHUWING: '+data.alarm);
    let message = messages.join(' · ') || 'Geen';


    setText(
        key + "_messages",
        message
    );


    setText(
        key + "_balancing",
        data.balancing === "geen" ? "Geen" : data.balancing
    );


    setText(
        key + "_charge_limiter",
        data.charge_limiter
    );


    setText(
        key + "_mosfet",

        "Laden "
        + data.charge_mosfet

        + " · Ontladen "
        + data.discharge_mosfet
    );


    setText(
        key + "_info",

        fmt(
            data.full_capacity_ah,
            2
        )
        + " Ah · cycli "
        + data.cycle_count
    );


    setText(
        key + "_raw44",
        data.raw44
    );


    setText(
        key + "_raw44_indexed",
        data.raw44_indexed
    );

}


let eventVersion='';
const hexByte=v=>v===null?'—':Number(v).toString(16).toUpperCase().padStart(2,'0');
async function refreshEvents(){
    const panel=document.getElementById('event-log');
    if(!panel.open)return;
    try{
        const r=await fetch('/api/events',{cache:'no-store'});if(!r.ok)throw Error('HTTP '+r.status);
        const data=await r.json();
        document.getElementById('event-log-state').textContent=data.error?'Logfout: '+data.error:'Laatste '+data.events.length+' gebeurtenissen';
        const version=JSON.stringify(data.events.map(e=>e.id));if(version===eventVersion)return;eventVersion=version;
        const list=document.getElementById('event-list');list.replaceChildren();
        for(const e of data.events){
            const row=document.createElement('details'),summary=document.createElement('summary');
            const d=e.detail;
            const changes=e.kind==='status'?d.changes.map(c=>'byte '+c.byte+' '+hexByte(c.from)+'→'+hexByte(c.to)+(c.bits.length?' ['+c.bits.map(b=>'b'+b.bit+' '+b.from+'→'+b.to+(b.first_observed?' nieuw waargenomen':'')).join(', ')+']':'')).join(' · '):
                e.kind==='setting_write'?'Instelling geschreven: '+d.setting:(e.kind==='parameters'||e.kind==='capacity')?d.changes.map(c=>c.parameter+' '+c.from+'→'+c.to).join(' · '):'Beginmeting vastgelegd';
            summary.textContent=new Date(e.timestamp*1000).toLocaleString('nl-NL')+' · '+e.bms+' · '+changes;
            row.appendChild(summary);
            if(e.kind==='status'){
                const context=document.createElement('p');
                const keys={current:'Stroom (A)',voltage:'Spanning (V)',soc:'SOC (%)',delta_mv:'Delta (mV)',max_cell:'Hoogste cel',max_v:'Hoogste spanning (V)',charging:'Laadstatus',full_capacity_ah:'FCC (Ah)'};
                context.textContent=Object.entries(keys).map(([k,label])=>label+': '+d.before?.[k]+' → '+d.after[k]).join(' · ');
                row.appendChild(context);
                if(d.recent_parameters){const note=document.createElement('p');note.textContent='Parameterwijziging van dit BMS waargenomen in de voorgaande 120 seconden; dit bewijst geen oorzaak.';row.appendChild(note);}
            }
            const raw=document.createElement('pre');raw.textContent=JSON.stringify(d,null,2);row.appendChild(raw);list.appendChild(row);
        }
    }catch(e){document.getElementById('event-log-state').textContent='Log niet beschikbaar: '+e.message;}
}
document.getElementById('event-log').addEventListener('toggle',refreshEvents);
setInterval(refreshEvents,5000);

const packColors = {BMS1:'#65d48b', BMS2:'#56a4ff', BMS3:'#ffab40'};
let historyPacks = {BMS1:[], BMS2:[], BMS3:[]};
let lastLive = {};
function drawCharts() {
    const spanSeconds=7200;
    const end = Date.now()/1000, start = end-spanSeconds;
    for (const field of ['soc','current','voltage']) {
        const canvas = document.getElementById('chart_'+field);
        const width = canvas.clientWidth, height = canvas.clientHeight;
        if (!width) continue;
        const dpr = window.devicePixelRatio || 1;
        canvas.width = width*dpr; canvas.height = height*dpr;
        const ctx = canvas.getContext('2d'); ctx.scale(dpr,dpr);
        const values = Object.values(historyPacks).flat().filter(p=>p.timestamp>=start && p.timestamp<=end && Number.isFinite(p[field])).map(p=>p[field]);
        let lo = values.length ? Math.min(...values) : 0, hi = values.length ? Math.max(...values) : 1;
        const pad = Math.max((hi-lo)*0.12, field==='voltage'?0.03:field==='soc'?0.2:0.5);
        lo-=pad; hi+=pad;
        if(field==='soc'){lo=Math.max(0,lo);hi=Math.min(100,hi);}
        const decimals = field==='voltage' || (hi-lo)/5<0.1 ? 2 : 1;
        const left=58, right=14, top=15, bottom=30;
        const x=t=>left+(t-start)/spanSeconds*(width-left-right);
        const y=v=>height-bottom-(v-lo)/(hi-lo)*(height-top-bottom);
        ctx.font='12px system-ui'; ctx.lineWidth=1;
        for(let i=0;i<=5;i++) {
            const v=lo+(hi-lo)*i/5, py=y(v);
            ctx.strokeStyle='#333b49'; ctx.beginPath();ctx.moveTo(left,py);ctx.lineTo(width-right,py);ctx.stroke();
            ctx.fillStyle='#b7c0cf';ctx.textAlign='right';ctx.fillText(v.toFixed(decimals),left-7,py+4);
        }
        for(let i=0;i<=4;i++) {
            const t=start+spanSeconds*i/4;
            ctx.textAlign=i===0?'left':i===4?'right':'center';
            ctx.fillText(new Date(t*1000).toLocaleTimeString('nl-NL',{hour:'2-digit',minute:'2-digit'}),x(t),height-8);
        }
        ctx.save();ctx.beginPath();ctx.rect(left,top,width-left-right,height-top-bottom);ctx.clip();
        for(const [bms,color] of Object.entries(packColors)) {
            ctx.strokeStyle=color;ctx.fillStyle=color;ctx.lineWidth=1.4;
            ctx.beginPath();let previous=null;
            for(const p of historyPacks[bms] || []) {
                if(p.timestamp<start || p.timestamp>end || !Number.isFinite(p[field])) {previous=null;continue;}
                if(!previous || p.timestamp-previous.timestamp>20) ctx.moveTo(x(p.timestamp),y(p[field]));
                else ctx.lineTo(x(p.timestamp),y(p[field]));
                previous=p;
            }
            ctx.stroke();
        }
        ctx.restore();
        if(!values.length) {ctx.fillStyle='#b7c0cf';ctx.textAlign='center';ctx.fillText('Wachten op metingen',width/2,height/2);}
    }
}
function updateHeadlines(data) {
    lastLive=data;
    setText('connection-status', 'MQTT: '+(data._health?.mqtt_connected?'verbonden':'niet verbonden')+' · Historie: '+(data._health?.history_error || 'SQLite actief')+' · '+Object.entries(data._health?.connections || {}).map(([b,h])=>b+': '+(h.error || (h.verified?'eigen RS232 OK':'verbinden…'))).join(' · ')); 
    for(const field of ['soc','current','voltage']) {
        const container=document.getElementById('headline_'+field);container.replaceChildren();
        for(const [bms,color] of Object.entries(packColors)) {
            const span=document.createElement('span');span.style.color=color;
            const p=data[bms];const fresh=p && Date.now()/1000-p.timestamp<=15;
            span.className='chart-reading';
            const label=document.createElement('span');label.className='chart-label';label.textContent=bms;
            const value=document.createElement('span');value.className='chart-number';value.textContent=fresh?Number(p[field]).toFixed(field==='voltage'?2:1):'—';
            span.append(label,value);
            container.appendChild(span);
        }
    }
}
async function refreshHistory() {
        try {
        const r=await fetch('/api/history',{cache:'no-store'});
        if(!r.ok) throw Error(r.status);
        const data=await r.json();historyPacks=data.packs;drawCharts();
        setText('history-status',data.error?'Historie opslaan mislukt: '+data.error:'SQLite · elke 5 s · laatste 2 uur · onderbrekingen blijven zichtbaar');
    } catch(e) {setText('history-status','Historie niet beschikbaar');}
    setTimeout(refreshHistory,5000);
}
window.addEventListener('resize',drawCharts);
refreshHistory();

async function refreshLive() {
    
    try {

        const response = await fetch(
            "/api/live?ts="
            + Date.now(),
            {
                cache:
                    "no-store"
            }
        );

        const data = await (
            response.json()
        );


                updateHeadlines(data);
        updateBms(
            "BMS1",
            data.BMS1
        );

        updateBms(
            "BMS2",
            data.BMS2
        );

        updateBms(
            "BMS3",
            data.BMS3
        );


        setText(
            "api-status",

            "WEB LIVE · "
            + new Date()
                .toLocaleTimeString()
        );

    }
    catch (error) {

        console.error(
            error
        );

        setText(
            "api-status",
            "WEB FOUT"
        );

    }

}


refreshLive();


window.setInterval(
    refreshLive,
    1000
);


})();

</script>
</body></html>"""


# ============================================================
# WEB ROUTES
# ============================================================

@app.route("/")
def index():
    with cache_lock:
        page = render_template_string(PAGE, all_params=dict(parameter_cache),
            all_errors=dict(last_parameter_error), all_messages=dict(last_action_message),
            capacities=dict(capacity_cache), capacity_errors=dict(capacity_errors),
            extra=extra_settings,temp_groups=TEMP_GROUPS)
        for bms in last_action_message:
            last_action_message[bms] = None
        return page


@app.route(
    "/api/live",
    methods=["GET"],
)
def api_live():

    with cache_lock:

        data = {
            "BMS1":
                live_cache.get(
                    "BMS1"
                ),

            "BMS2":
                live_cache.get(
                    "BMS2"
                ),

            "BMS3":
                live_cache.get(
                    "BMS3"
                ),
        }

    data["_health"] = {"mqtt_connected": bool(mqtt_client and mqtt_client.is_connected()), "history_error": history_error, "connections": {b: dict(h) for b, h in connection_health.items()}}
    response = jsonify(
        data
    )

    response.headers[
        "Cache-Control"
    ] = (
        "no-store, no-cache, "
        "must-revalidate, max-age=0"
    )

    return response


@app.route(
    "/parameters/read",
    methods=["POST"],
)
def web_read_parameters():

    try:

        read_parameters()

    except Exception as exc:

        last_action_message[current_bms()] = (
            str(exc)
        )

    return redirect(
        url_for("index")
    )


@app.route(
    "/parameters/balancing",
    methods=["POST"],
)
def web_write_balancing():

    try:

        threshold_mv = round(
            float(
                request.form[
                    "threshold_v"
                ]
            )
            * 1000
        )

        delta_mv = int(
            request.form[
                "delta_mv"
            ]
        )

        if not (
            3000
            <= threshold_mv
            <= 4500
        ):

            raise ValueError(
                "Balance startspanning "
                "buiten scriptbereik."
            )

        if not (
            1
            <= delta_mv
            <= 500
        ):

            raise ValueError(
                "Balance delta "
                "buiten scriptbereik."
            )

        with serial_locks[current_bms()]:

            write_balancing(
                threshold_mv,
                delta_mv,
            )

            time.sleep(0.40)

            after = (
                read_parameters()
            )

        b = (
            after[
                "balancing"
            ]
        )

        if not (
            b["threshold_mv"]
            == threshold_mv
            and
            b["delta_mv"]
            == delta_mv
        ):

            raise RuntimeError(
                "Read-back komt "
                "niet overeen."
            )

        last_action_message[current_bms()] = (
            "Balancing WRITE OK · "
            f"{threshold_mv / 1000:.3f} V · "
            f"{delta_mv} mV"
        )

    except Exception as exc:

        last_action_message[current_bms()] = (
            "Balancing WRITE FOUT: "
            f"{exc}"
        )

    return redirect(
        url_for("index")
    )


def read_limiter_status_fresh():
    bms = current_bms()
    address = next(a for a, name in BMS_ADDRESSES.items() if name == bms)
    payload = decode_frame(transact(build_request(address, 0x44), wait=0.30))
    status = parse_status(payload)
    snapshot = {
        "control_raw": f"0x{status['control']:02X}",
        "instructions_raw": f"0x{status['instructions']:02X}",
    }
    return limiter_status_from_snapshot(snapshot)


@app.route("/parameters/charge-overcurrent", methods=["POST"])
def web_write_charge_overcurrent():
    bms = current_bms()
    try:
        warning_a = int(request.form["warning_a"])
        protection_a = int(request.form["protection_a"])
        delay_ms = int(request.form["delay_ms"])
        if not (1 <= warning_a <= 220 and 1 <= protection_a <= 220):
            raise ValueError("CHG OC moet 1..220 A zijn")
        if warning_a > protection_a:
            raise ValueError("Waarschuwing mag niet hoger zijn dan Protect")
        if not (500 <= delay_ms <= 25000 and delay_ms % 100 == 0):
            raise ValueError("Delay moet 500..25000 ms zijn in stappen van 100 ms")
        wanted = {
            "warning_a": warning_a,
            "protection_a": protection_a,
            "delay_100ms": delay_ms // 100,
        }
        with serial_locks[bms]:
            before = parse_charge_overcurrent(param_request(0xD9))
            write_charge_overcurrent(warning_a, protection_a, delay_ms // 100)
            time.sleep(0.40)
            after = parse_charge_overcurrent(param_request(0xD9))
        if after != wanted:
            raise RuntimeError(f"Teruglezing wijkt af: {after}")
        with cache_lock:
            if parameter_cache.get(bms):
                parameter_cache[bms]["charge_overcurrent"] = after
        log_setting_write(bms, "charge_overcurrent", before, after)
        last_action_message[bms] = "CHG OC WRITE OK"
    except Exception as exc:
        last_action_message[bms] = f"CHG OC WRITE FOUT: {exc}"
    return redirect(url_for("index"))


@app.route("/parameters/limiter-start", methods=["POST"])
def web_write_limiter_start():
    bms = current_bms()
    try:
        start_current_a = int(request.form["start_current_a"])
        payload_address = int(request.form["payload_address"])
        if not 5 <= start_current_a <= 255:
            raise ValueError("Limiter-startstroom moet 5..255 A zijn")
        if not 0 <= payload_address <= 255:
            raise ValueError("Ongeldig lokaal payload-adres")
        with serial_locks[bms]:
            before = parse_limiter_start(param_request(0xED))
            write_limiter_start(start_current_a, payload_address)
            time.sleep(0.40)
            after = parse_limiter_start(param_request(0xED))
        if after["start_current_a"] != start_current_a:
            raise RuntimeError(f"Teruglezing wijkt af: {after}")
        with cache_lock:
            if parameter_cache.get(bms):
                parameter_cache[bms]["limiter_start"] = after
        log_setting_write(bms, "limiter_start", before, after)
        last_action_message[bms] = f"Limiter-startstroom WRITE OK · {start_current_a} A"
    except Exception as exc:
        last_action_message[bms] = f"Limiter-startstroom WRITE FOUT: {exc}"
    return redirect(url_for("index"))


def update_limiter_switch(command, expected_field, expected_value, label):
    bms = current_bms()
    try:
        with serial_locks[bms]:
            before = read_limiter_status_fresh()
            write_limiter_switch(command)
            time.sleep(0.50)
            after = read_limiter_status_fresh()
        if after.get(expected_field) != expected_value:
            raise RuntimeError(f"Statuscontrole wijkt af: {after}")
        with cache_lock:
            if parameter_cache.get(bms):
                parameter_cache[bms]["limiter_status"] = after
        log_setting_write(bms, label, before, after)
        last_action_message[bms] = f"{label} WRITE OK"
    except Exception as exc:
        last_action_message[bms] = f"{label} WRITE FOUT: {exc}"
    return redirect(url_for("index"))


@app.route("/parameters/limiter-switch", methods=["POST"])
def web_write_limiter_switch():
    enabled = request.form.get("enabled") == "1"
    return update_limiter_switch(
        0x0B if enabled else 0x0A, "enabled", enabled, "Charge Current Limiter"
    )


@app.route("/parameters/limiter-gear", methods=["POST"])
def web_write_limiter_gear():
    gear = request.form.get("gear")
    if gear not in ("high", "low"):
        last_action_message[current_bms()] = "Limiter gear WRITE FOUT: onbekende gear"
        return redirect(url_for("index"))
    return update_limiter_switch(
        0x08 if gear == "high" else 0x09, "gear", gear, "Limiter gear"
    )


@app.route(
    "/parameters/full-charge",
    methods=["POST"],
)
def web_write_full_charge():

    try:

        voltage_mv = round(
            float(
                request.form[
                    "voltage_v"
                ]
            )
            * 1000
        )

        current_ma = int(
            request.form[
                "current_ma"
            ]
        )

        low_soc = int(
            request.form[
                "low_soc"
            ]
        )

        if not (
            40000
            <= voltage_mv
            <= 65000
        ):

            raise ValueError(
                "Full Charge Voltage "
                "buiten scriptbereik."
            )

        if not (
            0
            <= current_ma
            <= 10000
        ):

            raise ValueError(
                "Full Charge Current "
                "buiten scriptbereik."
            )

        if not (
            0
            <= low_soc
            <= 100
        ):

            raise ValueError(
                "Low SOC moet "
                "0..100% zijn."
            )

        with serial_locks[current_bms()]:

            write_full_charge(
                voltage_mv,
                current_ma,
                low_soc,
            )

            time.sleep(0.40)

            after = (
                read_parameters()
            )

        fc = (
            after[
                "full_charge"
            ]
        )

        if not (
            fc["voltage_mv"]
            == voltage_mv
            and
            fc["current_ma"]
            == current_ma
            and
            fc["low_soc"]
            == low_soc
        ):

            raise RuntimeError(
                "Read-back komt "
                "niet overeen."
            )

        last_action_message[current_bms()] = (
            "Full Charge WRITE OK · "
            f"{voltage_mv / 1000:.3f} V · "
            f"{current_ma} mA · "
            f"{low_soc}%"
        )

    except Exception as exc:

        last_action_message[current_bms()] = (
            "Full Charge WRITE FOUT: "
            f"{exc}"
        )

    return redirect(
        url_for("index")
    )


@app.route(
    "/parameters/sleep",
    methods=["POST"],
)
def web_write_sleep():

    try:

        voltage_mv = round(
            float(
                request.form[
                    "voltage_v"
                ]
            )
            * 1000
        )

        delay_min = int(
            request.form[
                "delay_min"
            ]
        )

        if not (
            2000
            <= voltage_mv
            <= 4000
        ):

            raise ValueError(
                "Sleep voltage "
                "buiten scriptbereik."
            )

        if not (
            1
            <= delay_min
            <= 120
        ):

            raise ValueError(
                "Sleep delay "
                "buiten scriptbereik."
            )

        with serial_locks[current_bms()]:

            write_sleep(
                voltage_mv,
                delay_min,
            )

            time.sleep(0.40)

            after = (
                read_parameters()
            )

        s = (
            after[
                "sleep"
            ]
        )

        if not (
            s["voltage_mv"]
            == voltage_mv
            and
            s["delay_min"]
            == delay_min
        ):

            raise RuntimeError(
                "Read-back komt "
                "niet overeen."
            )

        last_action_message[current_bms()] = (
            "Sleep WRITE OK · "
            f"{voltage_mv / 1000:.3f} V · "
            f"{delay_min} min"
        )

    except Exception as exc:

        last_action_message[current_bms()] = (
            "Sleep WRITE FOUT: "
            f"{exc}"
        )

    return redirect(
        url_for("index")
    )


def write_uv_settings(group):
    bms = current_bms()
    try:
        vals = [round(float(request.form[key])*1000) for key in ("alarm_v","protection_v","release_v")]
        alarm, protection, release = vals
        delay = round(float(request.form["delay_s"])*10)
        low, high = (2000,3500) if group == "cell_uv" else (15000,50000)
        if not all(low <= v <= high and v % 10 == 0 for v in vals):
            raise ValueError(f"Spanningen moeten tussen {low/1000:g} en {high/1000:g} V liggen, in stappen van 0,01 V")
        if not protection < alarm or not protection < release:
            raise ValueError("UVP moet lager liggen dan waarschuwing en herstelspanning")
        if not 0 <= delay <= 255:
            raise ValueError("Vertraging buiten bereik 0–25,5 s")
        read_cmd, write_cmd = (0xD3,0xD2) if group == "cell_uv" else (0xD7,0xD6)
        with serial_locks[bms]:
            before = parse_ov(param_request(read_cmd))
            if before['marker'] != 1:
                raise ValueError("Onbekende UV-layout; niets geschreven")
            payload = b"\x01" + b"".join(v.to_bytes(2,"big") for v in vals) + bytes([delay])
            param_write(write_cmd,payload)
            time.sleep(0.4)
            after = parse_ov(param_request(read_cmd))
            expected = dict(marker=1,alarm_mv=alarm,protection_mv=protection,release_mv=release,delay_100ms=delay)
            if after != expected:
                raise RuntimeError(f"Teruglezing wijkt af: {after}")
            # Refresh the panel and baseline; the exact UV read-back is already verified.
            read_parameters()
        last_action_message[bms] = f"{group.replace('_',' ').upper()} WRITE OK · {alarm/1000:.3f} / {protection/1000:.3f} / {release/1000:.3f} V · {delay/10:g} s"
    except Exception as exc:
        last_action_message[bms] = f"{group.upper()} WRITE FOUT: {exc}"
    return redirect(url_for("index"))

@app.route("/parameters/cell-uv",methods=["POST"])
def web_write_cell_uv():
    return write_uv_settings("cell_uv")

@app.route("/parameters/pack-uv",methods=["POST"])
def web_write_pack_uv():
    return write_uv_settings("pack_uv")


@app.route(
    "/parameters/cell-ov",
    methods=["POST"],
)
def web_write_cell_ov():

    try:

        alarm_mv = round(
            float(
                request.form[
                    "alarm_v"
                ]
            )
            * 1000
        )

        protection_mv = round(
            float(
                request.form[
                    "protection_v"
                ]
            )
            * 1000
        )

        release_mv = round(
            float(
                request.form[
                    "release_v"
                ]
            )
            * 1000
        )

        delay_100ms = round(
            float(
                request.form[
                    "delay_s"
                ]
            )
            * 10
        )

        if not all(
            2000 <= v <= 4500
            for v in [
                alarm_mv,
                protection_mv,
                release_mv,
            ]
        ):

            raise ValueError(
                "Cell OV spanning "
                "buiten scriptbereik."
            )

        if not (
            0
            <= delay_100ms
            <= 255
        ):

            raise ValueError(
                "Cell OV delay "
                "buiten bereik."
            )

        with serial_locks[current_bms()]:

            write_cell_ov(
                alarm_mv,
                protection_mv,
                release_mv,
                delay_100ms,
            )

            time.sleep(0.40)

            after = (
                read_parameters()
            )

        o = (
            after[
                "cell_ov"
            ]
        )

        if not (
            o["alarm_mv"]
            == alarm_mv
            and
            o["protection_mv"]
            == protection_mv
            and
            o["release_mv"]
            == release_mv
            and
            o["delay_100ms"]
            == delay_100ms
        ):

            raise RuntimeError(
                "Read-back komt "
                "niet overeen."
            )

        last_action_message[current_bms()] = (
            "Cell OV WRITE OK · "
            f"warn "
            f"{alarm_mv / 1000:.3f} V · "
            f"OVP "
            f"{protection_mv / 1000:.3f} V"
        )

    except Exception as exc:

        last_action_message[current_bms()] = (
            "Cell OV WRITE FOUT: "
            f"{exc}"
        )

    return redirect(
        url_for("index")
    )


@app.route(
    "/parameters/pack-ov",
    methods=["POST"],
)
def web_write_pack_ov():

    try:

        alarm_mv = round(
            float(
                request.form[
                    "alarm_v"
                ]
            )
            * 1000
        )

        protection_mv = round(
            float(
                request.form[
                    "protection_v"
                ]
            )
            * 1000
        )

        release_mv = round(
            float(
                request.form[
                    "release_v"
                ]
            )
            * 1000
        )

        delay_100ms = round(
            float(
                request.form[
                    "delay_s"
                ]
            )
            * 10
        )

        if not all(
            30000 <= v <= 65000
            for v in [
                alarm_mv,
                protection_mv,
                release_mv,
            ]
        ):

            raise ValueError(
                "Pack OV spanning "
                "buiten scriptbereik."
            )

        if not (
            0
            <= delay_100ms
            <= 255
        ):

            raise ValueError(
                "Pack OV delay "
                "buiten bereik."
            )

        with serial_locks[current_bms()]:

            write_pack_ov(
                alarm_mv,
                protection_mv,
                release_mv,
                delay_100ms,
            )

            time.sleep(0.40)

            after = (
                read_parameters()
            )

        o = (
            after[
                "pack_ov"
            ]
        )

        if not (
            o["alarm_mv"]
            == alarm_mv
            and
            o["protection_mv"]
            == protection_mv
            and
            o["release_mv"]
            == release_mv
            and
            o["delay_100ms"]
            == delay_100ms
        ):

            raise RuntimeError(
                "Read-back komt "
                "niet overeen."
            )

        last_action_message[current_bms()] = (
            "Pack OV WRITE OK · "
            f"warn "
            f"{alarm_mv / 1000:.3f} V · "
            f"OVP "
            f"{protection_mv / 1000:.3f} V"
        )

    except Exception as exc:

        last_action_message[current_bms()] = (
            "Pack OV WRITE FOUT: "
            f"{exc}"
        )

    return redirect(
        url_for("index")
    )


# ============================================================
# MQTT
# ============================================================

def setup_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id="pace-bmsrs232", protocol=mqtt.MQTTv5)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.will_set(GLOBAL_AVAILABILITY, payload="offline", qos=1, retain=True)
    def on_connect(client, userdata, flags, reason_code, properties):
        print(f"MQTT connect: {reason_code}", flush=True)
        if reason_code.is_failure:
            return
        client.publish(GLOBAL_AVAILABILITY, "online", qos=1, retain=True)
        for bms in BMS_ADDRESSES.values():
            publish_discovery(client, bms)
    def on_disconnect(client, userdata, flags, reason_code, properties):
        print(f"MQTT disconnected: {reason_code}", flush=True)
    def on_connect_fail(client, userdata):
        print("MQTT broker niet bereikbaar; nieuwe poging volgt", flush=True)
    client.on_disconnect = on_disconnect
    client.on_connect_fail = on_connect_fail
    client.on_connect = on_connect
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()
    return client


# Persistence uses the script directory, independent of systemd WorkingDirectory.
HISTORY_PATH = Path(os.environ.get('PACE_HISTORY_DB', str(Path(__file__).resolve().with_name('pace_history.sqlite3'))))
CHART_HISTORY_SECONDS = 7200
SAMPLE_RETENTION_SECONDS = 3 * 86400
EVENT_RETENTION_SECONDS = 30 * 86400
CAPACITY_RETENTION_SECONDS = 365 * 86400
EVENT_LIMIT = 2000
CAPACITY_EVENT_LIMIT = 3000
HISTORY_SCHEMA_VERSION = 2
# Derived values and raw bytes are not repeated in each five-second sample.
SAMPLE_FIELDS = (
    'bms', 'timestamp', 'soc', 'remaining_capacity_ah', 'full_capacity_ah',
    'cycle_count', 'current', 'voltage', 'cells', 'temperatures',
    'fully_charged', 'charge_mosfet', 'discharge_mosfet',
    'alarm', 'protection', 'fault', 'balancing_cells',
)
SAMPLE_JSON_FIELDS = ('cells', 'temperatures', 'balancing_cells')
SAMPLE_SQL_FIELDS = tuple('ts' if k == 'timestamp' else k for k in SAMPLE_FIELDS)
HISTORY_PRUNE_INTERVAL = 3600
history_last_prune = 0.0
HISTORY_INTERVAL = 5
history_lock = threading.RLock()
history_db = None
history_last = {}
history_error = None
stop_event = threading.Event()

def delta_threshold(address):
    bms = BMS_ADDRESSES.get(address)
    with cache_lock:
        params = parameter_cache.get(bms)
        if params and not last_parameter_error.get(bms):
            return params['balancing']['delta_mv'], f'{bms} parameter read-back'
    return 30, 'fallback; actuele parameter niet beschikbaar'


def compact_measurement(key, value):
    if value is None:
        return None
    if key in ('cells', 'temperatures'):
        return [round(float(v), 3 if key == 'cells' else 2) for v in value]
    if isinstance(value, float):
        return round(value, {'timestamp': 3, 'soc': 4, 'voltage': 3}.get(key, 2))
    return value


def prune_history(force=False):
    """Run inside history_lock and the caller's database transaction."""
    global history_last_prune
    clock = time.monotonic()
    if not force and clock - history_last_prune < HISTORY_PRUNE_INTERVAL:
        return
    cutoff = time.time()
    history_db.execute('DELETE FROM samples WHERE ts < ?', (cutoff-SAMPLE_RETENTION_SECONDS,))
    for capacity, days, limit in ((False, EVENT_RETENTION_SECONDS, EVENT_LIMIT),
                                  (True, CAPACITY_RETENTION_SECONDS, CAPACITY_EVENT_LIMIT)):
        condition = "kind IN ('capacity','capacity_baseline')" if capacity else "kind NOT IN ('capacity','capacity_baseline')"
        history_db.execute('DELETE FROM status_events WHERE '+condition+' AND ts < ?', (cutoff-days,))
        history_db.execute('DELETE FROM status_events WHERE '+condition+
            ' AND id IN (SELECT id FROM status_events WHERE '+condition+
            ' ORDER BY id DESC LIMIT -1 OFFSET ?)', (limit,))
    history_last_prune = clock


def compact_event_detail(kind, detail):
    result = dict(detail)
    for key in ('recent_parameters', 'decoded_after', 'note'):
        result.pop(key, None)
    if kind in ('status', 'baseline'):
        for side in ('before', 'after'):
            if result.get(side):
                result[side] = {k: v for k, v in result[side].items()
                                if k not in ('design_capacity_ah', 'soh', 'temperatures')}
    return result


def parameter_log_values(data):
    values = {k: dict(v) for k,v in data.items() if k != 'read_at'}
    if 'limiter_status' in values:
        values['limiter_status'] = {k: v for k,v in values['limiter_status'].items()
                                   if k in ('enabled', 'gear')}
    if 'protocols' in values:
        values['protocols'] = {k:v for k,v in values['protocols'].items()
                              if k in ('can_raw', 'rs485_raw', 'selection_raw')}
    return values


def init_history():
    global history_db
    with history_lock:
        history_db = sqlite3.connect(str(HISTORY_PATH), timeout=5, check_same_thread=False)
        history_db.execute('PRAGMA journal_mode=WAL')
        history_db.execute('PRAGMA synchronous=FULL')
        history_db.execute('CREATE TABLE IF NOT EXISTS samples (bms TEXT NOT NULL, ts REAL NOT NULL, soc REAL, current REAL, voltage REAL, PRIMARY KEY (bms, ts))')
        # Breid een bestaande database ter plaatse uit; de grafiekhistorie blijft behouden.
        sample_columns = {
            'remaining_capacity_ah': 'REAL', 'full_capacity_ah': 'REAL',
            'design_capacity_ah': 'REAL', 'soh': 'REAL', 'cycle_count': 'INTEGER',
            'power': 'REAL', 'min_v': 'REAL', 'max_v': 'REAL',
            'min_cell': 'INTEGER', 'max_cell': 'INTEGER', 'delta_mv': 'INTEGER',
            'cells': 'TEXT', 'temperatures': 'TEXT', 'charging': 'INTEGER',
            'fully_charged': 'INTEGER', 'charge_mosfet': 'TEXT',
            'discharge_mosfet': 'TEXT', 'alarm': 'TEXT', 'protection': 'TEXT',
            'fault': 'TEXT', 'balancing_cells': 'TEXT',
            'high_warning_cells': 'TEXT', 'protection1_raw': 'TEXT',
            'protection2_raw': 'TEXT', 'instructions_raw': 'TEXT',
            'control_raw': 'TEXT', 'balance1_raw': 'TEXT', 'balance2_raw': 'TEXT',
            'warning1_raw': 'TEXT', 'warning2_raw': 'TEXT', 'raw44': 'TEXT',
        }
        existing_columns = {
            row[1] for row in history_db.execute('PRAGMA table_info(samples)')
        }
        for name, sql_type in sample_columns.items():
            if name not in existing_columns:
                history_db.execute(f'ALTER TABLE samples ADD COLUMN {name} {sql_type}')
        history_db.execute('CREATE TABLE IF NOT EXISTS status_events (id INTEGER PRIMARY KEY,ts REAL,bms TEXT,kind TEXT,detail TEXT)')
        history_db.execute('CREATE TABLE IF NOT EXISTS status_baselines (bms TEXT PRIMARY KEY,raw TEXT,context TEXT)')
        history_db.execute('CREATE TABLE IF NOT EXISTS parameter_baseline (id INTEGER PRIMARY KEY,value TEXT)')
        history_db.execute('CREATE TABLE IF NOT EXISTS parameter_baselines (bms TEXT PRIMARY KEY,value TEXT)')
        history_db.execute("INSERT OR IGNORE INTO parameter_baselines SELECT 'BMS1',value FROM parameter_baseline WHERE id=1")
        history_db.execute('CREATE TABLE IF NOT EXISTS capacity_baselines (bms TEXT PRIMARY KEY,value TEXT)')
        history_db.execute('CREATE TABLE IF NOT EXISTS observed_bits (bms TEXT,offset INTEGER,bit INTEGER,value INTEGER,PRIMARY KEY(bms,offset,bit,value))')
        history_db.execute('CREATE INDEX IF NOT EXISTS samples_ts ON samples(ts)')
        history_db.execute('CREATE INDEX IF NOT EXISTS events_ts ON status_events(ts)')
        history_db.execute('CREATE TABLE IF NOT EXISTS log_pack_metadata (bms TEXT PRIMARY KEY,value TEXT)')
        # Preserve static pack data from the old schema before clearing duplicates.
        for bms in BMS_ADDRESSES.values():
            row = history_db.execute('SELECT design_capacity_ah FROM samples WHERE bms=? AND design_capacity_ah IS NOT NULL ORDER BY ts DESC LIMIT 1', (bms,)).fetchone()
            if row:
                history_db.execute('INSERT OR IGNORE INTO log_pack_metadata VALUES (?,?)',
                                   (bms, json.dumps({'design_capacity_ah': row[0]})))
        redundant = [k for k in sample_columns if k not in SAMPLE_FIELDS]
        history_db.execute('UPDATE samples SET '+','.join(k+'=NULL' for k in redundant)+
                           ' WHERE '+ ' OR '.join(k+' IS NOT NULL' for k in redundant))
        # One-time compaction of existing events; retain alarms/full/FCC and settings.
        rows = history_db.execute('SELECT id,kind,detail FROM status_events').fetchall()
        for event_id, kind, payload in rows:
            detail = json.loads(payload)
            changes = detail.get('changes', [])
            if kind == 'status' and changes and all(c.get('byte') in (34,35) for c in changes):
                history_db.execute('DELETE FROM status_events WHERE id=?', (event_id,))
                continue
            payload_new = json.dumps(compact_event_detail(kind, detail), ensure_ascii=False, separators=(',', ':'))
            if payload_new != payload:
                history_db.execute('UPDATE status_events SET detail=? WHERE id=?', (payload_new,event_id))
        prune_history(force=True)
        history_db.commit()
        # Reclaim substantial free space once on startup; live writes reuse pages.
        pages = history_db.execute('PRAGMA page_count').fetchone()[0]
        free = history_db.execute('PRAGMA freelist_count').fetchone()[0]
        if free > 2500 and free > pages * 0.25:
            history_db.execute('VACUUM')

def save_history(bms, snapshot):
    global history_error
    now = time.monotonic()
    if now - history_last.get(bms, -HISTORY_INTERVAL) < HISTORY_INTERVAL:
        return
    try:
        with history_lock:
            with history_db:
                values = []
                for key in SAMPLE_FIELDS:
                    value = bms if key == 'bms' else snapshot[key]
                    if key == 'fully_charged':
                        value = value == 'Ja' if isinstance(value, str) else bool(value)
                    value = compact_measurement(key, value)
                    if key in SAMPLE_JSON_FIELDS:
                        value = json.dumps(value, separators=(',', ':'))
                    values.append(value)
                history_db.execute('INSERT OR REPLACE INTO samples ('+
                    ','.join(SAMPLE_SQL_FIELDS)+') VALUES ('+','.join('?' for _ in values)+')', values)
                metadata = json.dumps({'design_capacity_ah': snapshot['design_capacity_ah']}, separators=(',', ':'))
                history_db.execute('INSERT INTO log_pack_metadata VALUES (?,?) ON CONFLICT(bms) DO UPDATE SET value=excluded.value WHERE value != excluded.value', (bms,metadata))
                prune_history()
        history_last[bms] = now
        history_error = None
    except Exception as exc:
        # Storage failure must not prevent live reads or MQTT publication.
        if history_error != str(exc):
            print(f'Historie opslaan mislukt: {exc}', flush=True)
        history_error = str(exc)

@app.route('/api/history')
def api_history():
    now = time.time()
    with history_lock:
        rows = history_db.execute('SELECT bms,ts,soc,current,voltage FROM samples WHERE ts >= ? AND ts <= ? ORDER BY ts',
                                 (now-CHART_HISTORY_SECONDS, now)).fetchall()
    packs = {bms: [] for bms in BMS_ADDRESSES.values()}
    for bms, ts, soc, current, voltage in rows:
        if bms in packs:
            packs[bms].append({'timestamp': ts, 'soc': soc, 'current': current, 'voltage': voltage})
    response = jsonify(packs=packs, now=now, error=history_error)
    response.headers['Cache-Control'] = 'no-store'
    return response

# Status journal: observations/correlations, never inferred causes.
event_error = None
previous_observations = {}

def event_insert(bms, kind, detail):
    history_db.execute('INSERT INTO status_events(ts,bms,kind,detail) VALUES (?,?,?,?)',
        (time.time(), bms, kind, json.dumps(compact_event_detail(kind, detail), ensure_ascii=False, separators=(',', ':'))))
    # Event caps are enforced immediately; age pruning is also run by live samples.
    condition = "kind IN ('capacity','capacity_baseline')" if kind in ('capacity','capacity_baseline') else "kind NOT IN ('capacity','capacity_baseline')"
    limit = CAPACITY_EVENT_LIMIT if kind in ('capacity','capacity_baseline') else EVENT_LIMIT
    history_db.execute('DELETE FROM status_events WHERE id IN (SELECT id FROM status_events WHERE '+condition+' ORDER BY id DESC LIMIT -1 OFFSET ?)', (limit,))
    prune_history()


def observation_context(snapshot):
    return {key: snapshot[key] for key in (
            'timestamp','soc','remaining_capacity_ah','full_capacity_ah',
            'design_capacity_ah','soh','cycle_count','current','voltage','delta_mv',
            'min_cell','max_cell','min_v','max_v','cells','charging',
            'fully_charged','temperatures','alarm','protection')}

def record_status_event(bms, status, snapshot):
    global event_error
    raw = list(status['raw_payload'])
    context = observation_context(snapshot)
    try:
        with history_lock:
            before = previous_observations.get(bms)
            if before is None:
                row = history_db.execute('SELECT raw,context FROM status_baselines WHERE bms=?', (bms,)).fetchone()
                if row:
                    before = {'raw': json.loads(row[0]), 'context': json.loads(row[1])}
            if before is None or raw != before['raw']:
                changes=[]
                old_raw=before['raw'] if before else []
                with history_db:
                    for offset in range(max(len(old_raw),len(raw))):
                        old=old_raw[offset] if offset<len(old_raw) else None
                        new=raw[offset] if offset<len(raw) else None
                        if old==new:
                            continue
                        bits=[]
                        if old is not None and new is not None:
                            for bit in range(8):
                                if (old ^ new) & (1<<bit):
                                    first = not history_db.execute('SELECT 1 FROM observed_bits WHERE bms=? AND offset=? AND bit=? AND value=?',
                                             (bms,offset,bit,(new>>bit)&1)).fetchone()
                                    bits.append({'bit':bit,'from':(old>>bit)&1,'to':(new>>bit)&1,'first_observed':first})
                        changes.append({'byte':offset,'from':old,'to':new,'bits':bits})
                    # Baseline bits count as already observed, not as newly activated bits.
                    for offset,value in enumerate(raw):
                        for bit in range(8):
                            history_db.execute('INSERT OR IGNORE INTO observed_bits VALUES (?,?,?,?)',(bms,offset,bit,(value>>bit)&1))
                    if before is None or any(c['byte'] not in (34,35) for c in changes):
                        event_insert(bms,'status' if before else 'baseline',{
                            'changes':changes,'raw_before':old_raw,'raw_after':raw,
                            'before':before['context'] if before else None,'after':context})
                    history_db.execute('INSERT OR REPLACE INTO status_baselines VALUES (?,?,?)',(bms,json.dumps(raw),json.dumps(context)))
            # Compare measurements with the immediately preceding successful poll.
            previous_observations[bms]={'raw':raw,'context':context}
        event_error=None
    except Exception as exc:
        if event_error!=str(exc):
            print(f'Statuslog fout: {exc}',flush=True)
        event_error=str(exc)

def record_capacity_event(bms, status, snapshot):
    # Run on EVERY valid poll, independent of raw status-byte changes.
    values = {"full_capacity_ah": snapshot["full_capacity_ah"],
              "cycle_count": snapshot["cycle_count"]}
    try:
        with history_lock:
            row = history_db.execute("SELECT value FROM capacity_baselines WHERE bms=?", (bms,)).fetchone()
            old = json.loads(row[0]) if row else None
            if old != values:
                changes = [{"parameter": key, "from": old.get(key), "to": value}
                           for key, value in values.items() if old and old.get(key) != value]
                with history_db:
                    event_insert(bms, "capacity" if old else "capacity_baseline", {
                        "changes": changes, "before": old, "after": values,
                        "context": observation_context(snapshot),
                        "raw_status": list(status["raw_payload"]),
                        "note": "FCC/cycli gewijzigd ten opzichte van vorige vastgelegde waarde. De oorzaak wordt niet door dit bericht uitgelezen."})
                    history_db.execute("INSERT OR REPLACE INTO capacity_baselines VALUES (?,?)", (bms,json.dumps(values)))
    except Exception as exc:
        print(f"{bms} FCC-log fout: {exc}", flush=True)


def record_parameter_event(data):
    global event_error
    values=parameter_log_values(data)
    try:
        with history_lock:
            row=history_db.execute('SELECT value FROM parameter_baselines WHERE bms=?', (current_bms(),)).fetchone()
            old=parameter_log_values(json.loads(row[0])) if row else None
            if old!=values:
                with history_db:
                    changes=[]
                    if old:
                        for group,fields in values.items():
                            for field,value in fields.items():
                                previous=old.get(group,{}).get(field)
                                if previous!=value:
                                    changes.append({'parameter':group+'.'+field,'from':previous,'to':value})
                    event_insert(current_bms(),'parameters' if old else 'parameter_baseline',{
                        'changes':changes,'before':old,'after':values,
                        'note':'Parameter read-back. Tijdstip van waarneming; daadwerkelijke wijziging kan eerder of buiten deze portal zijn gedaan.'})
                    history_db.execute('INSERT OR REPLACE INTO parameter_baselines VALUES (?,?)',(current_bms(),json.dumps(values)))
    except Exception as exc:
        event_error=str(exc)
        print(f'Parameterlog fout: {exc}',flush=True)

@app.route('/api/events')
def api_events():
    with history_lock:
        rows=history_db.execute('SELECT id,ts,bms,kind,detail FROM status_events ORDER BY id DESC LIMIT 100').fetchall()
    response=jsonify(events=[dict(id=r[0],timestamp=r[1],bms=r[2],kind=r[3],detail=json.loads(r[4])) for r in rows],error=event_error)
    response.headers['Cache-Control']='no-store'
    return response

EXPORT_TIMEZONE = ZoneInfo("Europe/Amsterdam")

def export_time_range(args, now):
    period = args.get("period", "all")
    local = datetime.fromtimestamp(now, EXPORT_TIMEZONE)
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "all":
        return period, None, None
    if period in ("1h", "6h", "12h", "24h"):
        return period, now-int(period[:-1])*3600, now
    if period == "today":
        return period, midnight.timestamp(), now
    if period == "yesterday":
        return period, (midnight-timedelta(days=1)).timestamp(), midnight.timestamp()
    if period != "custom":
        raise ValueError("Onbekende periode")
    def parse_local(value):
        try:
            naive = datetime.fromisoformat(value)
        except (ValueError, TypeError):
            raise ValueError("Vul een geldige begin- en eindtijd in")
        if naive.tzinfo is not None:
            raise ValueError("Gebruik lokale Nederlandse tijd zonder tijdzone-offset")
        # Reject nonexistent/ambiguous local times during DST changes.
        candidates = set()
        for fold in (0, 1):
            aware = naive.replace(tzinfo=EXPORT_TIMEZONE, fold=fold)
            ts = aware.timestamp()
            if datetime.fromtimestamp(ts, EXPORT_TIMEZONE).replace(tzinfo=None) == naive:
                candidates.add(ts)
        if len(candidates) != 1:
            raise ValueError("Deze tijd bestaat niet of is dubbel door zomer-/wintertijd; kies een tijd buiten dat omschakeluur")
        return candidates.pop()
    start, end = parse_local(args.get("start")), parse_local(args.get("end"))
    if start >= end:
        raise ValueError("De eindtijd moet na de begintijd liggen")
    return period, start, end

@app.route('/api/events/export')
def api_events_export():
    now = time.time()
    try:
        period, start, end = export_time_range(request.args, now)
    except ValueError as exc:
        return jsonify(error=str(exc)), 400
    clauses, values = [], []
    if start is not None:
        clauses.append("ts >= ?"); values.append(start)
    if end is not None:
        clauses.append("ts < ?"); values.append(end)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    sample_fields = SAMPLE_FIELDS
    sample_select = ','.join(SAMPLE_SQL_FIELDS)
    with history_lock:
        with history_db:
            prune_history()
        rows=history_db.execute('SELECT id,ts,bms,kind,detail FROM status_events'+where+' ORDER BY id', values).fetchall()
        pack_metadata = {b:json.loads(v) for b,v in history_db.execute('SELECT bms,value FROM log_pack_metadata')}
        parameters_at_export = {b:parameter_log_values(json.loads(v)) for b,v in history_db.execute('SELECT bms,value FROM parameter_baselines')}
        retained=history_db.execute('SELECT MIN(ts),MAX(ts),COUNT(*) FROM status_events').fetchone()
        sample_rows=history_db.execute(
            'SELECT '+sample_select+' FROM samples'+where+' ORDER BY ts,bms', values
        ).fetchall()
        samples_retained=history_db.execute(
            'SELECT MIN(ts),MAX(ts),COUNT(*) FROM samples'
        ).fetchone()

    samples=[]
    for row in sample_rows:
        sample=dict(zip(sample_fields,row))
        for field in SAMPLE_JSON_FIELDS:
            value=sample[field]
            sample[field]=json.loads(value) if value else []
        for field in ('fully_charged',):
            value=sample[field]
            sample[field]=bool(value) if value is not None else None
        samples.append({key:compact_measurement(key,value) for key,value in sample.items()})

    response=jsonify(
        events=[dict(id=r[0],timestamp=r[1],bms=r[2],kind=r[3],detail=json.loads(r[4])) for r in rows],
        samples=samples,
        pack_metadata=pack_metadata,
        parameters_at_export=parameters_at_export,
        export={"schema_version":HISTORY_SCHEMA_VERSION,"period":period,"timezone":"Europe/Amsterdam","generated_at":now,
                "start_inclusive":start,"end_exclusive":end,
                "count":len(rows),"event_count":len(rows),"sample_count":len(samples),
                "retained_first":retained[0],"retained_last":retained[1],"retained_count":retained[2],
                "samples_retained_first":samples_retained[0],
                "samples_retained_last":samples_retained[1],
                "samples_retained_count":samples_retained[2],
                "sample_interval_seconds":HISTORY_INTERVAL,
                "sample_retention_seconds":SAMPLE_RETENTION_SECONDS,
                "event_retention_seconds":EVENT_RETENTION_SECONDS,
                "capacity_retention_seconds":CAPACITY_RETENTION_SECONDS,
                "event_limit":EVENT_LIMIT,"capacity_event_limit":CAPACITY_EVENT_LIMIT,
                "sample_fields":list(SAMPLE_FIELDS),
                "notes":["Vermogen, celmin/max/delta en SOH zijn afleidbaar uit de metingen.",
                         "Ruwe statusbytes staan bij statuswijzigingen; balansstatus staat in de samples.",
                         "Parameters_at_export zijn laatst uitgelezen instellingen, niet noodzakelijk de instellingen gedurende de gekozen periode."]})
    stamp=datetime.fromtimestamp(now,EXPORT_TIMEZONE).strftime("%Y%m%d-%H%M%S")
    response.headers['Content-Disposition']=f'attachment; filename="pace-statuslog-{period}-{stamp}.json"'
    response.headers['Cache-Control']='no-store'
    return response


def parameter_refresh_loop(bms):
    io_context.bms = bms
    while not stop_event.is_set():
        try:
            read_parameters()
        except Exception as exc:
            print(f'{bms} parameter refresh: {exc}', flush=True)
        if stop_event.wait(60):
            break


# ============================================================
# MAIN
# ============================================================

def main():
    global mqtt_client
    def terminate(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, terminate)
    workers = []
    try:
        init_history()
        mqtt_client = setup_mqtt()
        for address, bms in BMS_ADDRESSES.items():
            for target, args in ((polling_loop, (address, bms)), (parameter_refresh_loop, (bms,))):
                worker = threading.Thread(target=target, args=args, daemon=True,
                                          name=f"{bms}-{target.__name__}")
                workers.append(worker)
                worker.start()
        import logging
        logging.getLogger('werkzeug').setLevel(logging.ERROR)
        print(f"PACE monitor · 3 eigen RS232-kabels · web :{WEB_PORT} · historie {HISTORY_PATH}", flush=True)
        app.run(host=WEB_HOST, port=WEB_PORT, debug=False, use_reloader=False, threaded=True)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        for worker in workers:
            worker.join(timeout=5)
        for bms, lock in serial_locks.items():
            with lock:
                connection = serial_connections[bms]
                if connection:
                    connection.close()
                serial_connections[bms] = None
        if mqtt_client:
            try:
                mqtt_client.publish(GLOBAL_AVAILABILITY, "offline", qos=1, retain=True).wait_for_publish(timeout=2)
            except Exception:
                pass
            mqtt_client.disconnect()
            mqtt_client.loop_stop()
        with history_lock:
            if history_db:
                history_db.close()


if __name__ == "__main__":
    main()
