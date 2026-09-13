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
                 4:'Omgeving te warm',5:'Omgeving te koud',6:'MOSFET te warm',7:'Low power'}

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
        'limiter_note':'Instruction bit0: kandidaat lokale stroombegrenzer; volgt CAN CCL-verlagingen niet. Control bits zijn raw/configuratie, geen numerieke CAN CCL.',
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
    # Do not equate a configuration flag with active current limiting.
    return (f"Lokale limiterkandidaat · instruction bit0={int(bool(s['instructions'] & 0x01))}"
            f" · control bit3={int(bool(s['control'] & 0x08))}"
            f" · control bit4={int(bool(s['control'] & 0x10))} · geen CAN CCL")



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

        "voltage":
            analog["voltage"],

        "current":
            analog["current"],

        "power":
            analog["power"],

        "full_capacity_ah":
            analog[
                "full_capacity_ah"
            ],

        "cycle_count":
            analog[
                "cycle_count"
            ],

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

<title>PACE BMS Monitor</title>


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
#event-log a{color:#56a4ff}</style>

</head>


<body>

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
                Vol
            </div>

            <div
                class="status-value"
                id="{{ key }}_full"
            >
                –
            </div>

        </div>


    </div>


    <details class="diagnostics"><summary>Diagnostiek</summary>
<p>CAN CCL: niet uitgelezen. De ruwe limiterbits hieronder geven geen numerieke laadstroomlimiet aan.</p>
<div id="{{ key }}_charge_limiter"></div>
<div class="info-line" id="{{ key }}_info"></div>
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


<!-- ======================================================
     PARAMETERS: EIGEN RS232 PER BMS
     ====================================================== -->

{% if true %}
{% set params = all_params.get(bms) %}
{% set param_error = all_errors.get(bms) %}
{% set message = all_messages.get(bms) %}


<div class="parameter-area">


<div class="parameter-heading">

    <div>

        <h2>
            Parameters
        </h2>

        <div class="muted">
            {{ bms }} / eigen RS232-kabel
        </div>

    </div>


    <form
        method="post"
        action="/parameters/read"
    >
<input type="hidden" name="bms" value="{{ bms }}">

        <button
            class="read"
            type="submit"
        >
            Opnieuw lezen
        </button>

    </form>

</div>


<details class="param-card" style="margin:8px 0"><summary>Temperatuurinstellingen</summary>
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
<button class="write" type="submit">Schrijven + check</button></form>
{% else %}<p>Nog niet gelezen; gebruik Opnieuw lezen.</p>{% endif %}
{% endfor %}</details>
<details class="param-card" style="margin:8px 0"><summary>Datum en tijd BMS</summary>
{% set ex = extra[bms] %}
<p>BMS-klok: {{ ex.clock or 'Nog niet gelezen' }}</p>
<p class="muted">Momentopname · gelezen {{ ex.get('clock_read_at','–') }}. Synchroniseren gebruikt Nederlandse tijd van de Pi.</p>
{% if ex.errors.get('clock') %}<p class="bad">{{ ex.errors.clock }}</p>{% endif %}
<form method="post" action="/parameters/clock">
<input type="hidden" name="bms" value="{{ bms }}"><input type="hidden" name="mode" value="manual">
<label>Datum en tijd<input type="datetime-local" name="clock" step="1" min="2000-01-01T00:00" max="2099-12-31T23:59:59" value="{{ ex.clock or '' }}" required></label>
<button class="write" type="submit">Schrijven + check</button></form>
<form method="post" action="/parameters/clock">
<input type="hidden" name="bms" value="{{ bms }}"><input type="hidden" name="mode" value="sync">
<button class="write" type="submit">Synchroniseer met Pi-tijd</button></form>
</details>

{% if param_error %}

<div class="bad">
    {{ param_error }}
</div>

{% endif %}


{% if params %}


<div
    class="muted"
    style="margin-bottom:5px"
>

    Laatst gelezen:
    {{ params.read_at }}

</div>


<div class="parameter-stack">


<!-- CELL OV -->

<div class="param-card">

<h3>
    Cell OV
</h3>

<form
    method="post"
    action="/parameters/cell-ov"
>
<input type="hidden" name="bms" value="{{ bms }}">

<label>
Waarschuwing (V)

<input
    name="alarm_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.cell_ov.alarm_mv / 1000) }}"
    required
>

</label>


<label>
OVP (V)

<input
    name="protection_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.cell_ov.protection_mv / 1000) }}"
    required
>

</label>


<label>
Release (V)

<input
    name="release_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.cell_ov.release_mv / 1000) }}"
    required
>

</label>


<label>
Delay (s)

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
    Schrijven + check
</button>

</form>

</div>


<!-- PACK OV -->

<div class="param-card">

<h3>
    Pack OV
</h3>

<form
    method="post"
    action="/parameters/pack-ov"
>
<input type="hidden" name="bms" value="{{ bms }}">

<label>
Waarschuwing (V)

<input
    name="alarm_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.pack_ov.alarm_mv / 1000) }}"
    required
>

</label>


<label>
OVP (V)

<input
    name="protection_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.pack_ov.protection_mv / 1000) }}"
    required
>

</label>


<label>
Release (V)

<input
    name="release_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.pack_ov.release_mv / 1000) }}"
    required
>

</label>


<label>
Delay (s)

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
    Schrijven + check
</button>

</form>

</div>


<!-- CELL UV -->

<div class="param-card">

<h3>
    Cell UV
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
UVP (V)

<input
    name="protection_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.cell_uv.protection_mv / 1000) }}"
    required
>

</label>


<label>
Release (V)

<input
    name="release_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.cell_uv.release_mv / 1000) }}"
    required
>

</label>


<label>
Delay (s)

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
    Schrijven + check
</button>

</form>

</div>


<!-- PACK UV -->

<div class="param-card">

<h3>
    Pack UV
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
UVP (V)

<input
    name="protection_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.pack_uv.protection_mv / 1000) }}"
    required
>

</label>


<label>
Release (V)

<input
    name="release_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.pack_uv.release_mv / 1000) }}"
    required
>

</label>


<label>
Delay (s)

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
    Schrijven + check
</button>

</form>

</div>


<!-- BALANCEREN -->

<div class="param-card">

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
    Schrijven + check
</button>

</form>

</div>


<!-- SLEEP -->

<div class="param-card">

<h3>
    Sleep
</h3>

<form
    method="post"
    action="/parameters/sleep"
>
<input type="hidden" name="bms" value="{{ bms }}">

<label>
Celspanning (V)

<input
    name="voltage_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.sleep.voltage_mv / 1000) }}"
    required
>

</label>


<label>
Delay (min)

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
    Schrijven + check
</button>

</form>

</div>


<!-- FULL CHARGE -->

<div class="param-card full">

<h3>
    Full Charge / SOC
</h3>

<form
    method="post"
    action="/parameters/full-charge"
>
<input type="hidden" name="bms" value="{{ bms }}">

<label>
Voltage (V)

<input
    name="voltage_v"
    type="number"
    step="0.001"
    value="{{ '%.3f'|format(params.full_charge.voltage_mv / 1000) }}"
    required
>

</label>


<label>
Current (mA)

<input
    name="current_ma"
    type="number"
    step="1"
    value="{{ params.full_charge.current_ma }}"
    required
>

</label>


<label>
Low SOC (%)

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
    Schrijven + check
</button>

</form>

</div>


</div>

{% endif %}


</div>


{% endif %}


</div>

{% endfor %}


</div>


<details id="event-log"><summary>Statuswijzigingen · bytes &amp; bits</summary>
<p>Waarnemingen, geen bewezen oorzaken. Byte-index vanaf 0. Eerste waarneming geldt sinds dit log bestaat.</p>
<form action="/api/events/export" method="get" style="display:flex;flex-wrap:wrap;align-items:end;gap:10px">
<label>Periode<br><select name="period" id="export-period" style="padding:8px;background:#12151b;color:#e5e7eb;border:1px solid #555;border-radius:5px">
<option value="all">Alles wat bewaard is</option>
<option value="1h">Afgelopen 1 uur</option><option value="6h">Afgelopen 6 uur</option><option value="12h">Afgelopen 12 uur</option>
<option value="24h">Afgelopen 24 uur</option><option value="today">Vandaag</option>
<option value="yesterday">Gisteren</option><option value="custom">Zelf kiezen</option>
</select></label>
<span id="export-custom" hidden>
<label>Vanaf <input type="datetime-local" name="start" disabled></label>
<label>Tot (exclusief) <input type="datetime-local" name="end" disabled></label>
</span>
<button class="read" type="submit">Download JSON</button>
</form>
<p class="muted">Nederlandse tijd · alleen bewaarde gebeurtenissen · maximaal 7 dagen / 10.000 gebeurtenissen. Een lege periode levert een leeg logbestand op.</p>
<p id="event-log-state">Log laden…</p><div id="event-list"></div></details></main>


<script>

(function () {

"use strict";
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

        "FCC "
        + fmt(
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
        document.getElementById('event-log-state').textContent=data.error?'Logfout: '+data.error:'Laatste '+data.events.length+' gebeurtenissen · volledige export tot 10.000 / 7 dagen';
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
    if MQTT_USER:
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
HISTORY_SECONDS = 7200
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


def init_history():
    global history_db
    with history_lock:
        history_db = sqlite3.connect(str(HISTORY_PATH), timeout=5, check_same_thread=False)
        history_db.execute('PRAGMA journal_mode=WAL')
        history_db.execute('PRAGMA synchronous=FULL')
        history_db.execute('CREATE TABLE IF NOT EXISTS samples (bms TEXT NOT NULL, ts REAL NOT NULL, soc REAL, current REAL, voltage REAL, PRIMARY KEY (bms, ts))')
        history_db.execute('CREATE TABLE IF NOT EXISTS status_events (id INTEGER PRIMARY KEY,ts REAL,bms TEXT,kind TEXT,detail TEXT)')
        history_db.execute('CREATE TABLE IF NOT EXISTS status_baselines (bms TEXT PRIMARY KEY,raw TEXT,context TEXT)')
        history_db.execute('CREATE TABLE IF NOT EXISTS parameter_baseline (id INTEGER PRIMARY KEY,value TEXT)')
        history_db.execute('CREATE TABLE IF NOT EXISTS parameter_baselines (bms TEXT PRIMARY KEY,value TEXT)')
        history_db.execute("INSERT OR IGNORE INTO parameter_baselines SELECT 'BMS1',value FROM parameter_baseline WHERE id=1")
        history_db.execute('CREATE TABLE IF NOT EXISTS capacity_baselines (bms TEXT PRIMARY KEY,value TEXT)')
        history_db.execute('CREATE TABLE IF NOT EXISTS observed_bits (bms TEXT,offset INTEGER,bit INTEGER,value INTEGER,PRIMARY KEY(bms,offset,bit,value))')
        history_db.execute('DELETE FROM samples WHERE ts < ?', (time.time()-HISTORY_SECONDS,))
        history_db.commit()

def save_history(bms, snapshot):
    global history_error
    now = time.monotonic()
    if now - history_last.get(bms, -HISTORY_INTERVAL) < HISTORY_INTERVAL:
        return
    try:
        with history_lock:
            with history_db:
                history_db.execute('INSERT OR REPLACE INTO samples VALUES (?,?,?,?,?)',
                    (bms, snapshot['timestamp'], snapshot['soc'], snapshot['current'], snapshot['voltage']))
                history_db.execute('DELETE FROM samples WHERE ts < ?', (time.time()-HISTORY_SECONDS,))
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
                                 (now-HISTORY_SECONDS, now)).fetchall()
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
EVENT_LIMIT = 10000

def event_insert(bms, kind, detail):
    history_db.execute('INSERT INTO status_events(ts,bms,kind,detail) VALUES (?,?,?,?)',
                       (time.time(), bms, kind, json.dumps(detail, ensure_ascii=False)))
    history_db.execute('DELETE FROM status_events WHERE id <= (SELECT id FROM status_events ORDER BY id DESC LIMIT 1 OFFSET ?)', (EVENT_LIMIT,))
    history_db.execute('DELETE FROM status_events WHERE ts < ?', (time.time()-7*86400,))

def observation_context(snapshot):
    return {key: snapshot[key] for key in ('timestamp','soc','current','voltage','delta_mv','min_cell','max_cell',
            'min_v','max_v','cells','charging','full_capacity_ah','temperatures','alarm','protection')}

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
                    recent=history_db.execute("SELECT ts,detail FROM status_events WHERE kind='parameters' AND bms=? AND ts>=? ORDER BY id DESC LIMIT 1", (bms,time.time()-120,)).fetchone()
                    event_insert(bms,'status' if before else 'baseline',{
                        'changes':changes,'raw_before':old_raw,'raw_after':raw,
                        'before':before['context'] if before else None,'after':context,
                        'recent_parameters':{'timestamp':recent[0],'detail':json.loads(recent[1])} if recent else None,
                        'decoded_after':decoded_status(status),
                        'note':'Gelijktijdige waarnemingen; oorzaak niet vastgesteld. Byte-index vanaf 0. Eerste waarneming geldt sinds dit log bestaat.'})
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
                        "note": "FCC/cycli gewijzigd ten opzichte van vorige vastgelegde waarde. Geen bewijs van de oorzaak of een automatische recount."})
                    history_db.execute("INSERT OR REPLACE INTO capacity_baselines VALUES (?,?)", (bms,json.dumps(values)))
    except Exception as exc:
        print(f"{bms} FCC-log fout: {exc}", flush=True)


def record_parameter_event(data):
    global event_error
    values={key:value for key,value in data.items() if key!='read_at'}
    try:
        with history_lock:
            row=history_db.execute('SELECT value FROM parameter_baselines WHERE bms=?', (current_bms(),)).fetchone()
            old=json.loads(row[0]) if row else None
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
    with history_lock:
        rows=history_db.execute('SELECT id,ts,bms,kind,detail FROM status_events'+where+' ORDER BY id', values).fetchall()
        retained=history_db.execute('SELECT MIN(ts),MAX(ts),COUNT(*) FROM status_events').fetchone()
    response=jsonify(
        events=[dict(id=r[0],timestamp=r[1],bms=r[2],kind=r[3],detail=json.loads(r[4])) for r in rows],
        export={"period":period,"timezone":"Europe/Amsterdam","generated_at":now,
                "start_inclusive":start,"end_exclusive":end,"count":len(rows),
                "retained_first":retained[0],"retained_last":retained[1],"retained_count":retained[2]})
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
