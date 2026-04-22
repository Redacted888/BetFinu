"""
BetFinu — an "AI finance terminal" style local platform for quoting, matching,
and settling bets, with a risk-aware ledger.

This is a single-file app that can run as:
- a terminal UI (curses) for the desk workflow
- an HTTP JSON API for browser UIs and automation
- an offline simulator with deterministic seeds

It uses only the Python standard library by default.
If `web3` is installed (requirements.txt includes it), it can also watch an EVM
contract for events and mirror them into the local ledger.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import dataclasses
import datetime as _dt
import enum
import hashlib
import hmac
import http.server
import io
import json
import logging
import os
import queue
import random
import secrets
import signal
import socket
import socketserver
import sqlite3
import statistics
import string
import sys
import threading
import time
import traceback
import typing as t
import urllib.parse
import uuid


APP_NAME = "BetFinu"
APP_VERSION = "0.8.4"
DB_SCHEMA_TAG = "betfinu.schema.2026-04-20.terminal.v3"


LOG = logging.getLogger("betfinu")


class Side(str, enum.Enum):
    BACK = "BACK"
    LAY = "LAY"


class MarketStatus(str, enum.Enum):
    OPEN = "OPEN"
    HALTED = "HALTED"
    SETTLED = "SETTLED"
    VOIDED = "VOIDED"


class OrderStatus(str, enum.Enum):
    LIVE = "LIVE"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"


class MatchStatus(str, enum.Enum):
    OPEN = "OPEN"
    CLAIMED_MAKER = "CLAIMED_MAKER"
    CLAIMED_TAKER = "CLAIMED_TAKER"
    CLAIMED_BOTH = "CLAIMED_BOTH"


@dataclasses.dataclass(frozen=True)
class RiskCaps:
    max_market_notional: float
    max_user_notional: float
    max_fee_bps: int
    max_rebate_bps: int
    exposure_decay_window_s: int


@dataclasses.dataclass(frozen=True)
class FeeSchedule:
    fee_bps: int
    maker_rebate_bps: int


@dataclasses.dataclass(frozen=True)
class MarketConfig:
    key: str
    label: str
    outcomes: int
    close_ts: int
    settle_deadline_ts: int
    min_stake: float
    max_stake: float
    max_orders_per_user: int
    allow_unmatched: bool
    fee: FeeSchedule


@dataclasses.dataclass(frozen=True)
class Order:
    order_id: str
    market_id: int
    maker: str
    side: Side
    outcome: int
    price_e4: int
    size: float
    remaining: float
    expiry_ts: int
    status: OrderStatus
    created_ts: int


@dataclasses.dataclass(frozen=True)
class Match:
    match_id: str
    market_id: int
    maker: str
    taker: str
    maker_side: Side
    outcome: int
    price_e4: int
    stake: float
    status: MatchStatus
    created_ts: int


@dataclasses.dataclass(frozen=True)
class Balance:
    user: str
    available: float
    locked: float
    pending_payout: float


def now_ts() -> int:
    return int(time.time())


def clamp_int(name: str, v: int, lo: int, hi: int) -> int:
    if not isinstance(v, int):
        raise ValueError(f"{name} must be int")
    if v < lo or v > hi:
        raise ValueError(f"{name} out of range {lo}..{hi}")
    return v


def clamp_float(name: str, v: float, lo: float, hi: float) -> float:
    if not isinstance(v, (int, float)):
        raise ValueError(f"{name} must be number")
    v2 = float(v)
    if v2 < lo or v2 > hi:
        raise ValueError(f"{name} out of range {lo}..{hi}")
    return v2


def bps_fee(amount: float, bps: int) -> float:
    return (amount * float(bps)) / 10_000.0


def odds_to_float(price_e4: int) -> float:
    return float(price_e4) / 10_000.0


def float_to_e4(x: float) -> int:
    if x <= 0:
        raise ValueError("odds must be >0")
    return int(round(x * 10_000.0))


def ulidish() -> str:
    # Not a ULID, but stable-length, sortable-ish, and low collision.
    ts = now_ts()
    rnd = secrets.token_bytes(10)
    return f"BF{ts:010d}{base64.b32encode(rnd).decode('ascii').rstrip('=').lower()}"


def stable_id(*parts: t.Any) -> str:
    h = hashlib.blake2s(digest_size=16)
    for p in parts:
        if isinstance(p, bytes):
            h.update(p)
        else:
            h.update(str(p).encode("utf-8"))
            h.update(b"\x1f")
    return h.hexdigest()


def json_dumps(obj: t.Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, sort_keys=True)

