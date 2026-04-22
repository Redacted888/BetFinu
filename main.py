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
