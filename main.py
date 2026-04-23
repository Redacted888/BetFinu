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


def parse_user(user: str) -> str:
    u = (user or "").strip()
    if not u:
        raise ValueError("user required")
    if len(u) > 64:
        raise ValueError("user too long")
    return u


def parse_market_key(k: str) -> str:
    k = (k or "").strip()
    if not k:
        raise ValueError("market key required")
    if len(k) > 96:
        raise ValueError("market key too long")
    return k


def parse_label(s: str) -> str:
    s = (s or "").strip()
    if not s:
        raise ValueError("label required")
    if len(s) > 160:
        raise ValueError("label too long")
    return s


def parse_outcome(outcome: int, outcomes: int) -> int:
    outcome = int(outcome)
    if outcome < 1 or outcome > outcomes:
        raise ValueError("bad outcome")
    return outcome


def parse_side(side: str) -> Side:
    try:
        return Side(side.upper())
    except Exception as e:
        raise ValueError("bad side") from e


def parse_price_e4(price_e4: int) -> int:
    price_e4 = int(price_e4)
    if price_e4 < 1_000 or price_e4 > 99_900:
        raise ValueError("bad price_e4")
    return price_e4


def parse_ts(ts: int) -> int:
    ts = int(ts)
    if ts < 1:
        raise ValueError("bad timestamp")
    return ts


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def utc_iso(ts: int) -> str:
    return _dt.datetime.utcfromtimestamp(ts).replace(tzinfo=_dt.timezone.utc).isoformat()


class DB:
    def __init__(self, path: str):
        self.path = path
        self._local = threading.local()

    def conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL;")
            c.execute("PRAGMA synchronous=NORMAL;")
            c.execute("PRAGMA foreign_keys=ON;")
            self._local.conn = c
        return c

    @contextlib.contextmanager
    def tx(self):
        c = self.conn()
        c.execute("BEGIN;")
        try:
            yield c
            c.execute("COMMIT;")
        except Exception:
            c.execute("ROLLBACK;")
            raise

    def init(self) -> None:
        c = self.conn()
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta(
              k TEXT PRIMARY KEY,
              v TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS users(
              user TEXT PRIMARY KEY,
              created_ts INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS balances(
              user TEXT PRIMARY KEY,
              available REAL NOT NULL,
              locked REAL NOT NULL,
              pending_payout REAL NOT NULL,
              updated_ts INTEGER NOT NULL,
              FOREIGN KEY(user) REFERENCES users(user) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS markets(
              market_id INTEGER PRIMARY KEY AUTOINCREMENT,
              key TEXT NOT NULL UNIQUE,
              label TEXT NOT NULL,
              outcomes INTEGER NOT NULL,
              close_ts INTEGER NOT NULL,
              settle_deadline_ts INTEGER NOT NULL,
              min_stake REAL NOT NULL,
              max_stake REAL NOT NULL,
              max_orders_per_user INTEGER NOT NULL,
              allow_unmatched INTEGER NOT NULL,
              fee_bps INTEGER NOT NULL,
              maker_rebate_bps INTEGER NOT NULL,
              status TEXT NOT NULL,
              settled_outcome INTEGER,
              created_ts INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS orders(
              order_id TEXT PRIMARY KEY,
              market_id INTEGER NOT NULL,
              maker TEXT NOT NULL,
              side TEXT NOT NULL,
              outcome INTEGER NOT NULL,
              price_e4 INTEGER NOT NULL,
              size REAL NOT NULL,
              remaining REAL NOT NULL,
              expiry_ts INTEGER NOT NULL,
              status TEXT NOT NULL,
              created_ts INTEGER NOT NULL,
              FOREIGN KEY(market_id) REFERENCES markets(market_id) ON DELETE CASCADE,
              FOREIGN KEY(maker) REFERENCES users(user) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_orders_market ON orders(market_id);
            CREATE INDEX IF NOT EXISTS idx_orders_maker ON orders(maker);
            CREATE TABLE IF NOT EXISTS matches(
              match_id TEXT PRIMARY KEY,
              market_id INTEGER NOT NULL,
              maker TEXT NOT NULL,
              taker TEXT NOT NULL,
              maker_side TEXT NOT NULL,
              outcome INTEGER NOT NULL,
              price_e4 INTEGER NOT NULL,
              stake REAL NOT NULL,
              status TEXT NOT NULL,
              created_ts INTEGER NOT NULL,
              FOREIGN KEY(market_id) REFERENCES markets(market_id) ON DELETE CASCADE,
              FOREIGN KEY(maker) REFERENCES users(user) ON DELETE CASCADE,
              FOREIGN KEY(taker) REFERENCES users(user) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_matches_market ON matches(market_id);
            CREATE INDEX IF NOT EXISTS idx_matches_maker ON matches(maker);
            CREATE INDEX IF NOT EXISTS idx_matches_taker ON matches(taker);
            CREATE TABLE IF NOT EXISTS ledger(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts INTEGER NOT NULL,
              user TEXT NOT NULL,
              kind TEXT NOT NULL,
              ref TEXT,
              delta_available REAL NOT NULL,
              delta_locked REAL NOT NULL,
              delta_pending REAL NOT NULL,
              note TEXT NOT NULL,
              FOREIGN KEY(user) REFERENCES users(user) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_ledger_user_ts ON ledger(user, ts);
            """
        )
        c.execute("INSERT OR IGNORE INTO meta(k,v) VALUES(?,?)", ("schema", DB_SCHEMA_TAG))
        c.execute("INSERT OR IGNORE INTO meta(k,v) VALUES(?,?)", ("created_ts", str(now_ts())))


class RiskEngine:
    def __init__(self, caps: RiskCaps):
        self.caps = caps
        self.user_notional: dict[str, float] = {}
        self.market_notional: dict[int, float] = {}
        self.last_touch: dict[str, int] = {}

    def touch(self, user: str) -> None:
        ts = now_ts()
        prev = self.last_touch.get(user, 0)
        if prev and ts - prev > self.caps.exposure_decay_window_s:
            self.user_notional[user] = self.user_notional.get(user, 0.0) * 0.5
        self.last_touch[user] = ts

    def add_notional(self, market_id: int, maker: str, taker: str, notional: float) -> None:
        self.touch(maker)
        self.touch(taker)
        mnext = self.market_notional.get(market_id, 0.0) + notional
        if mnext > self.caps.max_market_notional:
            raise ValueError("risk: market cap exceeded")
        for u in (maker, taker):
            unext = self.user_notional.get(u, 0.0) + notional
            if unext > self.caps.max_user_notional:
                raise ValueError(f"risk: user cap exceeded: {u}")
            self.user_notional[u] = unext
        self.market_notional[market_id] = mnext

    @staticmethod
    def notional(price_e4: int, stake: float) -> float:
        return stake * odds_to_float(price_e4)


class LedgerService:
    def __init__(self, db: DB, risk: RiskEngine):
        self.db = db
        self.risk = risk

    def _ensure_user(self, c: sqlite3.Connection, user: str) -> None:
        user = parse_user(user)
        c.execute("INSERT OR IGNORE INTO users(user, created_ts) VALUES(?,?)", (user, now_ts()))
        c.execute(
            "INSERT OR IGNORE INTO balances(user, available, locked, pending_payout, updated_ts) VALUES(?,?,?,?,?)",
            (user, 0.0, 0.0, 0.0, now_ts()),
        )

    def get_balance(self, user: str) -> Balance:
        user = parse_user(user)
        c = self.db.conn()
        self._ensure_user(c, user)
        row = c.execute("SELECT user, available, locked, pending_payout FROM balances WHERE user=?", (user,)).fetchone()
        assert row is not None
        return Balance(user=row["user"], available=row["available"], locked=row["locked"], pending_payout=row["pending_payout"])

    def _post_ledger(
        self, c: sqlite3.Connection, user: str, kind: str, ref: str | None, da: float, dl: float, dp: float, note: str
    ) -> None:
        c.execute(
            "INSERT INTO ledger(ts,user,kind,ref,delta_available,delta_locked,delta_pending,note) VALUES(?,?,?,?,?,?,?,?)",
            (now_ts(), user, kind, ref, da, dl, dp, note),
        )

    def _apply_balance(self, c: sqlite3.Connection, user: str, da: float, dl: float, dp: float) -> Balance:
        row = c.execute("SELECT available, locked, pending_payout FROM balances WHERE user=?", (user,)).fetchone()
        if row is None:
            raise ValueError("missing user")
        a, l, p = float(row["available"]), float(row["locked"]), float(row["pending_payout"])
        na, nl, np = a + da, l + dl, p + dp
        if na < -1e-9 or nl < -1e-9 or np < -1e-9:
            raise ValueError("insufficient balance")
        c.execute(
            "UPDATE balances SET available=?, locked=?, pending_payout=?, updated_ts=? WHERE user=?",
            (na, nl, np, now_ts(), user),
        )
        return Balance(user=user, available=na, locked=nl, pending_payout=np)

    def deposit(self, user: str, amount: float, note: str = "deposit") -> Balance:
        user = parse_user(user)
        amount = clamp_float("amount", amount, 0.0000001, 1e18)
        c = self.db.conn()
        with self.db.tx() as tx:
            self._ensure_user(tx, user)
            b = self._apply_balance(tx, user, da=amount, dl=0.0, dp=0.0)
            self._post_ledger(tx, user, "DEPOSIT", None, amount, 0.0, 0.0, note)
            return b

    def queue_withdraw(self, user: str, amount: float, note: str = "queue_withdraw") -> Balance:
        user = parse_user(user)
        amount = clamp_float("amount", amount, 0.0000001, 1e18)
        c = self.db.conn()
        with self.db.tx() as tx:
            self._ensure_user(tx, user)
            b = self._apply_balance(tx, user, da=-amount, dl=0.0, dp=amount)
            self._post_ledger(tx, user, "QUEUE_WITHDRAW", None, -amount, 0.0, amount, note)
            return b

    def withdraw(self, user: str, note: str = "withdraw") -> Balance:
        user = parse_user(user)
        c = self.db.conn()
        with self.db.tx() as tx:
            self._ensure_user(tx, user)
            row = tx.execute("SELECT pending_payout FROM balances WHERE user=?", (user,)).fetchone()
            amt = float(row["pending_payout"]) if row else 0.0
            if amt <= 0:
                raise ValueError("nothing to withdraw")
            b = self._apply_balance(tx, user, da=0.0, dl=0.0, dp=-amt)
            self._post_ledger(tx, user, "WITHDRAW", None, 0.0, 0.0, -amt, note)
            return b

    def create_market(self, cfg: MarketConfig) -> int:
        c = self.db.conn()
        key = parse_market_key(cfg.key)
        label = parse_label(cfg.label)
        outcomes = clamp_int("outcomes", cfg.outcomes, 2, 8)
        close_ts = parse_ts(cfg.close_ts)
        settle_deadline_ts = parse_ts(cfg.settle_deadline_ts)
        if close_ts <= now_ts():
            raise ValueError("close_ts must be in the future")
        if settle_deadline_ts <= close_ts:
            raise ValueError("settle_deadline_ts must be > close_ts")
        min_stake = clamp_float("min_stake", cfg.min_stake, 0.0000001, 1e18)
        max_stake = clamp_float("max_stake", cfg.max_stake, min_stake, 1e18)
        max_orders = clamp_int("max_orders_per_user", cfg.max_orders_per_user, 1, 10_000)
        fee_bps = clamp_int("fee_bps", cfg.fee.fee_bps, 0, self.risk.caps.max_fee_bps)
        rebate_bps = clamp_int("maker_rebate_bps", cfg.fee.maker_rebate_bps, 0, self.risk.caps.max_rebate_bps)
        if rebate_bps > fee_bps:
            raise ValueError("maker_rebate_bps must be <= fee_bps")
        with self.db.tx() as tx:
            tx.execute(
                """
                INSERT INTO markets(
                  key,label,outcomes,close_ts,settle_deadline_ts,
                  min_stake,max_stake,max_orders_per_user,allow_unmatched,
                  fee_bps,maker_rebate_bps,status,settled_outcome,created_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    key,
                    label,
                    outcomes,
                    close_ts,
                    settle_deadline_ts,
                    min_stake,
                    max_stake,
                    max_orders,
                    1 if cfg.allow_unmatched else 0,
                    fee_bps,
                    rebate_bps,
                    MarketStatus.OPEN.value,
                    None,
                    now_ts(),
                ),
            )
            mid = int(tx.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
            return mid

    def list_markets(self) -> list[dict[str, t.Any]]:
        c = self.db.conn()
        rows = c.execute(
            "SELECT market_id,key,label,outcomes,close_ts,settle_deadline_ts,min_stake,max_stake,fee_bps,maker_rebate_bps,status,settled_outcome FROM markets ORDER BY market_id DESC"
        ).fetchall()
        out = []
        for r in rows:
            out.append(
                {
                    "market_id": int(r["market_id"]),
                    "key": r["key"],
                    "label": r["label"],
                    "outcomes": int(r["outcomes"]),
                    "close_ts": int(r["close_ts"]),
                    "settle_deadline_ts": int(r["settle_deadline_ts"]),
                    "min_stake": float(r["min_stake"]),
                    "max_stake": float(r["max_stake"]),
                    "fee_bps": int(r["fee_bps"]),
                    "maker_rebate_bps": int(r["maker_rebate_bps"]),
                    "status": r["status"],
                    "settled_outcome": int(r["settled_outcome"]) if r["settled_outcome"] is not None else None,
                }
            )
        return out

    def get_market(self, market_id: int) -> dict[str, t.Any]:
        c = self.db.conn()
        r = c.execute(
            "SELECT market_id,key,label,outcomes,close_ts,settle_deadline_ts,min_stake,max_stake,max_orders_per_user,allow_unmatched,fee_bps,maker_rebate_bps,status,settled_outcome FROM markets WHERE market_id=?",
            (int(market_id),),
        ).fetchone()
        if not r:
            raise ValueError("market not found")
        return {
            "market_id": int(r["market_id"]),
            "key": r["key"],
            "label": r["label"],
            "outcomes": int(r["outcomes"]),
            "close_ts": int(r["close_ts"]),
            "settle_deadline_ts": int(r["settle_deadline_ts"]),
            "min_stake": float(r["min_stake"]),
            "max_stake": float(r["max_stake"]),
            "max_orders_per_user": int(r["max_orders_per_user"]),
            "allow_unmatched": bool(int(r["allow_unmatched"])),
            "fee_bps": int(r["fee_bps"]),
            "maker_rebate_bps": int(r["maker_rebate_bps"]),
            "status": r["status"],
            "settled_outcome": int(r["settled_outcome"]) if r["settled_outcome"] is not None else None,
        }

    def _market_cfg_row(self, c: sqlite3.Connection, market_id: int) -> tuple[sqlite3.Row, MarketConfig]:
        r = c.execute("SELECT * FROM markets WHERE market_id=?", (int(market_id),)).fetchone()
        if not r:
            raise ValueError("market not found")
        cfg = MarketConfig(
            key=r["key"],
            label=r["label"],
            outcomes=int(r["outcomes"]),
            close_ts=int(r["close_ts"]),
            settle_deadline_ts=int(r["settle_deadline_ts"]),
            min_stake=float(r["min_stake"]),
            max_stake=float(r["max_stake"]),
            max_orders_per_user=int(r["max_orders_per_user"]),
            allow_unmatched=bool(int(r["allow_unmatched"])),
            fee=FeeSchedule(fee_bps=int(r["fee_bps"]), maker_rebate_bps=int(r["maker_rebate_bps"])),
        )
        return r, cfg

    def post_order(
        self,
        market_id: int,
        maker: str,
        side: Side,
        outcome: int,
        price_e4: int,
        size: float,
        expiry_ts: int,
    ) -> Order:
        maker = parse_user(maker)
        price_e4 = parse_price_e4(price_e4)
        size = float(clamp_float("size", size, 0.0000001, 1e18))
        expiry_ts = parse_ts(expiry_ts)
        c = self.db.conn()
        with self.db.tx() as tx:
            self._ensure_user(tx, maker)
            mrow, cfg = self._market_cfg_row(tx, market_id)
            if mrow["status"] != MarketStatus.OPEN.value:
                raise ValueError("market not open")
            if now_ts() >= cfg.close_ts:
                raise ValueError("market closed")
            outcome = parse_outcome(outcome, cfg.outcomes)
            if expiry_ts <= now_ts():
                raise ValueError("order expired")
            if size < cfg.min_stake or size > cfg.max_stake:
                raise ValueError("size out of bounds")
            live = int(
                tx.execute(
                    "SELECT COUNT(1) AS n FROM orders WHERE market_id=? AND maker=? AND status IN (?,?,?)",
                    (int(market_id), maker, OrderStatus.LIVE.value, OrderStatus.PARTIAL.value, OrderStatus.EXPIRED.value),
                ).fetchone()["n"]
            )
            if live >= cfg.max_orders_per_user:
                raise ValueError("too many open orders")
            lock = self._lock_for(side, price_e4, size)
            self._apply_balance(tx, maker, da=-lock, dl=lock, dp=0.0)
            oid = ulidish()
            created = now_ts()
            tx.execute(
                """
                INSERT INTO orders(order_id,market_id,maker,side,outcome,price_e4,size,remaining,expiry_ts,status,created_ts)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    oid,
                    int(market_id),
                    maker,
                    side.value,
                    int(outcome),
                    int(price_e4),
                    float(size),
                    float(size),
                    int(expiry_ts),
                    OrderStatus.LIVE.value,
                    created,
                ),
            )
            self._post_ledger(tx, maker, "LOCK", oid, da=-lock, dl=lock, dp=0.0, note="order lock")
            return Order(
                order_id=oid,
                market_id=int(market_id),
                maker=maker,
                side=side,
                outcome=int(outcome),
                price_e4=int(price_e4),
                size=float(size),
                remaining=float(size),
                expiry_ts=int(expiry_ts),
                status=OrderStatus.LIVE,
                created_ts=int(created),
            )

    def cancel_order(self, order_id: str, by: str) -> Order:
        by = parse_user(by)
        c = self.db.conn()
        with self.db.tx() as tx:
            r = tx.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()
            if not r:
                raise ValueError("order not found")
            if r["maker"] != by:
                raise ValueError("not maker")
            st = r["status"]
            if st in (OrderStatus.CANCELED.value, OrderStatus.FILLED.value):
                raise ValueError("already final")
            remaining = float(r["remaining"])
            side = Side(r["side"])
            lock = self._lock_for(side, int(r["price_e4"]), remaining)
            tx.execute("UPDATE orders SET status=?, remaining=? WHERE order_id=?", (OrderStatus.CANCELED.value, 0.0, order_id))
            self._apply_balance(tx, by, da=lock, dl=-lock, dp=0.0)
            self._post_ledger(tx, by, "UNLOCK", order_id, da=lock, dl=-lock, dp=0.0, note="order cancel unlock")
            return self.get_order(order_id)

    def get_order(self, order_id: str) -> Order:
        c = self.db.conn()
        r = c.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()
        if not r:
            raise ValueError("order not found")
        return Order(
            order_id=r["order_id"],
            market_id=int(r["market_id"]),
            maker=r["maker"],
            side=Side(r["side"]),
            outcome=int(r["outcome"]),
            price_e4=int(r["price_e4"]),
            size=float(r["size"]),
            remaining=float(r["remaining"]),
            expiry_ts=int(r["expiry_ts"]),
            status=OrderStatus(r["status"]),
            created_ts=int(r["created_ts"]),
        )

    def list_orders(self, market_id: int | None = None, user: str | None = None) -> list[dict[str, t.Any]]:
        c = self.db.conn()
        q = "SELECT * FROM orders"
        args: list[t.Any] = []
        cond: list[str] = []
        if market_id is not None:
            cond.append("market_id=?")
            args.append(int(market_id))
        if user is not None:
            cond.append("maker=?")
            args.append(parse_user(user))
        if cond:
            q += " WHERE " + " AND ".join(cond)
        q += " ORDER BY created_ts DESC LIMIT 500"
        rows = c.execute(q, args).fetchall()
        out: list[dict[str, t.Any]] = []
        for r in rows:
            out.append(
                {
                    "order_id": r["order_id"],
                    "market_id": int(r["market_id"]),
                    "maker": r["maker"],
                    "side": r["side"],
                    "outcome": int(r["outcome"]),
                    "price_e4": int(r["price_e4"]),
                    "size": float(r["size"]),
                    "remaining": float(r["remaining"]),
                    "expiry_ts": int(r["expiry_ts"]),
                    "status": r["status"],
                    "created_ts": int(r["created_ts"]),
                }
            )
        return out

    def take_order(self, order_id: str, taker: str, stake: float) -> Match:
        taker = parse_user(taker)
        stake = float(clamp_float("stake", stake, 0.0000001, 1e18))
        c = self.db.conn()
        with self.db.tx() as tx:
            self._ensure_user(tx, taker)
            r = tx.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()
            if not r:
                raise ValueError("order not found")
            if r["maker"] == taker:
                raise ValueError("self-take not allowed")
            mrow, cfg = self._market_cfg_row(tx, int(r["market_id"]))
            if mrow["status"] != MarketStatus.OPEN.value:
                raise ValueError("market not open")
            if now_ts() >= cfg.close_ts:
                raise ValueError("market closed")
            if int(r["expiry_ts"]) <= now_ts():
                tx.execute("UPDATE orders SET status=? WHERE order_id=?", (OrderStatus.EXPIRED.value, order_id))
                raise ValueError("order expired")
            remaining = float(r["remaining"])
            if remaining <= 0:
                raise ValueError("no liquidity")
            fill = min(stake, remaining)
            if fill < cfg.min_stake:
                raise ValueError("fill too small")
            if fill > cfg.max_stake:
                raise ValueError("fill too large")
            side = Side(r["side"])
            maker_side = side
            taker_side = Side.LAY if maker_side == Side.BACK else Side.BACK
            price_e4 = int(r["price_e4"])
            lock = self._lock_for(taker_side, price_e4, fill)
            self._apply_balance(tx, taker, da=-lock, dl=lock, dp=0.0)
            self._post_ledger(tx, taker, "LOCK", order_id, da=-lock, dl=lock, dp=0.0, note="take lock")
            # Risk accounting
            notional = self.risk.notional(price_e4, fill)
            self.risk.add_notional(int(r["market_id"]), maker=r["maker"], taker=taker, notional=notional)
            # Update order remaining
            nrem = remaining - fill
            nst = OrderStatus.PARTIAL.value if nrem > 0 else OrderStatus.FILLED.value
            tx.execute("UPDATE orders SET remaining=?, status=? WHERE order_id=?", (nrem, nst, order_id))
            mid = ulidish()
            created = now_ts()
            tx.execute(
                """
                INSERT INTO matches(match_id,market_id,maker,taker,maker_side,outcome,price_e4,stake,status,created_ts)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    mid,
                    int(r["market_id"]),
                    r["maker"],
                    taker,
                    maker_side.value,
                    int(r["outcome"]),
                    price_e4,
                    fill,
                    MatchStatus.OPEN.value,
                    created,
                ),
            )
            return self.get_match(mid)

    def get_match(self, match_id: str) -> Match:
        c = self.db.conn()
        r = c.execute("SELECT * FROM matches WHERE match_id=?", (match_id,)).fetchone()
        if not r:
            raise ValueError("match not found")
        return Match(
            match_id=r["match_id"],
            market_id=int(r["market_id"]),
            maker=r["maker"],
            taker=r["taker"],
            maker_side=Side(r["maker_side"]),
            outcome=int(r["outcome"]),
            price_e4=int(r["price_e4"]),
            stake=float(r["stake"]),
            status=MatchStatus(r["status"]),
            created_ts=int(r["created_ts"]),
        )

    def list_matches(self, market_id: int | None = None, user: str | None = None) -> list[dict[str, t.Any]]:
        c = self.db.conn()
        q = "SELECT * FROM matches"
        args: list[t.Any] = []
        cond: list[str] = []
        if market_id is not None:
            cond.append("market_id=?")
            args.append(int(market_id))
        if user is not None:
            u = parse_user(user)
            cond.append("(maker=? OR taker=?)")
            args.extend([u, u])
        if cond:
            q += " WHERE " + " AND ".join(cond)
        q += " ORDER BY created_ts DESC LIMIT 500"
        rows = c.execute(q, args).fetchall()
        out: list[dict[str, t.Any]] = []
        for r in rows:
            out.append(
                {
                    "match_id": r["match_id"],
                    "market_id": int(r["market_id"]),
                    "maker": r["maker"],
                    "taker": r["taker"],
                    "maker_side": r["maker_side"],
                    "outcome": int(r["outcome"]),
                    "price_e4": int(r["price_e4"]),
                    "stake": float(r["stake"]),
                    "status": r["status"],
                    "created_ts": int(r["created_ts"]),
                }
            )
        return out

    def settle_market(self, market_id: int, outcome: int) -> dict[str, t.Any]:
        c = self.db.conn()
        with self.db.tx() as tx:
            mrow, cfg = self._market_cfg_row(tx, market_id)
            st = MarketStatus(mrow["status"])
            if st in (MarketStatus.SETTLED, MarketStatus.VOIDED):
                raise ValueError("market already final")
            if now_ts() < cfg.close_ts:
                raise ValueError("market not closed")
            outcome = parse_outcome(outcome, cfg.outcomes)
            tx.execute(
                "UPDATE markets SET status=?, settled_outcome=? WHERE market_id=?",
                (MarketStatus.SETTLED.value, int(outcome), int(market_id)),
            )
        return self.get_market(market_id)

    def void_market(self, market_id: int) -> dict[str, t.Any]:
        c = self.db.conn()
        with self.db.tx() as tx:
            mrow, _cfg = self._market_cfg_row(tx, market_id)
            st = MarketStatus(mrow["status"])
            if st in (MarketStatus.SETTLED, MarketStatus.VOIDED):
                raise ValueError("market already final")
            tx.execute("UPDATE markets SET status=?, settled_outcome=NULL WHERE market_id=?", (MarketStatus.VOIDED.value, int(market_id)))
        return self.get_market(market_id)

    def claim(self, match_id: str, claimant: str) -> dict[str, t.Any]:
        claimant = parse_user(claimant)
        c = self.db.conn()
        with self.db.tx() as tx:
            self._ensure_user(tx, claimant)
            m = tx.execute("SELECT * FROM matches WHERE match_id=?", (match_id,)).fetchone()
            if not m:
                raise ValueError("match not found")
            market_id = int(m["market_id"])
            mrow, cfg = self._market_cfg_row(tx, market_id)
            st = MarketStatus(mrow["status"])
            if st not in (MarketStatus.SETTLED, MarketStatus.VOIDED):
                raise ValueError("market not settled")
            maker = m["maker"]
            taker = m["taker"]
            if claimant not in (maker, taker):
                raise ValueError("not participant")
            status = MatchStatus(m["status"])
            if claimant == maker and status in (MatchStatus.CLAIMED_MAKER, MatchStatus.CLAIMED_BOTH):
                raise ValueError("already claimed")
            if claimant == taker and status in (MatchStatus.CLAIMED_TAKER, MatchStatus.CLAIMED_BOTH):
                raise ValueError("already claimed")
            maker_side = Side(m["maker_side"])
            taker_side = Side.LAY if maker_side == Side.BACK else Side.BACK
            claimant_side = maker_side if claimant == maker else taker_side
            price_e4 = int(m["price_e4"])
            stake = float(m["stake"])
            outcome = int(m["outcome"])
            if st == MarketStatus.VOIDED:
                gross = self._lock_for(claimant_side, price_e4, stake)
                fee = 0.0
                net = gross
            else:
                settled_outcome = int(mrow["settled_outcome"])
                wins = (settled_outcome == outcome) if claimant_side == Side.BACK else (settled_outcome != outcome)
                if wins:
                    if claimant_side == Side.BACK:
                        gross = stake * odds_to_float(price_e4)
                    else:
                        gross = stake
                else:
                    gross = 0.0
                fee = bps_fee(gross, cfg.fee.fee_bps)
                rebate = bps_fee(fee, cfg.fee.maker_rebate_bps) if claimant == maker else 0.0
                net = gross - fee + rebate
                fee = fee - rebate
            # Move locked -> pending payout for claimant.
            lock = self._lock_for(claimant_side, price_e4, stake)
            self._apply_balance(tx, claimant, da=0.0, dl=-lock, dp=net)
            self._post_ledger(tx, claimant, "CLAIM", match_id, da=0.0, dl=-lock, dp=net, note="claim payout")
            # Fee goes to house user.
            house = "HOUSE"
            self._ensure_user(tx, house)
            if fee > 0:
                self._apply_balance(tx, house, da=0.0, dl=0.0, dp=fee)
                self._post_ledger(tx, house, "FEE", match_id, da=0.0, dl=0.0, dp=fee, note="house fee")
            # Update match status
            nstatus = status
            if claimant == maker:
                nstatus = MatchStatus.CLAIMED_MAKER if status == MatchStatus.OPEN else MatchStatus.CLAIMED_BOTH
            else:
                nstatus = MatchStatus.CLAIMED_TAKER if status == MatchStatus.OPEN else MatchStatus.CLAIMED_BOTH
            tx.execute("UPDATE matches SET status=? WHERE match_id=?", (nstatus.value, match_id))
            return {"net": net, "fee": fee, "gross": gross, "match": dict(self.get_match(match_id).__dict__)}

    @staticmethod
    def _lock_for(side: Side, price_e4: int, stake: float) -> float:
        if side == Side.BACK:
            return float(stake)
        odds = odds_to_float(price_e4)
        if odds <= 1.0:
            return float(stake)
        liability = stake * (odds - 1.0)
