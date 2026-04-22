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

