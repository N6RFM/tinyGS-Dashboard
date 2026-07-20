#!/usr/bin/env python3
"""
TinyGS Dashboard Server for Linux Mint
========================================
Code written by N6RFM with help from Claude AI

- Reads serial from an ESP32 running TinyGS firmware
- Serves a live web dashboard at http://localhost:5000
- Auto-saves logs to ~/tinygs-dashboard/logs/ (both a processed, timestamped
  text log AND a raw unprocessed byte capture - see TinyGSServer.__init__)
- Lets you browse/view saved log files from the dashboard itself (see the
  logs_list_handler / logs_file_handler routes and the "Logs" button in the UI)

Architecture in one paragraph: aiohttp runs the web server (HTTP + WebSocket)
on the asyncio event loop in the main thread. A single background thread
(serial_reader, started in on_startup) owns the actual pyserial connection and
continuously drains it, splitting incoming bytes into lines and handing each
one to TinyGSServer.log(). log() updates in-memory state, writes to the two
on-disk log files, and pushes the new line out to every connected browser tab
over its WebSocket. Because log() can be called from either the event-loop
thread (e.g. a "Connected to ..." message logged in response to a websocket
action) or the background serial thread, its client-broadcast step has to be
thread-safe - see the comment on asyncio.run_coroutine_threadsafe below.

Notable bugs fixed during development (kept here so the reasoning isn't lost):
  - Broadcasting from the serial thread with asyncio.create_task() silently
    raised "no running event loop" and was swallowed, so the browser never
    got live updates even though serial data was being read fine. Fixed with
    run_coroutine_threadsafe() (see log()).
  - serial.Serial(port, baud, ...) opens the port *during construction* and
    briefly asserts DTR/RTS while doing so, which resets some ESP32 boards
    and can also change what the firmware sends. Fixed by building the
    Serial object unopened, dropping DTR/RTS first, then calling .open()
    (see connect_serial()).
  - Re-broadcasting the full line history after every connect/disconnect
    action raced with the individual per-line broadcast for that same event,
    causing duplicate lines to render in the browser. Fixed by splitting
    get_state() (full, used once on initial page load) from get_status()
    (lightweight, used for connect/disconnect broadcasts - see both methods).
  - log() used to open/write/close the log file on *every single line*,
    synchronously, inside the same thread responsible for draining the
    serial port - under a burst (e.g. a 4-line hex dump) this could stall
    the reader thread long enough to fall behind. Fixed by keeping a single
    persistent file handle open for the process lifetime (see __init__).
  - Added a raw, unprocessed byte-capture file (self._raw_fh) written before
    any decoding/line-splitting, so truncation/data-loss questions can be
    debugged by comparing it against the processed stream_*.log without
    needing a second program (e.g. picocom) fighting over the same port.

RESOLVED (diagnostically) - console hex-dump truncation:
  Long hex-dump lines from the ESP32 sometimes arrive already truncated with
  a literal "..." (or missing entirely). Root cause confirmed directly from
  the TinyGS firmware source (Logger.cpp, Log::AddLog()): before writing
  each queued log line, the firmware checks Serial.availableForWrite() - the
  free space in its own fixed-size (256 byte, ESP32 Arduino default) UART TX
  buffer. A single packet burst (RSSI/SNR/freq line + header + separators +
  up to 4 hex-dump lines) is ~400-500+ bytes queued in rapid succession by
  an async logger task; at 115200 baud that physically takes time to drain,
  and the fixed buffer can't hold it all. When a line doesn't fully fit, the
  firmware writes what it can and appends "..." (or, if there's no room at
  all, skips the line entirely and silently - explaining lines that were
  fully absent rather than truncated).

  This is deliberate firmware behavior, gated purely by the ESP32's own
  internal buffer state vs. its fixed UART baud rate - NOT by anything the
  receiving/reading side does. Confirmed NOT fixable from this codebase:
  none of DTR/RTS handling, HUPCL, or read-loop speed can influence it,
  since (absent hardware RTS/CTS flow control, which this board doesn't
  have wired - see the "Line state after open" diagnostic in
  connect_serial()) nothing downstream can signal backpressure to the
  firmware's UART peripheral at all.

  IMPORTANT: this only affects the human-readable CONSOLE PREVIEW. The full,
  untruncated packet bytes are captured and sent to the TinyGS MQTT network
  completely independently (see Radio.cpp: MQTT_Client::queueRx() uses the
  full respFrame, never the truncated console text) - no actual satellite
  data is lost, ever, regardless of what the local serial console shows.
  See TERMINAL_TRUNCATION_NOTE below for how the frontend surfaces this.
"""

import serial
import serial.tools.list_ports
import asyncio
import json
import datetime
import os
import re
import subprocess
import threading
import time
from collections import deque
from aiohttp import web, WSMsgType
import aiohttp

# Matches the TinyGS firmware's own truncation marker: Log::AddLog() in the
# firmware (Logger.cpp) appends a literal "..." when a line doesn't fully
# fit in the ESP32's fixed-size Serial TX buffer during a high-volume burst
# (see the module docstring's "RESOLVED (diagnostically)" section below for
# the full explanation).
TRUNCATION_MARKER_RE = re.compile(r'\.\.\.$')

# Known firmware strings that legitimately end in "..." as decoration, NOT
# as a truncation marker. Found by grepping the ENTIRE firmware source tree
# for every literal "..." in a Log::console/error/info/debug/*Async() call
# (see tinyGS/src/Radio/Radio.cpp, Mqtt/MQTT_Client.cpp,
# ConfigManager/ConfigManager.cpp, OTA/OTA.cpp, Improv/tinygs_improv.cpp) -
# not just excluding phrases one at a time as they're encountered, which
# missed several of these the first time around.
_KNOWN_NON_TRUNCATION_SUFFIXES = (
    "Initializing ...",                        # Radio.cpp - SX12xx radio init
    "Enable TCXO 33...",                       # Radio.cpp - SX1276 TCXO enable
    "Processing AX.25 frame...",                # Radio.cpp - AX.25 frame processing
    "Attempting MQTT connection...",            # MQTT_Client.cpp
    "Restarting...",                            # MQTT_Client.cpp - after repeated MQTT failures
    "retrying...",                              # MQTT_Client.cpp - MQTT timeout retry
    "Automatic board detection running...",     # ConfigManager.cpp
    "Checking for firmware Updates...",         # OTA.cpp
    "Failed to connect to wifi...",             # Improv/tinygs_improv.cpp
)


def _is_truncated(text):
    """Best-effort UI hint: does this line look like it was cut short by
    the firmware's Serial TX buffer limit (see Log::AddLog() in the
    firmware's Logger.cpp)? Not authoritative, not used for any
    data-integrity decision - only affects a cosmetic badge in the UI.

    A naive "ends with ...' check has a real false-positive case, seen in
    practice: a COMPLETE, full hex-dump data line from Log::log_packet()
    can legitimately end in "..." if its last 1-3 bytes happen to be
    non-printable - the firmware renders every non-printable byte as a
    literal '.' in the ASCII column (log_packet(): `sprintf(ascii+k, "%c",
    0x2E)`), so "the last few characters are periods" can mean either
    "genuinely truncated" or "these particular bytes just don't print".

    The distinguishing structural fact (from the same source): a complete
    line's hex portion and ASCII portion are always separated by a double
    space (each hex byte is formatted as "XX ", and log_packet() joins the
    two halves with another literal space: `"%s %s"` - see log_packet() in
    Logger.cpp), and the ASCII portion is always exactly bytes_per_line=16
    characters wide (padded if the packet's last line is short). A genuine
    truncation, by contrast, cuts raw bytes via Serial.write() with no
    awareness of that structure - it's very unlikely to coincidentally land
    exactly at "double-space followed by <=16 characters" the way a real,
    complete ASCII column does. So: if the text after the LAST double-space
    is 16 characters or fewer, treat it as a complete line whose ASCII
    column just happens to end in dots, not a truncation."""
    if not TRUNCATION_MARKER_RE.search(text):
        return False
    if text.rstrip().endswith(_KNOWN_NON_TRUNCATION_SUFFIXES):
        return False
    idx = text.rfind('  ')  # last double-space = the hex/ASCII column separator, if present
    if idx != -1 and len(text[idx + 2:]) <= 16:
        return False
    return True


# ============ CONFIG ============
SERIAL_PORT = None      # Auto-detect if None
BAUD_RATE = 115200
MAX_LINES = 2000        # in-memory + browser-side line history cap (see deque below)
LOG_DIR = os.path.expanduser("~/tinygs-dashboard/logs")
CONFIG_FILE = os.path.expanduser("~/tinygs-dashboard/config.json")
# ================================


def _get_git_version():
    """Best-effort short git commit hash of the running server code, shown
    in the UI and startup banner so it's always possible to tell exactly
    which build is actually live - useful after a `git pull` + restart, or
    when troubleshooting "is my fix actually deployed?" (a real recurring
    point of confusion during development). Appends "-dirty" if there are
    uncommitted local changes. Falls back to "unknown" if this isn't running
    from a git checkout, or git isn't installed - never fatal."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=script_dir, stderr=subprocess.DEVNULL, timeout=2
        ).decode().strip()
        is_dirty = subprocess.call(
            ["git", "diff", "--quiet"],
            cwd=script_dir, stderr=subprocess.DEVNULL
        ) != 0
        return commit + ("-dirty" if is_dirty else "")
    except Exception:
        return "unknown"


GIT_VERSION = _get_git_version()


def _load_config():
    """Best-effort load of the persisted config file (currently just WiFi
    Console settings). Returns {} on any error (missing file, corrupt JSON,
    permissions issue) - this is a convenience feature, never something that
    should prevent the server from starting."""
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_config(config):
    """Best-effort save of the config file, with restrictive permissions
    (owner read/write only) since it can contain a plaintext password.

    SECURITY NOTE: this password is stored in plaintext on disk, protected
    only by OS file permissions (chmod 600) - not encrypted. That's an
    explicit, documented tradeoff for convenience (avoiding re-entering it
    after every restart/upgrade) on what's assumed to be a personal,
    single-user machine. If that assumption doesn't hold for your setup,
    don't rely on this - the file lives at CONFIG_FILE (~/tinygs-dashboard/
    config.json) and can be deleted/edited manually, or this feature simply
    not used (re-enter the password each session instead)."""
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f)
        os.chmod(CONFIG_FILE, 0o600)
    except Exception:
        pass  # convenience feature only - never fatal if this fails



class TinyGSServer:
    """Owns all shared state: the serial connection, the in-memory line
    history, JSON-frame extraction, the on-disk log files, and the set of
    connected WebSocket clients. One instance (`server`, below) lives for the
    whole process."""

    def __init__(self):
        self.lines = deque(maxlen=MAX_LINES)
        self.json_frames = []
        self.total_bytes = 0
        self.connected = False
        self.ser = None
        self.clients = set()
        self.start_time = None  # serial-specific connect time, kept for compatibility
        # UPTIME/session-time display needs a start time that's active
        # whenever EITHER connection type is running, not just serial - the
        # original design only ever set self.start_time from
        # connect_serial()/disconnect_serial(), so UPTIME stayed frozen at
        # 00:00:00 for anyone using WiFi Console without a serial
        # connection, despite a real session genuinely being active. See
        # _refresh_session_start_time(), called from all four
        # connect/disconnect/start/stop methods.
        self.session_start_time = None
        self._lock = threading.Lock()
        self.loop = None  # set once the asyncio event loop is running (see on_startup)
        os.makedirs(LOG_DIR, exist_ok=True)
        # Filenames use UTC too (with a trailing Z), matching the timestamps
        # written inside the files - keeps everything on one clock and keeps
        # filenames correctly sortable regardless of what timezone/DST state
        # the machine happens to be in when the server starts.
        _file_ts = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        self._stream_file = os.path.join(LOG_DIR, f"stream_{_file_ts}.log")
        # Keep a single open file handle for the life of the process instead of
        # doing open()+write()+close() on every logged line. That per-line
        # open/close was happening synchronously inside serial_reader() - the
        # same thread responsible for draining the serial port - so during a
        # burst (e.g. a 4-line hex dump arriving in a tight burst from the
        # ESP32) the reader thread could fall behind. If the firmware's own
        # Serial.print() blocks/gives up when its TX buffer fills up because
        # the host isn't draining fast enough, that would show up exactly as
        # observed: only high-volume bursts get truncated with "...", while
        # simple short status lines never do.
        self._stream_fh = open(self._stream_file, "a", buffering=1)  # line-buffered

        # Raw, unprocessed byte capture - written the instant bytes come off
        # the wire, before decoding/line-splitting/logging touch them at all.
        # This is our "ground truth": if this file is complete but stream_*.log
        # is truncated, the bug is in our processing pipeline. If this file is
        # ALSO truncated, the truncation is happening upstream of us (firmware
        # or transport) and no Python-side fix can address it. Lets us settle
        # that question from a single session, no picocom/port-sharing needed.
        self._raw_file = os.path.join(LOG_DIR, f"raw_{_file_ts}.bin")
        self._raw_fh = open(self._raw_file, "ab", buffering=0)  # unbuffered binary

        # WiFi console polling: a SECOND, independent data source that reads
        # the ESP32's own local web dashboard log endpoint (GET /cs?c2=<id>)
        # over WiFi instead of USB serial. This reads from the firmware's
        # complete, untruncated internal log buffer (Log::AddLog()/getLog()
        # in Logger.cpp) - it is NOT subject to the 256-byte Serial TX buffer
        # limit that causes the "..." truncation on the USB serial path (see
        # the module docstring's "RESOLVED (diagnostically)" section). This
        # is genuinely independent of connect_serial()/disconnect_serial()
        # (works purely over WiFi) and is off by default - enabled via the
        # "🌐 WiFi Console" control in the UI once you know your board's
        # local IP (shown on its own web dashboard page).
        #
        # Credentials persist across restarts via CONFIG_FILE (see
        # _load_config/_save_config above) - loaded here as defaults so a
        # server restart/upgrade doesn't require re-entering the IP/password
        # in the browser every time. If polling was active when the server
        # last stopped (was_enabled), on_startup() below resumes it
        # automatically with no browser interaction needed at all.
        _saved = _load_config()
        self.wifi_console_enabled = False
        self.wifi_console_ip = _saved.get("wifi_console_ip")
        self.wifi_console_password = _saved.get("wifi_console_password", "")
        self._wifi_console_should_resume = bool(_saved.get("wifi_console_was_enabled"))
        self.wifi_console_last_id = "0"
        self.wifi_console_lines_received = 0
        self.wifi_console_last_error = None
        self.wifi_console_consecutive_errors = 0
        # Tracks whether a "poll failed" warning was actually logged for the
        # CURRENT failure streak - isolated single-attempt blips (WiFi jitter,
        # the embedded server being briefly busy) are extremely common over
        # many hours of continuous polling and self-heal within one retry;
        # logging every one of them just adds noise for something that costs
        # no data (the position tracker only advances on success, so a
        # failed poll never skips content). Only sustained failure streaks
        # get logged - see the threshold in _wifi_console_poll_loop(). This
        # flag makes sure a "recovered" message never appears without a
        # matching "failed" message having been shown first.
        self.wifi_console_error_logged = False
        self._wifi_console_task = None

    def _refresh_session_start_time(self):
        """Keeps self.session_start_time in sync with whether EITHER
        connection type (serial or WiFi Console) is currently active. Call
        this after any change to self.connected or self.wifi_console_enabled.
        Sets it on the transition from "nothing active" to "something
        active" (first connection of a session), and clears it once BOTH
        are inactive (session fully ended) - so reconnecting later starts a
        fresh UPTIME count rather than continuing a stale one. If one
        connection type is already active when the other one starts, the
        original start time is preserved (the session started when the
        FIRST connection came up, not when a second one joined)."""
        any_active = self.connected or self.wifi_console_enabled
        if any_active and self.session_start_time is None:
            self.session_start_time = datetime.datetime.now()
        elif not any_active:
            self.session_start_time = None

    def log(self, text, line_type="normal", source="serial"):
        """Record one logical line of output (or an internal event like
        "Connected to ..."). This is the single choke point for: updating
        in-memory state, extracting JSON telemetry frames, writing to the
        on-disk stream log, and broadcasting to connected browsers. Safe to
        call from any thread (see the run_coroutine_threadsafe note below).

        `source` is "serial" (default, USB serial) or "wifi" (WiFi console
        poller - see _wifi_console_poll_loop). This is tracked all the way
        through to exported JSON frames (`_source` field) as well as the
        live UI, since decoded telemetry could otherwise come from either
        path with no way to tell which - important given the two paths have
        different completeness guarantees (see the module docstring's
        "RESOLVED (diagnostically)" section on why WiFi-sourced data is
        never subject to the USB serial buffer's truncation).

        Timestamps: entry['time'] stays local-time-only (HH:MM:SS.mmm) since
        that's what's shown in the live browser terminal, which is more
        natural to read in real time than UTC. Everything written to disk
        (the stream log line, and JSON-frame _receivedAt below) uses GMT/UTC
        with a full date instead, so log files are unambiguous and directly
        comparable across sessions/timezones/DST changes. The local UTC
        offset is computed fresh on every call (not just once at file-open
        time) and appended to each line, so it stays correct even if a
        session happens to span a DST transition."""
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        now_local = now_utc.astimezone()  # same instant, in the system's local timezone
        offset_raw = now_local.strftime('%z')  # e.g. '-0800' or '+0000'
        utc_offset = f"{offset_raw[:3]}:{offset_raw[3:]}" if offset_raw else "+00:00"

        entry = {
            "text": text,
            "type": line_type,
            "time": now_local.strftime("%H:%M:%S.%f")[:-3],  # local time, for the live browser display only
            "timestamp": now_utc.isoformat(),                 # UTC, unambiguous - used for everything persisted
            "source": source,                                  # "serial" or "wifi" - see docstring above
            # Best-effort UI hint (not authoritative) - see _is_truncated().
            # Kept separate from `type` so it doesn't change which All/JSON/
            # TX/RX/Errors filter chip a line shows up under.
            "truncated": _is_truncated(text),
        }

        with self._lock:
            self.lines.append(entry)
            self.total_bytes += len(text.encode('utf-8'))

            # Extract JSON frames
            if '{' in text and '}' in text:
                try:
                    start = text.find('{')
                    end = text.rfind('}') + 1
                    if start >= 0 and end > start:
                        obj = json.loads(text[start:end])
                        obj["_receivedAt"] = now_utc.isoformat()
                        obj["_raw"] = text
                        obj["_source"] = source
                        self.json_frames.append(obj)
                except:
                    pass

        # Append to continuous stream log - reuse the already-open handle
        # (line-buffered, so this is a fast in-process write, not a syscall
        # storm) rather than reopening the file every single line.
        # Format: [YYYY-MM-DD HH:MM:SS.mmmZ (+HH:MM local)] text
        #   - date + time in GMT/UTC, unambiguous regardless of the machine's
        #     timezone or DST state
        #   - the trailing "Z" marks it explicitly as UTC (ISO 8601 convention)
        #   - "(+HH:MM local)" is this line's local UTC offset at the moment
        #     it was logged - add it to the UTC time algebraically to get
        #     local time (e.g. 07:15 UTC with offset -07:00 => 00:15 local)
        gmt_str = now_utc.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        self._stream_fh.write(f"[{gmt_str}Z ({utc_offset} local)] {entry['text']}\n")

        # Broadcast to web clients. log() can be called either from the asyncio
        # event loop thread (websocket handlers) or from the background serial
        # reader thread, so we can't use asyncio.create_task() here - it raises
        # "RuntimeError: no running event loop" when called off-thread, which
        # was silently swallowed and made the dashboard appear to hang with no
        # live updates. run_coroutine_threadsafe works safely from any thread.
        if self.loop is not None:
            asyncio.run_coroutine_threadsafe(self._broadcast(entry), self.loop)

    async def _broadcast(self, entry):
        """Push one log entry to every connected browser tab. Runs on the
        event loop (scheduled via run_coroutine_threadsafe from log(), which
        may itself be called from the background serial thread). Any client
        whose send fails (e.g. it disconnected) is dropped from the set."""
        dead = set()
        for ws in self.clients:
            try:
                await ws.send_json({"type": "line", "data": entry})
            except:
                dead.add(ws)
        self.clients -= dead

    def serial_reader(self):
        """Background thread (started once, in on_startup, and never
        restarted): the sole owner/reader of the pyserial connection. Loops
        forever regardless of connect/disconnect state - when disconnected it
        just idles (checking self.connected every 0.3s) rather than exiting,
        so a later connect_serial() call has a reader ready to go immediately.

        Bytes are accumulated in `buffer` and only handed off to log() once a
        complete line (terminated by \\n or \\r) has arrived; partial lines at
        the end of a read stay in `buffer` for the next iteration. This is
        deliberately conservative - it should never invent or drop bytes on
        its own. See the raw-capture write below for the actual ground-truth
        record of what came off the wire, used to rule this loop in or out
        as a source of data loss."""
        buffer = ""
        while True:
            if not self.connected or not self.ser:
                time.sleep(0.3)
                continue

            try:
                # Request a generous chunk rather than exactly `in_waiting`.
                # Under VMIN=0 semantics this doesn't add latency (read()
                # returns immediately with whatever's currently available,
                # never blocks waiting to fill the requested size) - it just
                # means that during a burst, we do fewer, larger read() calls
                # instead of many tiny ones, each of which costs a Python
                # function-call/syscall round trip. This matters because the
                # current best lead on the console hex-dump truncation issue
                # is USB-level backpressure: if this host doesn't drain
                # incoming bytes fast enough, that can propagate back through
                # the USB-serial bridge to the ESP32's own UART peripheral,
                # competing with its async console logger - see the module
                # docstring's "KNOWN UNRESOLVED ISSUE" section.
                data = self.ser.read(max(self.ser.in_waiting, 4096))
                if not data:
                    continue

                # Ground-truth capture: raw bytes, unprocessed, written first.
                try:
                    self._raw_fh.write(data)
                except Exception:
                    pass

                text = data.decode('utf-8', errors='replace')
                # Normalize line endings ONCE per read chunk, not once per
                # extracted line. The previous version ran this full-buffer
                # replace() call inside the line-extraction loop below, so a
                # single read containing N complete lines re-scanned and
                # rebuilt the (shrinking, but still substantial) buffer N
                # times instead of once - real, avoidable overhead on every
                # multi-line burst, which is exactly when it matters most.
                buffer += text.replace('\r\n', '\n').replace('\r', '\n')

                # Split whatever we've accumulated into complete lines. A
                # line only gets logged once we've actually seen its
                # terminator; anything left over (no \n yet) stays in
                # `buffer` and gets prepended to on the next read.
                while '\n' in buffer:
                    idx = buffer.find('\n')
                    line = buffer[:idx].strip()
                    buffer = buffer[idx+1:]
                    if line:
                        line_type = self._detect_type(line)
                        self.log(line, line_type)

            except serial.SerialException as e:
                self.log(f"Serial error: {e}", "error")
                self.connected = False
                self._refresh_session_start_time()
                if self.ser:
                    try: self.ser.close()
                    except: pass
                    self.ser = None
            except Exception as e:
                time.sleep(0.1)

    def _detect_type(self, text):
        """Best-effort classification of a line for the color-coded terminal
        and the All/JSON/TX/RX/Errors filter chips in the UI. Pure heuristic
        (keyword matching) - not authoritative, just a display convenience.
        Order matters: checked top to bottom, first match wins."""
        t = text.lower()
        if '"' in text or ('{' in text and '}' in text and '"satellite"' in t):
            return "json"
        if 'error' in t or 'fail' in t or 'exception' in t:
            return "error"
        if 'tx' in t or 'transmit' in t or 'sending' in t or 'uplink' in t:
            return "tx"
        if 'rx' in t or 'received' in t or 'packet' in t or 'frame' in t or 'rssi' in t:
            return "rx"
        if 'warn' in t or 'warning' in t:
            return "warn"
        if 'ok' in t or 'success' in t or 'connected' in t or 'ack' in t:
            return "success"
        return "normal"

    async def connect_serial(self, port=None):
        """Open the serial connection. If `port` is None/"Auto-detect", scans
        available ports and prefers ones whose USB descriptor matches common
        ESP32 USB-serial chips. Safe to call when already connected (no-op).
        See the inline comment below for why the port is opened in two steps
        instead of the simpler serial.Serial(port, baud) one-liner."""
        if self.connected:
            return {"ok": True, "message": "Already connected"}

        if port is None or port == "Auto-detect":
            ports = [p.device for p in serial.tools.list_ports.comports()]
            # Prefer common ESP32 USB chips
            preferred = []
            for p in serial.tools.list_ports.comports():
                if any(x in p.description.lower() for x in ['cp210', 'ch340', 'ft232', 'usb-serial', 'uart']):
                    preferred.append(p.device)
            ports = preferred + [p for p in ports if p not in preferred]
            if not ports:
                return {"ok": False, "error": "No serial ports found. Is the ESP32 plugged in?"}
            port = ports[0]

        try:
            # IMPORTANT: pyserial's Serial(port, baud, ...) constructor opens the
            # port immediately as part of construction, briefly asserting DTR/RTS
            # during that open sequence. Setting .dtr/.rts to False *after* that
            # call is too late - the reset pulse (and the firmware's "web serial
            # console" detection) has already happened by then.
            #
            # The fix is to build the Serial object *unopened*, set DTR/RTS low
            # first, and only then call .open(). This matches what a passive
            # terminal like picocom does, avoids the spurious ESP32 reset on
            # connect, and gets us the firmware's full (non-abbreviated) output.
            self.ser = serial.Serial()
            self.ser.port = port
            self.ser.baudrate = BAUD_RATE
            self.ser.timeout = 0.1
            self.ser.dsrdtr = False
            self.ser.rtscts = False
            self.ser.dtr = False
            self.ser.rts = False
            self.ser.open()

            # NOTE: an earlier version of this code also cleared HUPCL here,
            # on the theory that Linux dropping DTR-on-close was contributing
            # to the reset/truncation issues. A direct `stty -F <port> -a`
            # comparison against a known-working picocom connection proved
            # that theory backwards: picocom's actual working connection has
            # HUPCL *ON*, not off. That patch was reverted rather than left
            # in place based on a theory its own test data contradicted. The
            # rest of this comparison came back with every other termios
            # setting identical between picocom and pyserial (raw mode, flow
            # control, parity - all the same) - so the OS-level line
            # discipline is very likely NOT the source of the reset/
            # truncation behavior. See the module docstring / commit history
            # for the fuller chain of reasoning; the remaining suspects are
            # things `stty -a` can't show at all, like actual DTR/RTS/CTS
            # signal-line state at specific moments.

            self.connected = True
            self.start_time = datetime.datetime.now()
            self._refresh_session_start_time()
            self.log(f"Connected to {port} @ {BAUD_RATE} baud", "success")

            # Diagnostic: log the actual read-back modem control line state.
            # `stty -a` cannot show this at all (it only reports termios line
            # discipline settings, which a side-by-side comparison against a
            # known-working picocom connection showed were already identical
            # apart from HUPCL/VMIN, neither of which turned out to matter -
            # see the comment above). This is the next real lead: if this
            # ever gets compared against picocom's actual line state at the
            # same moment, whatever differs here is the remaining suspect.
            #
            # Each line is read individually (not one big try/except) because
            # not every property is supported on every backend/adapter - e.g.
            # RI/CD often aren't wired on cheap USB-serial adapters and raise
            # OSError when read. A single unsupported property shouldn't
            # silently drop every other value along with it.
            def _read_line_state(attr):
                try:
                    return str(getattr(self.ser, attr))
                except Exception as e:
                    return f"n/a ({type(e).__name__})"

            line_state = ", ".join(
                f"{name}={_read_line_state(attr)}"
                for name, attr in [("DTR", "dtr"), ("RTS", "rts"), ("CTS", "cts"),
                                    ("DSR", "dsr"), ("RI", "ri"), ("CD", "cd")]
            )
            self.log(f"Line state after open: {line_state}", "normal")

            return {"ok": True, "port": port, "baud": BAUD_RATE}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    async def disconnect_serial(self):
        """Close the serial connection if open. Always succeeds (best-effort
        close - errors while closing are swallowed since there's nothing
        useful to do about them). serial_reader() notices self.connected is
        False on its next loop iteration and idles until reconnected."""
        self.connected = False
        self._refresh_session_start_time()
        if self.ser:
            try: self.ser.close()
            except: pass
            self.ser = None
        self.log("Disconnected", "normal")
        self.start_time = None
        return {"ok": True}

    async def start_wifi_console(self, ip, password=""):
        """Start polling the ESP32's local web dashboard for complete,
        untruncated console text over WiFi - see the WiFi console polling
        comment in __init__ for why this exists. Independent of the USB
        serial connection; can run with or without it connected.

        `password` is your device's AP/admin password - confirmed directly
        from the firmware source (ConfigManager.cpp: handleRefreshConsole())
        that this endpoint requires HTTP Basic Auth with username "admin"
        (IOTWEBCONF_ADMIN_USER_NAME, hardcoded in IotWebConf2.h) and that
        same password, whenever the device is in its normal online state.
        Without it, every poll request gets rejected and nothing ever comes
        through - this was the actual cause of "0 lines received" with no
        visible error before this was added.

        If `ip`/`password` are omitted (empty), falls back to whatever was
        previously saved to CONFIG_FILE (or used earlier this session) -
        lets the browser resubmit with just an empty password field after a
        restart, rather than forcing full re-entry every time. On success,
        the working credentials are persisted to disk (see _save_config)."""
        if self.wifi_console_enabled:
            return {"ok": False, "error": "WiFi console is already running"}
        ip = (ip or "").strip() or (self.wifi_console_ip or "")
        password = password or self.wifi_console_password or ""
        if not ip:
            return {"ok": False, "error": "ESP32 IP address is required"}
        if not password:
            return {"ok": False, "error": "Admin/AP password is required"}

        self.wifi_console_ip = ip
        self.wifi_console_password = password
        self.wifi_console_last_id = "0"
        self.wifi_console_lines_received = 0
        self.wifi_console_enabled = True
        self._refresh_session_start_time()
        self._wifi_console_task = asyncio.create_task(self._wifi_console_poll_loop())
        _save_config({
            "wifi_console_ip": ip,
            "wifi_console_password": password,
            "wifi_console_was_enabled": True,
        })
        self.log(
            f"WiFi console polling started ({ip}) - lines from this source "
            f"are tagged [WiFi] and are NOT subject to the USB serial "
            f"buffer limit, so hex dumps via this path are always complete.",
            "success"
        )
        return {"ok": True, "ip": ip}

    async def stop_wifi_console(self):
        """Stop WiFi console polling. Always succeeds. Credentials stay
        saved to disk (so they're still pre-filled/reusable next time) -
        only the "was running" flag is cleared, so a restart won't
        auto-resume a session you deliberately stopped."""
        self.wifi_console_enabled = False
        self._refresh_session_start_time()
        if self._wifi_console_task:
            self._wifi_console_task.cancel()
            self._wifi_console_task = None
        _save_config({
            "wifi_console_ip": self.wifi_console_ip,
            "wifi_console_password": self.wifi_console_password,
            "wifi_console_was_enabled": False,
        })
        self.log("WiFi console polling stopped", "normal")
        return {"ok": True}

    async def _wifi_console_poll_loop(self):
        """Repeatedly GET /cs?c2=<id> from the ESP32's local web dashboard,
        matching the same protocol its own JS page uses (see the id-passing
        scheme: response is "<next_id>\\n<any new text since last id>").
        Runs as an asyncio task (not a thread) since this is pure async I/O
        via aiohttp's client session - no blocking calls involved.

        Sends HTTP Basic Auth (admin / self.wifi_console_password) on every
        request - confirmed required by the actual firmware source
        (handleRefreshConsole() in ConfigManager.cpp calls
        server.authenticate(...) and, on failure, server.requestAuthentication()
        then returns immediately with no content). This was missing entirely
        in the original version of this feature.

        Failures are tracked (self.wifi_console_last_error /
        _consecutive_errors), NOT silently swallowed - an earlier version of
        this loop just did `except: pass`, which meant a persistently
        failing poll (bad IP, network issue, unexpected response format, or
        - as turned out to be the actual cause - missing auth) would look
        identical in the UI to "connected but quiet", with zero way to tell
        the difference. Now: the first failure and every 5th consecutive one
        after that get logged as a visible warning line, and the error state
        is exposed via get_state()/get_status() for the UI.

        force_close=True: the ESP32's web server sends "Connection: close"
        on every response (confirmed via browser DevTools inspection) - it
        never keeps connections alive. aiohttp's default connector still
        tries to pool/reuse connections assuming keep-alive is possible,
        which can race against the server closing its end and produce
        occasional single-request failures (seen in practice as a "fail
        once, recover, fail once, recover" pattern - each failure isolated
        and immediately self-correcting, never sustained). Telling the
        connector not to bother pooling matches what the server already
        tells us to expect and removes that race entirely.

        Auth is sent as an explicit Authorization header via
        aiohttp.encode_basic_auth() rather than the ClientSession(auth=...)
        /aiohttp.BasicAuth shortcut - the latter is deprecated as of
        aiohttp 3.14 and slated for removal in 4.0."""
        auth_header = {"Authorization": aiohttp.encode_basic_auth("admin", self.wifi_console_password)}
        connector = aiohttp.TCPConnector(force_close=True)
        async with aiohttp.ClientSession(headers=auth_header, connector=connector) as session:
            while self.wifi_console_enabled:
                try:
                    url = f"http://{self.wifi_console_ip}/cs?c2={self.wifi_console_last_id}"
                    timeout = aiohttp.ClientTimeout(total=5)
                    async with session.get(url, timeout=timeout) as resp:
                        if resp.status != 200:
                            raise RuntimeError(f"HTTP {resp.status} from {url}")
                        text = await resp.text()
                        nl = text.find('\n')
                        if nl >= 0:
                            new_id, new_content = text[:nl], text[nl + 1:]
                        else:
                            new_id, new_content = text, ''
                        if new_id:
                            self.wifi_console_last_id = new_id.strip()
                        for line in new_content.split('\n'):
                            line = line.strip()
                            if line:
                                self.wifi_console_lines_received += 1
                                line_type = self._detect_type(line)
                                self.log(f"[WiFi] {line}", line_type, source="wifi")

                    # Success - clear any prior error state
                    if self.wifi_console_error_logged:
                        self.log(
                            f"WiFi console polling recovered after "
                            f"{self.wifi_console_consecutive_errors} failed attempt(s)",
                            "success"
                        )
                    self.wifi_console_last_error = None
                    self.wifi_console_consecutive_errors = 0
                    self.wifi_console_error_logged = False

                except Exception as e:
                    self.wifi_console_consecutive_errors += 1
                    self.wifi_console_last_error = str(e)
                    # Only surface a warning once a failure streak actually
                    # persists (3+ in a row), not on every isolated blip -
                    # see the __init__ comment on wifi_console_error_logged
                    # for why. Once surfaced, keep reminding every 5th
                    # attempt for a genuinely sustained outage.
                    if self.wifi_console_consecutive_errors == 3 or (
                        self.wifi_console_consecutive_errors > 3
                        and self.wifi_console_consecutive_errors % 5 == 0
                    ):
                        self.wifi_console_error_logged = True
                        self.log(
                            f"WiFi console poll failed ({self.wifi_console_consecutive_errors} "
                            f"consecutive failure(s)): {e}",
                            "error"
                        )

                # Match the ~2.3s interval the firmware's own web page uses,
                # so we're not hammering the ESP32's web server needlessly.
                await asyncio.sleep(2.3)

    async def get_state(self):
        """Full snapshot including the entire in-memory line history. Used
        exactly once per browser connection - right after its WebSocket opens
        (see websocket_handler) - to hydrate a freshly-loaded page. NOT used
        for the connect/disconnect broadcasts; see get_status() for why."""
        with self._lock:
            return {
                "connected": self.connected,
                "lines": list(self.lines),
                "jsonFrames": self.json_frames,
                "totalBytes": self.total_bytes,
                "uptime": (datetime.datetime.now() - self.session_start_time).total_seconds() if self.session_start_time else 0,
                "ports": [p.device for p in serial.tools.list_ports.comports()],
                "logDir": LOG_DIR,
                "wifiConsoleEnabled": self.wifi_console_enabled,
                "wifiConsoleIp": self.wifi_console_ip,
                "wifiConsoleLines": self.wifi_console_lines_received,
                "wifiConsoleError": self.wifi_console_last_error,
                "wifiConsoleErrorCount": self.wifi_console_consecutive_errors,
                "gitVersion": GIT_VERSION,
                "wifiConsoleHasSavedPassword": bool(self.wifi_console_password),
            }

    async def get_status(self):
        """Like get_state() but omits `lines` - used for connect/disconnect
        broadcasts to avoid re-sending the full log history to every client.
        New lines are already delivered incrementally by log()'s own broadcast;
        including `lines` here as well raced with that and caused the same
        entry (e.g. "Connected to ...") to be rendered twice in the browser."""
        with self._lock:
            return {
                "connected": self.connected,
                "jsonFramesCount": len(self.json_frames),
                "totalBytes": self.total_bytes,
                "uptime": (datetime.datetime.now() - self.session_start_time).total_seconds() if self.session_start_time else 0,
                "ports": [p.device for p in serial.tools.list_ports.comports()],
                "logDir": LOG_DIR,
                "wifiConsoleEnabled": self.wifi_console_enabled,
                "wifiConsoleIp": self.wifi_console_ip,
                "wifiConsoleLines": self.wifi_console_lines_received,
                "wifiConsoleError": self.wifi_console_last_error,
                "wifiConsoleErrorCount": self.wifi_console_consecutive_errors,
                "gitVersion": GIT_VERSION,
                "wifiConsoleHasSavedPassword": bool(self.wifi_console_password),
            }


server = TinyGSServer()


async def logs_list_handler(request):
    """Returns JSON metadata for every file in the logs directory."""
    files = []
    try:
        for name in os.listdir(LOG_DIR):
            path = os.path.join(LOG_DIR, name)
            if os.path.isfile(path):
                stat = os.stat(path)
                files.append({
                    "name": name,
                    "size": stat.st_size,
                    "mtime": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "current": os.path.abspath(path) == os.path.abspath(server._stream_file),
                })
    except FileNotFoundError:
        pass
    files.sort(key=lambda f: f["mtime"], reverse=True)
    return web.json_response({"logDir": LOG_DIR, "files": files})


async def logs_file_handler(request):
    """Serves the raw contents of a single file from the logs directory."""
    name = os.path.basename(request.match_info["filename"])  # strip any path components
    path = os.path.realpath(os.path.join(LOG_DIR, name))
    log_dir_real = os.path.realpath(LOG_DIR)

    # Make sure the resolved path is actually inside LOG_DIR (blocks ../ traversal)
    if not path.startswith(log_dir_real + os.sep) and path != log_dir_real:
        raise web.HTTPForbidden(text="Invalid log filename")

    if not os.path.isfile(path):
        raise web.HTTPNotFound(text="Log file not found")

    with open(path, "r", errors="replace") as f:
        content = f.read()

    return web.Response(text=content, content_type="text/plain")


async def websocket_handler(request):
    """The one WebSocket endpoint (/ws) driving the whole live UI.

    On connect: sends one {"type": "state", ...} message with the full
    current snapshot (see get_state()).

    Incoming client messages are JSON: {"action": "<name>", ...extra fields}.
    Supported actions: connect, disconnect, clear, export, listPorts,
    startWifiConsole, stopWifiConsole.

    Outgoing message types the frontend listens for (see connectWS() in the
    HTML/JS below): state (full snapshot, sent once on open), status
    (lightweight connect/disconnect update - no `lines`, see get_status()),
    line (one new log entry, from log()'s own broadcast), cleared, exported,
    result (ok/error response to the action that was just performed), ports,
    error."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    server.clients.add(ws)

    state = await server.get_state()
    await ws.send_json({"type": "state", "data": state})

    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
                action = data.get("action")

                if action == "connect":
                    result = await server.connect_serial(data.get("port"))
                    await ws.send_json({"type": "result", "data": result})
                    status = await server.get_status()
                    for c in server.clients:
                        try: await c.send_json({"type": "status", "data": status})
                        except: pass

                elif action == "disconnect":
                    result = await server.disconnect_serial()
                    await ws.send_json({"type": "result", "data": result})
                    status = await server.get_status()
                    for c in server.clients:
                        try: await c.send_json({"type": "status", "data": status})
                        except: pass

                elif action == "clear":
                    server.lines.clear()
                    server.json_frames.clear()
                    server.total_bytes = 0
                    for c in server.clients:
                        try: await c.send_json({"type": "cleared"})
                        except: pass

                elif action == "export":
                    filename = f"frames_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
                    filepath = os.path.join(LOG_DIR, filename)
                    with open(filepath, "w") as f:
                        json.dump(server.json_frames, f, indent=2)
                    await ws.send_json({"type": "exported", "data": {"file": filepath}})

                elif action == "listPorts":
                    ports = [p.device for p in serial.tools.list_ports.comports()]
                    await ws.send_json({"type": "ports", "data": ports})

                elif action == "startWifiConsole":
                    result = await server.start_wifi_console(data.get("ip"), data.get("password", ""))
                    await ws.send_json({"type": "result", "data": result})
                    status = await server.get_status()
                    for c in server.clients:
                        try: await c.send_json({"type": "status", "data": status})
                        except: pass

                elif action == "stopWifiConsole":
                    result = await server.stop_wifi_console()
                    await ws.send_json({"type": "result", "data": result})
                    status = await server.get_status()
                    for c in server.clients:
                        try: await c.send_json({"type": "status", "data": status})
                        except: pass

            except Exception as e:
                await ws.send_json({"type": "error", "data": str(e)})

    server.clients.discard(ws)
    return ws


async def index_handler(request):
    """Serves the single-page dashboard (inline HTML/CSS/JS - no separate
    static files/build step, kept as one file for easy deployment). All live
    behavior after page load happens over the /ws WebSocket; see
    websocket_handler's docstring for the message protocol."""
    html = '''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>TinyGS Dashboard</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: "Ubuntu", "DejaVu Sans", system-ui, -apple-system, sans-serif;
      background: #111118;
      color: #e8e8f0;
      padding: 20px;
      min-height: 100vh;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 20px;
      padding-bottom: 15px;
      border-bottom: 1px solid #2a2a3a;
    }
    .header h2 {
      font-size: 20px;
      font-weight: 600;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .version-tag {
      font-size: 11px;
      font-weight: 400;
      font-family: "Ubuntu Mono", monospace;
      color: #666680;
      background: #1a1a28;
      padding: 2px 8px;
      border-radius: 4px;
      cursor: help;
    }
    .status {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
      color: #8888a0;
    }
    .status-dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      background: #444;
      transition: all 0.3s ease;
    }
    .status-dot.active {
      background: #4ade80;
      box-shadow: 0 0 0 3px rgba(74,222,128,0.2);
    }
    .meters {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }
    .meter {
      background: #1a1a28;
      border: 1px solid #2a2a3a;
      border-radius: 10px;
      padding: 16px;
    }
    .meter-label {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: #666680;
      margin-bottom: 6px;
    }
    .meter-value {
      font-size: 26px;
      font-weight: 700;
      font-family: "Ubuntu Mono", "DejaVu Sans Mono", monospace;
      color: #f0f0f8;
    }
    .meter-sub {
      font-size: 11px;
      color: #555570;
      margin-top: 4px;
    }
    .filters {
      display: flex;
      gap: 8px;
      margin-bottom: 14px;
      flex-wrap: wrap;
    }
    .filter-chip {
      padding: 5px 14px;
      border-radius: 6px;
      border: 1px solid #333340;
      background: #1a1a28;
      font-size: 12px;
      color: #8888a0;
      cursor: pointer;
      transition: all 0.15s ease;
      user-select: none;
    }
    .filter-chip:hover {
      border-color: #555570;
    }
    .filter-chip.active {
      background: #e8e8f0;
      color: #111118;
      border-color: #e8e8f0;
    }
    .controls {
      display: flex;
      gap: 10px;
      margin-bottom: 16px;
      flex-wrap: wrap;
      align-items: center;
    }
    select {
      padding: 9px 14px;
      background: #1a1a28;
      color: #e8e8f0;
      border: 1px solid #333340;
      border-radius: 8px;
      font-size: 13px;
      outline: none;
    }
    select:focus {
      border-color: #555570;
    }
    button {
      padding: 9px 18px;
      border: 1px solid #333340;
      border-radius: 8px;
      background: #1e1e30;
      color: #e8e8f0;
      cursor: pointer;
      font-size: 13px;
      font-weight: 500;
      transition: all 0.15s ease;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    button:hover:not(:disabled) {
      background: #252540;
      border-color: #444460;
    }
    button:disabled {
      opacity: 0.35;
      cursor: not-allowed;
    }
    button.primary {
      background: #4ade80;
      color: #0a0a12;
      border-color: #4ade80;
    }
    button.primary:hover:not(:disabled) {
      background: #22c55e;
      border-color: #22c55e;
    }
    button.danger {
      border-color: #f87171;
      color: #f87171;
    }
    button.danger:hover:not(:disabled) {
      background: rgba(248,113,113,0.1);
    }
    .terminal {
      background: #0d0d15;
      border: 1px solid #2a2a3a;
      border-radius: 10px;
      overflow: hidden;
    }
    .term-header {
      padding: 12px 16px;
      background: #161624;
      border-bottom: 1px solid #2a2a3a;
      font-size: 12px;
      color: #666680;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .term-dots {
      display: flex;
      gap: 6px;
      margin-right: 10px;
    }
    .term-dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      border: 1px solid #333340;
    }
    .term-dot.red { background: #f87171; }
    .term-dot.yellow { background: #fbbf24; }
    .term-dot.green { background: #4ade80; }
    .term-body {
      height: 420px;
      overflow-y: auto;
      padding: 12px 16px;
      font-family: "Ubuntu Mono", "DejaVu Sans Mono", monospace;
      font-size: 12px;
      line-height: 1.65;
      color: #a0a0b8;
    }
    .term-body::-webkit-scrollbar { width: 6px; }
    .term-body::-webkit-scrollbar-track { background: transparent; }
    .term-body::-webkit-scrollbar-thumb { background: #333340; border-radius: 3px; }
    .line {
      padding: 1px 0;
      border-bottom: 1px solid transparent;
      animation: fadeIn 0.15s ease;
    }
    @keyframes fadeIn {
      from { opacity: 0; transform: translateX(-4px); }
      to { opacity: 1; transform: translateX(0); }
    }
    .line:hover {
      background: rgba(255,255,255,0.03);
    }
    .line .ts {
      color: #444460;
      margin-right: 10px;
      user-select: none;
      font-size: 11px;
    }
    .line.json { color: #60a5fa; }
    .line.error { color: #f87171; }
    .line.success { color: #4ade80; }
    .line.warn { color: #fbbf24; }
    .line.tx { color: #a78bfa; }
    .line.rx { color: #34d399; }
    .trunc-badge {
      display: inline-block;
      margin-left: 8px;
      padding: 0 6px;
      border-radius: 4px;
      background: rgba(251, 191, 36, 0.15);
      color: #fbbf24;
      font-size: 10px;
      cursor: help;
      user-select: none;
      vertical-align: middle;
    }
    .line .wifi-tag {
      color: #22d3ee;
      font-weight: 600;
      margin-right: 4px;
    }
    .bottom-bar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 10px 16px;
      background: #161624;
      border-top: 1px solid #2a2a3a;
      font-size: 11px;
      color: #555570;
    }
    .toast {
      position: fixed;
      bottom: 24px;
      right: 24px;
      padding: 12px 20px;
      border-radius: 8px;
      background: #e8e8f0;
      color: #111118;
      font-size: 13px;
      font-weight: 500;
      opacity: 0;
      transform: translateY(12px);
      transition: all 0.3s ease;
      pointer-events: none;
      z-index: 100;
      box-shadow: 0 8px 24px rgba(0,0,0,0.3);
    }
    .toast.show {
      opacity: 1;
      transform: translateY(0);
    }
    .empty-state {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      height: 200px;
      color: #444460;
      font-size: 13px;
      gap: 10px;
    }
    .modal-overlay {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0,0,0,0.6);
      z-index: 200;
      align-items: center;
      justify-content: center;
    }
    .modal-overlay.show { display: flex; }
    .modal {
      background: #161624;
      border: 1px solid #2a2a3a;
      border-radius: 12px;
      width: min(720px, 92vw);
      max-height: 82vh;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }
    .modal-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 14px 18px;
      border-bottom: 1px solid #2a2a3a;
    }
    .modal-header h3 { font-size: 15px; font-weight: 600; }
    .modal-close {
      background: none;
      border: none;
      color: #8888a0;
      font-size: 18px;
      cursor: pointer;
      line-height: 1;
      padding: 4px;
    }
    .modal-close:hover { color: #e8e8f0; }
    .modal-body { overflow-y: auto; padding: 10px; }
    .log-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 10px 12px;
      border-radius: 8px;
      cursor: pointer;
      font-size: 13px;
    }
    .log-row:hover { background: #1e1e2e; }
    .log-row.current { border: 1px solid #4ade80; }
    .log-name { display: flex; align-items: center; gap: 8px; color: #e8e8f0; }
    .log-meta { color: #666680; font-size: 11px; }
    .log-empty { padding: 24px; text-align: center; color: #555570; font-size: 13px; }
    .log-viewer {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0,0,0,0.6);
      z-index: 210;
      align-items: center;
      justify-content: center;
    }
    .log-viewer.show { display: flex; }
    .log-viewer-inner {
      background: #0c0c14;
      border: 1px solid #2a2a3a;
      border-radius: 12px;
      width: min(900px, 94vw);
      height: min(640px, 86vh);
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }
    .log-viewer-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 12px 16px;
      border-bottom: 1px solid #2a2a3a;
      font-size: 13px;
      color: #b0b0c8;
    }
    .log-viewer-header .actions { display: flex; gap: 8px; align-items: center; }
    .log-viewer-header a { color: #8888a0; text-decoration: none; font-size: 12px; }
    .log-viewer-header a:hover { color: #e8e8f0; }
    .log-viewer-body {
      flex: 1;
      overflow: auto;
      margin: 0;
      padding: 14px 16px;
      font-family: "Ubuntu Mono", "DejaVu Sans Mono", monospace;
      font-size: 12px;
      color: #d0d0e0;
      white-space: pre-wrap;
      word-break: break-word;
    }
  </style>
</head>
<body>
  <div class="header">
    <h2>🛰️ TinyGS Serial Dashboard <span class="version-tag" title="Git commit of the currently running server code - use this to confirm an upgrade/restart actually picked up your latest changes">__GIT_VERSION__</span></h2>
    <div class="status">
      <div class="status-dot" id="statusDot"></div>
      <span id="statusText">Disconnected</span>
    </div>
  </div>

  <div class="meters">
    <div class="meter">
      <div class="meter-label">Frames Received</div>
      <div class="meter-value" id="mFrames">0</div>
      <div class="meter-sub">total lines</div>
    </div>
    <div class="meter">
      <div class="meter-label">Bytes Streamed</div>
      <div class="meter-value" id="mBytes" style="font-size:18px;padding-top:4px;">0 B</div>
      <div class="meter-sub">since start</div>
    </div>
    <div class="meter">
      <div class="meter-label">JSON Frames</div>
      <div class="meter-value" id="mJson">0</div>
      <div class="meter-sub">parsed objects</div>
    </div>
    <div class="meter">
      <div class="meter-label">Uptime</div>
      <div class="meter-value" id="mUptime" style="font-size:18px;padding-top:4px;">00:00:00</div>
      <div class="meter-sub">session time</div>
    </div>
  </div>

  <div class="filters">
    <div class="filter-chip active" onclick="setFilter('all')">All</div>
    <div class="filter-chip" onclick="setFilter('json')">JSON Only</div>
    <div class="filter-chip" onclick="setFilter('tx')">TX Events</div>
    <div class="filter-chip" onclick="setFilter('rx')">RX Events</div>
    <div class="filter-chip" onclick="setFilter('error')">Errors</div>
  </div>

  <div class="controls">
    <select id="portSelect"><option>Auto-detect</option></select>
    <button class="primary" id="btnConnect" onclick="toggleConnect()">
      <span>▶</span> Connect
    </button>
    <button id="btnClear" onclick="sendAction('clear')" disabled>
      <span>🗑</span> Clear
    </button>
    <button id="btnExport" onclick="sendAction('export')" disabled>
      <span>⬇</span> Export JSON
    </button>
    <button onclick="sendAction('listPorts')">
      <span>🔄</span> Refresh Ports
    </button>
    <button onclick="openLogsModal()">
      <span>📂</span> Logs
    </button>
    <button id="btnWifiConsole" onclick="openWifiConsoleModal()">
      <span>🌐</span> WiFi Console
    </button>
  </div>

  <div class="terminal">
    <div class="term-header">
      <div style="display:flex;align-items:center;">
        <div class="term-dots">
          <div class="term-dot red"></div>
          <div class="term-dot yellow"></div>
          <div class="term-dot green"></div>
        </div>
        <span id="termTitle">serial://waiting</span>
      </div>
      <span id="lineCount">0 lines</span>
    </div>
    <div class="term-body" id="terminal">
      <div class="empty-state">
        <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
          <rect x="2" y="3" width="20" height="14" rx="2"/>
          <line x1="8" y1="21" x2="16" y2="21"/>
          <line x1="12" y1="17" x2="12" y2="21"/>
        </svg>
        Click Connect to open the serial port
      </div>
    </div>
    <div class="bottom-bar">
      <span id="portInfo">No port selected</span>
      <span id="lastActivity">--</span>
    </div>
  </div>

  <div class="toast" id="toast"></div>

  <div class="modal-overlay" id="logsModal">
    <div class="modal">
      <div class="modal-header">
        <h3>📂 Log Files</h3>
        <button class="modal-close" onclick="closeLogsModal()">✕</button>
      </div>
      <div class="modal-body" id="logsModalBody">
        <div class="log-empty">Loading...</div>
      </div>
    </div>
  </div>

  <div class="log-viewer" id="logViewer">
    <div class="log-viewer-inner">
      <div class="log-viewer-header">
        <span id="logViewerTitle">log</span>
        <div class="actions">
          <a id="logViewerRaw" href="#" target="_blank">Open raw ↗</a>
          <button class="modal-close" onclick="closeLogViewer()">✕</button>
        </div>
      </div>
      <pre class="log-viewer-body" id="logViewerBody"></pre>
    </div>
  </div>

  <div class="modal-overlay" id="wifiConsoleModal">
    <div class="modal">
      <div class="modal-header">
        <h3>🌐 WiFi Console</h3>
        <button class="modal-close" onclick="closeWifiConsoleModal()">✕</button>
      </div>
      <div class="modal-body" style="padding: 16px;">
        <p style="color:#a0a0b8; font-size:13px; line-height:1.5; margin-bottom:14px;">
          Polls your ESP32's own local web dashboard (the same one you'd
          visit in a browser at its IP address) for console text, over WiFi
          instead of USB serial. This source reads the firmware's complete,
          untruncated log buffer directly - hex dumps from here are never
          cut short, unlike the USB serial connection. Lines from this
          source appear in the terminal tagged <code>[WiFi]</code>.
        </p>
        <p style="color:#a0a0b8; font-size:12px; line-height:1.5; margin-bottom:14px;">
          This endpoint requires your device's <strong>admin/AP password</strong>
          (the same one you use to log into its web config panel) - the
          firmware rejects unauthenticated requests, which is why this field
          is required, not optional.
        </p>
        <p id="wifiConsoleSavedHint" style="display:none; color:#4ade80; font-size:12px; margin-bottom:10px;">
          🔑 Using a previously saved IP/password - leave fields blank to
          reuse them, or fill in new values to update.
        </p>
        <div id="wifiConsoleIdleForm">
          <input id="wifiConsoleIpInput" placeholder="ESP32 IP address, e.g. 192.168.113.210" style="margin-bottom:10px;">
          <input id="wifiConsolePasswordInput" type="password" placeholder="Admin/AP password" style="margin-bottom:10px;">
          <button class="primary" onclick="startWifiConsole()">Start Polling</button>
        </div>
        <div id="wifiConsoleActiveInfo" style="display:none;">
          <p id="wifiConsoleStatusLine" style="color:#4ade80; font-size:13px;">
            🟢 Polling <span id="wifiConsoleActiveIp"></span> -
            <span id="wifiConsoleLineCount">0</span> lines received
          </p>
          <p id="wifiConsoleErrorLine" style="display:none; color:#f87171; font-size:12px; margin-top:-8px;"></p>
          <button class="danger" onclick="stopWifiConsole()">Stop Polling</button>
        </div>
      </div>
    </div>
  </div>

  <script>
    // Frontend overview: a single persistent WebSocket (see connectWS) drives
    // everything - there's no polling. `lines` is the client-side mirror of
    // the server's line history, capped at MAX_LINES-worth (2000, matching
    // the server's deque) and re-rendered into the terminal pane whenever it
    // changes. See websocket_handler's docstring (server side) for the full
    // message protocol this listens for.
    let ws, currentFilter = 'all', lines = [], isConnected = false;
    // Wall-clock anchor for the live uptime ticker (see the setInterval near
    // the bottom of this script). Sever gives us `state.uptime` (seconds
    // elapsed as of that message) only occasionally - on connect/disconnect
    // and initial page load - not every second. Without ticking locally in
    // between those messages, the uptime display would show a stale, frozen
    // value the rest of the time. `uptimeAnchor` is the epoch-ms timestamp
    // (client clock) corresponding to when the connection started, computed
    // from the most recent state/status message; null while disconnected.
    let uptimeAnchor = null;
    const terminal = document.getElementById('terminal');
    const btnConnect = document.getElementById('btnConnect');
    const btnClear = document.getElementById('btnClear');
    const btnExport = document.getElementById('btnExport');

    function showToast(msg) {
      const t = document.getElementById('toast');
      t.textContent = msg;
      t.classList.add('show');
      setTimeout(() => t.classList.remove('show'), 2500);
    }

    function formatLogBytes(b) {
      if (b < 1024) return b + ' B';
      if (b < 1024*1024) return (b/1024).toFixed(1) + ' KB';
      return (b/1024/1024).toFixed(2) + ' MB';
    }

    function formatLogTime(iso) {
      try { return new Date(iso).toLocaleString(); } catch (e) { return iso; }
    }

    async function openLogsModal() {
      const modal = document.getElementById('logsModal');
      const body = document.getElementById('logsModalBody');
      modal.classList.add('show');
      body.innerHTML = '<div class="log-empty">Loading...</div>';
      try {
        const res = await fetch('/logs');
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const data = await res.json();
        if (!data.files || data.files.length === 0) {
          body.innerHTML = '<div class="log-empty">No log files yet</div>';
          return;
        }
        body.innerHTML = data.files.map(f => `
          <div class="log-row ${f.current ? 'current' : ''}" onclick="openLogViewer('${encodeURIComponent(f.name)}')">
            <div class="log-name">${f.current ? '🟢' : '📄'} ${f.name}</div>
            <div class="log-meta">${formatLogBytes(f.size)} · ${formatLogTime(f.mtime)}</div>
          </div>
        `).join('');
      } catch (e) {
        body.innerHTML = '<div class="log-empty">Failed to load logs: ' + e.message + '</div>';
      }
    }

    function closeLogsModal() {
      document.getElementById('logsModal').classList.remove('show');
    }

    function openWifiConsoleModal() {
      document.getElementById('wifiConsoleModal').classList.add('show');
    }

    function closeWifiConsoleModal() {
      document.getElementById('wifiConsoleModal').classList.remove('show');
    }

    let wifiConsoleHasSavedPassword = false;

    function startWifiConsole() {
      const ip = document.getElementById('wifiConsoleIpInput').value.trim();
      const password = document.getElementById('wifiConsolePasswordInput').value;
      if (!ip) { showToast('Enter an IP address first'); return; }
      if (!password && !wifiConsoleHasSavedPassword) {
        showToast('Enter the admin/AP password - the firmware requires it');
        return;
      }
      sendAction('startWifiConsole', {ip, password});
    }

    function stopWifiConsole() {
      sendAction('stopWifiConsole');
    }

    function updateWifiConsoleUI(state) {
      const idleForm = document.getElementById('wifiConsoleIdleForm');
      const activeInfo = document.getElementById('wifiConsoleActiveInfo');
      const btn = document.getElementById('btnWifiConsole');
      const statusLine = document.getElementById('wifiConsoleStatusLine');
      const errorLine = document.getElementById('wifiConsoleErrorLine');
      const savedHint = document.getElementById('wifiConsoleSavedHint');
      const ipInput = document.getElementById('wifiConsoleIpInput');

      wifiConsoleHasSavedPassword = !!state.wifiConsoleHasSavedPassword;

      // Pre-fill the IP field from a previous session (server-persisted -
      // see CONFIG_FILE), but only if the user hasn't already typed
      // something themselves, so we don't clobber active typing on every
      // state update.
      if (state.wifiConsoleIp && !ipInput.value && document.activeElement !== ipInput) {
        ipInput.value = state.wifiConsoleIp;
      }
      savedHint.style.display = (wifiConsoleHasSavedPassword && !state.wifiConsoleEnabled) ? 'block' : 'none';

      if (state.wifiConsoleEnabled) {
        idleForm.style.display = 'none';
        activeInfo.style.display = 'block';
        const ip = state.wifiConsoleIp || '';
        const lineCount = (state.wifiConsoleLines || 0).toLocaleString();
        // Rebuild fully each time (not a partial mutation) so the status
        // line can't get stuck showing a stale "(currently failing)" state
        // after polling actually recovers.
        if (state.wifiConsoleErrorCount > 0) {
          statusLine.innerHTML = `🟡 Polling <span id="wifiConsoleActiveIp">${ip}</span> - <span id="wifiConsoleLineCount">${lineCount}</span> lines received (currently failing)`;
          errorLine.style.display = 'block';
          errorLine.textContent = `⚠ ${state.wifiConsoleErrorCount} consecutive failure(s): ${state.wifiConsoleError || 'unknown error'}`;
        } else {
          statusLine.innerHTML = `🟢 Polling <span id="wifiConsoleActiveIp">${ip}</span> - <span id="wifiConsoleLineCount">${lineCount}</span> lines received`;
          errorLine.style.display = 'none';
        }
        btn.classList.add('danger');
      } else {
        idleForm.style.display = 'block';
        activeInfo.style.display = 'none';
        btn.classList.remove('danger');
      }
    }

    async function openLogViewer(encodedName) {
      const name = decodeURIComponent(encodedName);
      const viewer = document.getElementById('logViewer');
      const viewerBody = document.getElementById('logViewerBody');
      const viewerTitle = document.getElementById('logViewerTitle');
      const rawLink = document.getElementById('logViewerRaw');

      viewerTitle.textContent = name;
      rawLink.href = '/logs/' + encodedName;
      viewerBody.textContent = 'Loading...';
      viewer.classList.add('show');

      try {
        const res = await fetch('/logs/' + encodedName);
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const text = await res.text();
        viewerBody.textContent = text.length ? text : '(empty file)';
        viewerBody.scrollTop = viewerBody.scrollHeight;
      } catch (e) {
        viewerBody.textContent = 'Failed to load file: ' + e.message;
      }
    }

    function closeLogViewer() {
      document.getElementById('logViewer').classList.remove('show');
    }

    document.getElementById('logsModal').addEventListener('click', (e) => {
      if (e.target.id === 'logsModal') closeLogsModal();
    });
    document.getElementById('logViewer').addEventListener('click', (e) => {
      if (e.target.id === 'logViewer') closeLogViewer();
    });
    document.getElementById('wifiConsoleModal').addEventListener('click', (e) => {
      if (e.target.id === 'wifiConsoleModal') closeWifiConsoleModal();
    });

    function connectWS() {
      // Opens the single live WebSocket connection. On close (server
      // restart, network blip, laptop sleep, etc.) automatically retries
      // every 2s - the browser tab never needs a manual refresh to recover.
      const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
      ws = new WebSocket(`${proto}//${location.host}/ws`);

      ws.onopen = () => console.log('WS connected');

      ws.onmessage = (e) => {
        const msg = JSON.parse(e.data);
        if (msg.type === 'state') updateState(msg.data);
        if (msg.type === 'status') updateStatusUI(msg.data);
        if (msg.type === 'line') addLine(msg.data);
        if (msg.type === 'cleared') { lines = []; render(); showToast('Terminal cleared'); }
        if (msg.type === 'exported') showToast('Exported: ' + msg.data.file.split('/').pop());
        if (msg.type === 'result') handleResult(msg.data);
        if (msg.type === 'ports') updatePortList(msg.data);
        if (msg.type === 'error') showToast('Error: ' + msg.data);
      };

      ws.onclose = () => {
        setTimeout(connectWS, 2000);
      };
    }

    function updateStatusUI(state) {
      // Updates everything EXCEPT the `lines` array/terminal render. Used both
      // by the full initial state load and by the lightweight connect/disconnect
      // status broadcasts, which deliberately omit `lines` - new lines already
      // arrive individually via 'line' messages, and re-including the full
      // history here caused each just-logged line (e.g. "Connected to ...")
      // to be rendered twice: once from the resync, once from its own broadcast.
      isConnected = state.connected;
      document.getElementById('statusDot').classList.toggle('active', state.connected);
      document.getElementById('statusText').textContent = state.connected ? 'Connected' : 'Disconnected';
      document.getElementById('mBytes').textContent = formatBytes(state.totalBytes);
      document.getElementById('mJson').textContent = (state.jsonFramesCount ?? (state.jsonFrames ? state.jsonFrames.length : 0)).toLocaleString();
      document.getElementById('mUptime').textContent = formatUptime(state.uptime);

      // Re-derive the anchor every time we get a fresh reading from the
      // server, so the local ticker (below) stays in sync and self-corrects
      // for any client/server clock drift rather than accumulating error.
      // A session is "active" for UPTIME purposes if EITHER serial OR WiFi
      // Console is connected - originally this only checked state.connected
      // (serial), so UPTIME stayed frozen at 00:00:00 for anyone running
      // WiFi Console without a serial connection, despite a real session
      // genuinely being active server-side (see session_start_time).
      const sessionActive = state.connected || state.wifiConsoleEnabled;
      uptimeAnchor = sessionActive ? (Date.now() - state.uptime * 1000) : null;
      document.getElementById('portInfo').textContent = state.connected ? 'Streaming...' : 'No port selected';
      document.getElementById('termTitle').textContent = state.connected ? 'serial://active @ 115200' : 'serial://waiting';

      updatePortList(state.ports);
      updateWifiConsoleUI(state);

      btnConnect.innerHTML = state.connected ? '<span>⏹</span> Disconnect' : '<span>▶</span> Connect';
      btnConnect.classList.toggle('danger', state.connected);
      btnConnect.classList.toggle('primary', !state.connected);
      btnConnect.disabled = false;
      btnClear.disabled = !state.connected && lines.length === 0;
      if (state.jsonFrames) btnExport.disabled = state.jsonFrames.length === 0;
      else if (typeof state.jsonFramesCount === 'number') btnExport.disabled = state.jsonFramesCount === 0;
    }

    function updateState(state) {
      // Full state load (initial websocket connection): includes `lines`.
      lines = state.lines;
      document.getElementById('mFrames').textContent = state.lines.length.toLocaleString();
      updateStatusUI(state);
      render();
    }

    function addLine(entry) {
      // Mirrors the server's deque(maxlen=MAX_LINES) cap client-side so this
      // array can't grow unbounded over a long session.
      lines.push(entry);
      if (lines.length > 2000) lines.shift();
      render();
      setTimeout(() => terminal.scrollTop = terminal.scrollHeight, 10);
    }

    function render() {
      // Rebuilds the terminal pane's DOM from `lines`. Only the newest 400
      // (post-filter) lines are actually rendered - past that, rebuilding
      // innerHTML on every incoming line gets visibly slow. Nothing is lost:
      // the full history up to `lines.length` still lives in memory and in
      // the server-side log files; this is purely a rendering-cost cap.
      const filtered = currentFilter === 'all' ? lines : lines.filter(l => l.type === currentFilter);

      if (filtered.length === 0) {
        terminal.innerHTML = `<div class="empty-state">
          <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
            <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>
          </svg>
          ${lines.length === 0 ? 'Click Connect to open the serial port' : 'No lines match this filter'}
        </div>`;
        document.getElementById('lineCount').textContent = '0 lines';
        return;
      }

      const toShow = filtered.slice(-400);
      terminal.innerHTML = toShow.map(l => {
        const cls = l.type || 'normal';
        // Firmware-side truncation badge: see TRUNCATION_MARKER_RE (backend)
        // and the module docstring's "RESOLVED (diagnostically)" section.
        // This is purely informational - the underlying satellite data was
        // still captured in full and sent to the TinyGS MQTT network; only
        // this local console preview got cut short by the ESP32's own
        // fixed-size Serial buffer during a high-volume burst.
        const truncBadge = l.truncated
          ? `<span class="trunc-badge" title="Console preview cut short by the ESP32's own serial buffer during a high-volume burst - the full packet data was still captured and sent to the TinyGS network via MQTT. Nothing was actually lost.">⚠ preview truncated</span>`
          : '';
        // Lines from the WiFi console poller (see startWifiConsole) carry
        // source: 'wifi' (set server-side in log(), not inferred here) and
        // are also prefixed "[WiFi] " in their raw text - the prefix is
        // useful when reading the raw log file directly, but for display we
        // strip it and show a proper tag instead, driven by the authoritative
        // `source` field rather than guessing from the text itself.
        let bodyText = escapeHtml(l.text);
        let wifiTag = '';
        if (l.source === 'wifi') {
          wifiTag = '<span class="wifi-tag" title="From the WiFi console poller - reads the complete internal log buffer maintained by the firmware, never truncated">🌐 WiFi</span>';
          if (bodyText.startsWith('[WiFi] ')) bodyText = bodyText.slice('[WiFi] '.length);
        }
        return `<div class="line ${cls}"><span class="ts">${l.time}</span>${wifiTag}${bodyText}${truncBadge}</div>`;
      }).join('');
      document.getElementById('lineCount').textContent = filtered.length.toLocaleString() + ' lines';
    }

    function setFilter(f) {
      currentFilter = f;
      document.querySelectorAll('.filter-chip').forEach(c => {
        c.classList.toggle('active', 
          (f === 'all' && c.textContent === 'All') ||
          (f === 'json' && c.textContent === 'JSON Only') ||
          (f === 'tx' && c.textContent === 'TX Events') ||
          (f === 'rx' && c.textContent === 'RX Events') ||
          (f === 'error' && c.textContent === 'Errors')
        );
      });
      render();
    }

    function toggleConnect() {
      const port = document.getElementById('portSelect').value;
      const action = isConnected ? 'disconnect' : 'connect';
      btnConnect.disabled = true;  // re-enabled by the next state update from the server
      sendAction(action, {port: port === 'Auto-detect' ? null : port});
    }

    function sendAction(action, data={}) {
      if (ws && ws.readyState === 1) {
        ws.send(JSON.stringify({action, ...data}));
      }
    }

    function handleResult(r) {
      if (!r.ok) showToast('Error: ' + r.error);
      else if (r.port) showToast('Connected to ' + r.port);
    }

    function updatePortList(ports) {
      const sel = document.getElementById('portSelect');
      const current = sel.value;
      sel.innerHTML = '<option>Auto-detect</option>' + ports.map(p => `<option>${p}</option>`).join('');
      if (ports.includes(current)) sel.value = current;
    }

    function formatBytes(b) {
      if (b < 1024) return b + ' B';
      if (b < 1024*1024) return (b/1024).toFixed(1) + ' KB';
      return (b/1024/1024).toFixed(2) + ' MB';
    }

    function formatUptime(s) {
      const h = Math.floor(s/3600).toString().padStart(2,'0');
      const m = Math.floor((s%3600)/60).toString().padStart(2,'0');
      const sec = Math.floor(s%60).toString().padStart(2,'0');
      return `${h}:${m}:${sec}`;
    }

    function escapeHtml(t) {
      return t.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    // Ticks the UPTIME display once a second using uptimeAnchor (set in
    // updateStatusUI), so it behaves like an actual live timer instead of
    // only updating whenever a status/state message happens to arrive.
    setInterval(() => {
      if (uptimeAnchor !== null) {
        document.getElementById('mUptime').textContent = formatUptime((Date.now() - uptimeAnchor) / 1000);
      }
    }, 1000);

    connectWS();
  </script>
</body>
</html>'''
    html = html.replace('__GIT_VERSION__', GIT_VERSION)
    return web.Response(text=html, content_type='text/html')


async def on_startup(app):
    # Grab a handle to the running event loop so the background serial-reader
    # thread can safely hand broadcasts back to it (see log()/run_coroutine_threadsafe).
    server.loop = asyncio.get_event_loop()
    t = threading.Thread(target=server.serial_reader, daemon=True)
    t.start()

    # Auto-resume WiFi Console polling if it was active when the server last
    # stopped (restart/upgrade) - see the credential-persistence comment in
    # TinyGSServer.__init__. Entirely automatic; no browser interaction
    # needed. Only attempted if we actually have both an IP and password
    # saved (a corrupt/partial config file just means this silently doesn't
    # auto-resume, same as never having enabled it).
    if server._wifi_console_should_resume and server.wifi_console_ip and server.wifi_console_password:
        await server.start_wifi_console(server.wifi_console_ip, server.wifi_console_password)


app = web.Application()
app.router.add_get('/', index_handler)
app.router.add_get('/ws', websocket_handler)
app.router.add_get('/logs', logs_list_handler)
app.router.add_get('/logs/{filename}', logs_file_handler)
app.on_startup.append(on_startup)


if __name__ == '__main__':
    print("=" * 55)
    print("🛰️  TinyGS Dashboard Server")
    print(f"Version: {GIT_VERSION}")
    print("=" * 55)
    print("Open http://localhost:5000 in your browser")
    print("Logs saved to: " + LOG_DIR)
    print("Press Ctrl+C to stop")
    print("=" * 55)

    web.run_app(app, host='0.0.0.0', port=5000)
