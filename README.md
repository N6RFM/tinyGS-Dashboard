# 🛰️ TinyGS Serial Dashboard

*Code written by N6RFM with help from Claude AI*

A live web dashboard for monitoring TinyGS ESP32 ground station serial output on Linux Mint / Ubuntu-based systems.

## What it does

- Connects to your ESP32 via USB serial (auto-detects CP2102, CH340, FT232, native USB chips)
- Displays live LoRa frame data in a web browser, with color-coded terminal output
- Filters by event type (All / JSON / TX / RX / Errors)
- Saves **two** log files per session to disk (see "Log Files" below)
- Lets you browse and view saved log files right from the dashboard (📂 Logs button)
- Extracts and exports parsed JSON telemetry frames separately
- Works from any device on your network, not just the machine it's running on

## Quick Start

```bash
# 1. Get the code: either
#      git clone <this-repo-url> && cd tinyGS-Dashboard
#    or download/extract the zip and open a terminal in that folder
# 2. Run the installer
bash install.sh

# 3. LOG OUT and log back in (required for serial port permissions - the
#    installer adds you to the `dialout` group, which only takes effect
#    on your next login)

# 4. Plug in your ESP32

# 5. Start the server - NOTE: always run this from ~/tinyGS-Dashboard,
#    NOT from wherever you cloned/extracted the repo. See "Where things
#    live" below.
cd ~/tinyGS-Dashboard
source venv/bin/activate
python3 tinygs_server.py

# 6. Open http://localhost:5000 in Firefox/Chrome
```

## Where things live (read this if anything seems missing)

`install.sh` always installs to **`~/tinyGS-Dashboard`**, regardless of what
folder you cloned or extracted the code into or what you renamed it to. If
you cloned to, say, `~/TinyGS_Serial_Dashboard` and later can't find the
venv or the server won't start from that folder - that's why. Everything
(venv, logs, the server script) actually lives in `~/tinyGS-Dashboard`.
Always `cd` there before running anything manually.

## Auto-Start on Boot

```bash
sudo bash install_service.sh
```

Then access it anytime at `http://your-mint-ip:5000`. The service uses
`Restart=always`, meaning systemd will immediately relaunch the server if you
kill it manually. **If you want to run a one-off manual instance for
testing while the service is installed, stop the service first:**

```bash
sudo systemctl stop tinygs-dashboard
```
(it'll come back on next boot unless you also `sudo systemctl disable tinygs-dashboard`)

Running both at once causes "device disconnected or multiple access" errors,
since two processes end up fighting over the same serial port.

## Log Files

Every session creates **two** files in `~/tinyGS-Dashboard/logs/`, named with
a UTC timestamp:

| File | Contents |
|------|----------|
| `stream_YYYYMMDDTHHMMSSZ.log` | Processed, timestamped, human-readable log - one line per serial line received |
| `raw_YYYYMMDDTHHMMSSZ.bin` | The exact, unprocessed bytes as they came off the wire, written *before* any decoding or line-splitting touches them |

**Timestamp format inside `stream_*.log`:** each line is prefixed with
`[YYYY-MM-DD HH:MM:SS.mmmZ (±HH:MM local)]` - date and time in GMT/UTC (the
trailing `Z` marks it as UTC per ISO 8601 convention), followed by the local
UTC offset *at the moment that line was logged*. This makes log files
directly comparable across sessions/timezones without any guesswork, while
still letting you reconstruct local time by adding the offset algebraically
(e.g. `07:15 UTC` with `-07:00` → `00:15 local`). The offset is computed
fresh per line rather than once per file, so it stays correct even if a
session happens to span a DST transition. (The live browser terminal still
shows local time only, since that's more natural to read in real time while
watching a pass - only the persisted files use UTC.)

The raw file (`raw_*.bin`) is a byte-for-byte capture with **no timestamps
injected** - adding any text to it would break its purpose as a pure
ground-truth copy of the wire data. It exists specifically so you never need
a second program (like `picocom`) to get that ground truth - if you ever
suspect the dashboard is dropping or mangling data, compare it against the
processed `stream_*.log` for the same time window. If the raw file is
complete but the processed one isn't, the bug is in the dashboard's
processing. If the raw file is *also* incomplete, the data was already lost
before it even reached the dashboard (upstream - the ESP32 firmware or the
USB/serial transport itself), and no change to this codebase can fix that.

Exported JSON telemetry (via the "Export JSON" button) is saved separately as
`frames_YYYYMMDDTHHMMSSZ.json`, with each frame's `_receivedAt` field also in
UTC, and a `_source` field (`"serial"` or `"wifi"`) noting which connection
it came from - see "WiFi Console" below.

Browse and view any of these files without leaving the browser via the 📂
**Logs** button in the dashboard toolbar.

## File Layout

```
~/tinyGS-Dashboard/
├── tinygs_server.py      # Main server (single file: backend + inline HTML/JS/CSS frontend)
├── requirements.txt      # Python dependencies
├── venv/                 # Python virtual environment (created by install.sh, gitignored)
├── logs/                 # Auto-created (contents gitignored, folder itself tracked via .gitkeep)
│   ├── stream_YYYYMMDDTHHMMSSZ.log    # Processed serial log (GMT timestamps, local offset per line)
│   ├── raw_YYYYMMDDTHHMMSSZ.bin       # Raw unprocessed byte capture (ground truth, no timestamps)
│   └── frames_YYYYMMDDTHHMMSSZ.json   # Exported JSON telemetry frames
├── install.sh             # Installer (creates venv, installs deps, sets up dialout group)
├── install_service.sh     # Optional: installs as a systemd service for auto-start on boot
├── LICENSE                # MIT
└── .gitignore
```

## Dashboard Features

| Feature | Description |
|---------|-------------|
| **Connect/Disconnect** | One-click serial port control |
| **Auto-detect** | Finds CP2102, CH340, FT232, native USB ESP32s |
| **Live meters** | Frame count, bytes, JSON objects, uptime |
| **Filters** | View All / JSON / TX / RX / Errors only |
| **📂 Logs** | Browse and view any saved log file (processed or raw) without leaving the browser |
| **🌐 WiFi Console** | Polls the ESP32's own local web dashboard over WiFi for guaranteed-complete console text (bypasses the USB serial buffer limit entirely - see below) |
| **Export JSON** | Download all parsed telemetry frames as `.json` |
| **Clear** | Reset terminal and counters |
| **Dark theme** | Easy on the eyes for long monitoring sessions |

## WiFi Console

USB serial has a hard limitation: the ESP32's own internal Serial transmit
buffer is fixed-size (256 bytes by default), and a high-volume burst (a full
packet hex dump) can exceed it - when that happens, the firmware itself
gracefully truncates its console output with `...` (see "Known/Accepted
Behavior" below). **No actual satellite data is lost when this happens** -
the full packet is captured and sent to the TinyGS network via MQTT
regardless - but the local text preview can be incomplete.

The **🌐 WiFi Console** button polls your ESP32's own local web dashboard
(the same page you'd visit in a browser at its IP address) over WiFi,
reading its complete, untruncated internal log buffer directly - the same
data source that page's own live console uses. This is independent of the
USB serial connection; run either one alone, or both together.

To use it: find your ESP32's local IP (shown on its own web dashboard page,
or check your router), click **🌐 WiFi Console**, enter the IP and your
device's **admin/AP password** (the same one you use to log into its web
config panel), click **Start Polling**.

The password is required, not optional - confirmed directly from the
firmware source (`ConfigManager.cpp`: `handleRefreshConsole()` calls
`server.authenticate(...)` and rejects the request entirely if it fails).
Without it, every poll attempt is silently rejected by the device and no
data ever comes through - if you forget it, the WiFi Console panel will
show a 🟡 status with a specific `HTTP 401` error instead of the normal 🟢,
so it's visible rather than silently doing nothing.

**Telling sources apart:** lines from the WiFi poller are tagged with a
cyan **🌐 WiFi** marker in the terminal; USB serial lines have no tag. This
distinction also carries through to exported JSON telemetry - every frame
in `frames_*.json` has a `_source` field (`"serial"` or `"wifi"`) so you can
tell, even after the fact, which connection a given decoded packet came
through.

**Credentials persist across restarts.** Once you've entered your IP and
password successfully, they're saved to `~/tinygs-dashboard/config.json`
(permissions locked to owner-only) - a server restart or `git pull` +
upgrade won't make you re-enter them. If WiFi Console was actively polling
when the server last stopped (a crash or restart, not a deliberate "Stop
Polling" click), it **automatically resumes on the next startup**, with no
browser interaction needed at all.

**Security note:** the password is stored in plaintext on disk, protected
only by OS file permissions (`chmod 600`), not encrypted. This is a
deliberate convenience tradeoff intended for a personal, single-user
machine. If that doesn't describe your setup, either don't use this feature
(re-enter the password each session instead) or delete/edit
`~/tinygs-dashboard/config.json` directly.

## Version Display

The page header shows a small tag with the currently-running server code's
git commit hash (e.g. `a1b2c3d`, or `a1b2c3d-dirty` if there are uncommitted
local changes). Also printed in the server's startup console banner. Use
this to confirm a `git pull` + restart actually picked up your latest
changes - "unknown" means the code isn't running from a git checkout (e.g.
you're running from an extracted zip rather than a clone).

## Known/Accepted Behavior

**The board resets when you click Connect.** On this hardware, opening the
serial port triggers a brief ESP32 reset (`ESP-ROM:esp32s3-...`, `rst:0x1
(POWERON)` boot spam) even with DTR/RTS explicitly held low before opening.
This is a known ESP32 dev-board auto-reset-circuit behavior and doesn't
appear to be fixable purely from the Python/pyserial side - it would need
either a hardware change (some boards have a jumper/resistor to disconnect
EN from the USB-serial chip's control lines) or firmware changes. If it
doesn't bother your workflow, no action needed; this has been left as-is by
design rather than continuing to chase a fix with diminishing returns.

**Long hex-dump lines sometimes show `...` or are missing entirely, and a
"⚠ preview truncated" badge appears next to them.** This is confirmed,
directly from the TinyGS firmware source, to be the ESP32 itself gracefully
truncating its own serial console output when a burst of data (a full
packet dump) doesn't fit in its fixed-size internal Serial buffer at the
current baud rate - see `Log::AddLog()` in the firmware's `Logger.cpp`. It
is **not** a bug in this dashboard, and it is **not** fixable from this
codebase - none of DTR/RTS handling, serial port settings, or read-loop
speed can influence it, since it's governed purely by the ESP32's own
internal buffer state, not by anything the receiving side does.

**Most importantly: no actual satellite data is lost.** The truncation only
affects the human-readable console preview. The full, untruncated packet
bytes are captured and uploaded to the TinyGS network via MQTT completely
independently of this console-printing code path - your station's data on
tinygs.com is complete regardless of what the local serial console shows.

## Troubleshooting

**"No serial ports found"**
- Make sure ESP32 is plugged in
- Check: `ls /dev/ttyUSB* /dev/ttyACM*`
- Run: `sudo usermod -a -G dialout $USER` then log out/in

**"Permission denied" on port**
- Same fix: add to `dialout` group, log out/in

**venv missing / "No such file or directory: venv/bin/activate"**
- You're almost certainly not in `~/tinyGS-Dashboard`. See "Where things live" above.

**"address already in use" on port 5000**
- Something's already listening. Find it: `sudo lsof -i :5000`
- If it's a leftover process: `kill <pid>` (or `kill -9 <pid>` if it won't die)
- If it's the systemd service auto-restarting what you just killed: `sudo systemctl stop tinygs-dashboard`

**"device disconnected or multiple access on port" / connects then immediately errors**
- Something else has the port open. Check: `sudo fuser -v /dev/ttyUSB2` (use your actual port)
- Common culprits found during development on Linux Mint:
  - **ModemManager** auto-probes new USB-serial devices thinking they might be
    cellular modems. Check `systemctl status ModemManager`; if active, add a
    udev rule to make it ignore your board's specific USB vendor/product ID
    (`lsusb` to find it) rather than disabling ModemManager system-wide:
    ```
    # /etc/udev/rules.d/99-mm-ignore-esp32.rules
    ATTRS{idVendor}=="10c4", ATTRS{idProduct}=="ea60", ENV{ID_MM_DEVICE_IGNORE}="1"
    ```
    (the vendor/product shown is for the Silicon Labs CP210x chip - substitute
    your board's actual IDs from `lsusb`), then:
    ```
    sudo udevadm control --reload-rules && sudo udevadm trigger
    ```
    and unplug/replug the board.
  - **brltty** (Braille display support, enabled by default on Mint/Ubuntu)
    similarly auto-probes USB-serial devices. Check `systemctl status brltty`;
    if active and you don't use a Braille display: `sudo systemctl disable brltty`
    (or mask `brltty-udev.service` if it keeps re-triggering via udev).
  - **A leftover terminal program** (`picocom`, `minicom`, Arduino IDE's Serial
    Monitor, PlatformIO's monitor) left open on the same port. `fuser -v` will
    show you the PID; close that program before connecting from the dashboard.
  - **The systemd service AND a manual instance both running** - see the
    "Auto-Start on Boot" section above.

**Port keeps disconnecting (unrelated to the above)**
- Check USB cable (some cables are power-only, no data lines)
- Try a different USB port (avoid low-power hubs/front-panel headers)

**Data looks truncated / incomplete in the log**
- See "Known/Accepted Behavior" above - this is expected ESP32 firmware
  behavior, not data loss. The `raw_*.bin` capture (see "Log Files" above)
  will show the same truncation, since it's present in the bytes the
  firmware actually sent, not introduced by this dashboard.

**Service won't start**
- Check: `sudo journalctl -u tinygs-dashboard -f`
- Make sure venv exists: `ls ~/tinyGS-Dashboard/venv/bin/python`

## Requirements

- Linux Mint (or any Debian/Ubuntu-based distro)
- Python 3.8+
- ESP32 with TinyGS firmware
- USB cable (data-capable, not power-only)

## License

MIT — see [LICENSE](LICENSE). Hack away.
