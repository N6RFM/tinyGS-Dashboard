#!/usr/bin/env python3
"""
Parse telemetry frames (60-byte hex packets) out of one or more WiFi console
logs, label each with its UTC capture timestamp, and drop duplicate frames
(same payload, e.g. from a re-poll of an already-read buffer).

Usage:
    python parse_telemetry.py log1.log [log2.log ...]
"""

import re
import sys

UTC_RE = re.compile(r'^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)Z')
PACKET_HDR_RE = re.compile(r'\[WiFi\] \d{2}:\d{2}:\d{2} Packet \(\d+ bytes\):')
HEX_LINE_RE = re.compile(r'\] (?:\d{2}:\d{2}:\d{2} )?((?:[0-9A-F]{2} ){2,}[0-9A-F]{2})\s')


def get_utc(line: str):
    m = UTC_RE.search(line)
    return f"{m.group(1)}Z" if m else None


def parse_frames(lines):
    """Return a list of (utc_timestamp, hex_bytes) tuples, one per frame."""
    frames = []
    n = len(lines)
    i = 0
    first_header_line_idx = None

    while i < n:
        line = lines[i]
        if PACKET_HDR_RE.search(line):
            if first_header_line_idx is None:
                first_header_line_idx = i
            utc_ts = get_utc(line)
            j = i + 1
            if j < n and '---' in lines[j]:
                j += 1  # skip the opening dashed separator

            hexbytes = []
            while j < n and '---' not in lines[j]:
                m = HEX_LINE_RE.search(lines[j])
                if m:
                    hexbytes.extend(m.group(1).split())
                j += 1

            frames.append((utc_ts, hexbytes))
            i = j + 1
            continue
        i += 1

    # Handle a frame that appears at the very start of the log, before any
    # "Packet (N bytes):" header line has been seen (log truncated/started
    # mid-packet). Only run this if the main loop above didn't already find
    # a real header within that same window - otherwise this just re-grabs
    # part of an already-correctly-parsed frame as a spurious, incomplete
    # duplicate with the wrong timestamp (confirmed as a real bug: a log
    # starting with a normal, complete, properly-headed packet within the
    # first few lines produced an extra bogus 16-byte "frame" stamped with
    # an unrelated earlier line's timestamp).
    FALLBACK_WINDOW = 6
    # Cap the scan at whichever comes first: the fallback window, or the
    # first real header line - not a blanket "skip entirely if a header
    # exists anywhere in the window" (which was too coarse: a header can
    # fall within the window while its own hex data starts well after it,
    # leaving genuine pre-header hex lines that the fallback should still
    # legitimately capture as a separate, truncated earlier frame).
    scan_end = FALLBACK_WINDOW
    if first_header_line_idx is not None:
        scan_end = min(FALLBACK_WINDOW, first_header_line_idx)
    first_hex, first_utc = [], None
    for line in lines[:scan_end]:
        if first_utc is None:
            first_utc = get_utc(line)
        m = HEX_LINE_RE.search(line)
        if m:
            first_hex.extend(m.group(1).split())
    if first_hex:
        frames.insert(0, (first_utc, first_hex))

    return frames


def dedupe(frames):
    """Drop frames whose timestamp has already been seen; keep the first."""
    seen = {}
    for utc_ts, hexbytes in frames:
        if utc_ts not in seen:
            seen[utc_ts] = (utc_ts, hexbytes)
    # sort chronologically (None timestamps sort first)
    return sorted(seen.values(), key=lambda f: f[0] or '')


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <logfile> [logfile2 ...]")
        sys.exit(1)

    all_frames = []
    for path in sys.argv[1:]:
        with open(path, encoding='utf-8', errors='replace') as f:
            lines = f.read().split('\n')
        frames = parse_frames(lines)
        all_frames.extend(frames)
        print(f"# {path}: {len(frames)} frames", file=sys.stderr)

    unique_frames = dedupe(all_frames)

    for utc_ts, hexbytes in unique_frames:
        print(utc_ts)
        print(' '.join(hexbytes))
        print()

    dupes_removed = len(all_frames) - len(unique_frames)
    print(f"# {len(all_frames)} total, {dupes_removed} duplicate(s) removed, "
          f"{len(unique_frames)} unique frames", file=sys.stderr)


if __name__ == '__main__':
    main()
