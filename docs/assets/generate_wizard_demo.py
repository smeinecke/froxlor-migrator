#!/usr/bin/env python3
"""Generate an animated terminal-style SVG showing the froxlor-migrator wizard.

Pure SMIL (no CSS animations) so it runs identically inside <img> embeds
(GitHub README) and can be verified via chrome --virtual-time-budget.

Regenerate:  uv run docs/assets/generate_wizard_demo.py
"""

from pathlib import Path

W, H = 780, 500
R = 10
TITLEBAR = 34
HEADER = 26
FOOTER = 24
LOOP = 28.0  # seconds
FADE = 0.3   # seconds

BG = "#0d1117"
PANEL = "#161b22"
BORDER = "#30363d"
TEXT = "#c9d1d9"
BRIGHT = "#e6edf3"
MUTED = "#8b949e"
BLUE = "#58a6ff"
BLUE_BG = "#1f6feb"
GREEN = "#3fb950"
GREEN_BG = "#238636"
YELLOW = "#d29922"
CYAN = "#39c5cf"

FONT = "ui-monospace,'Cascadia Code','JetBrains Mono',Menlo,Consolas,monospace"
FS = 13
LH = 21
X0 = 22
TOP = TITLEBAR + HEADER + 14


def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def kt(s: float) -> float:
    """seconds -> keyTimes fraction of the loop."""
    return s / LOOP


def fade(values_times: list[tuple[float, float]]) -> str:
    """SMIL opacity animate over the whole loop.

    values_times: ordered (keyTime_seconds, opacity) pairs covering 0..LOOP.
    """
    values = ";".join(f"{o:g}" for _s, o in values_times)
    times = ";".join(f"{s / LOOP:.5f}" for s, _o in values_times)
    return (f'<animate attributeName="opacity" dur="{LOOP}s" repeatCount="indefinite"'
            f' calcMode="linear" values="{values}" keyTimes="{times}"/>')


def appear(at: float, rise: float = 0.35) -> str:
    """Fade in at `at` seconds, then stay visible."""
    return fade([(0, 0), (at, 0), (at + rise, 1), (LOOP, 1)])


def t(x, row, s, fill=TEXT, weight="", anchor=""):
    y = TOP + row * LH
    w = f' font-weight="{weight}"' if weight else ""
    a = f' text-anchor="{anchor}"' if anchor else ""
    return f'<text x="{x}" y="{y}" fill="{fill}" font-size="{FS}"{w}{a}>{esc(s)}</text>'


def rect(x, y, w, h, fill, rx=0, stroke=""):
    s = f' stroke="{stroke}"' if stroke else ""
    r = f' rx="{rx}"' if rx else ""
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="{fill}"{r}{s}/>'


def button(cx, row, label, kind="normal"):
    y = TOP + row * LH - 15
    w = len(label) * 7.8 + 24
    x = cx - w / 2
    if kind == "primary":
        bg, fg, st = BLUE_BG, "#ffffff", ""
    elif kind == "success":
        bg, fg, st = GREEN_BG, "#ffffff", ""
    else:
        bg, fg, st = "#21262d", TEXT, BORDER
    s = f' stroke="{st}"' if st else ""
    return (f'<rect x="{x:.1f}" y="{y}" width="{w:.1f}" height="26" rx="6" fill="{bg}"{s}/>'
            f'<text x="{cx}" y="{y + 17}" fill="{fg}" font-size="{FS}" text-anchor="middle">{esc(label)}</text>')


def hl_row(row, w=720):
    return rect(X0 - 8, TOP + row * LH - 15, w, 21, "rgba(56,139,253,0.18)", rx=4)


def tab(cx, row, label, active=False):
    y = TOP + row * LH - 15
    w = len(label) * 7.8 + 20
    x = cx - w / 2
    fill = "#1f6feb33" if active else "#21262d"
    fg = BRIGHT if active else MUTED
    st = BLUE if active else BORDER
    return (rect(x, y, w, 24, fill, rx=6, stroke=st)
            + f'<text x="{cx}" y="{y + 16}" fill="{fg}" font-size="12" text-anchor="middle">{esc(label)}</text>')


# ---------------------------------------------------------------- frames ----
frames: list[tuple[float, float, str]] = []


def frame(start, end, body):
    frames.append((start, end, "".join(body)))


# F1: command + connect ---------------------------------------------------------
CMD = "$ uv run python main.py --config config.toml"
f1 = [t(X0, 0, CMD, BRIGHT)]

type_start, type_end = 0.4, 1.8
cursor_end = X0 + 2 + len(CMD) * 7.8 + 6
cover_w = len(CMD) * 7.8 + 4
slide = (f'values="{{v}};{{v}};{cursor_end:.0f};{cursor_end:.0f}"'
         f' keyTimes="0;{kt(type_start):.5f};{kt(type_end):.5f};1"')
# cover rect: bg-colored block sliding right, revealing the typed command
f1.append(
    f'<rect x="{X0 + 2}" y="{TOP - 14}" width="{cover_w:.0f}" height="18" fill="{BG}">'
    + f'<animate attributeName="x" dur="{LOOP}s" repeatCount="indefinite" calcMode="linear" '
    + slide.format(v=X0 + 2) + "/>"
    + f'<animate attributeName="width" dur="{LOOP}s" repeatCount="indefinite" calcMode="linear"'
    f' values="{cover_w:.0f};{cover_w:.0f};0;0"'
    f' keyTimes="0;{kt(type_start):.5f};{kt(type_end):.5f};1"/></rect>')
# green cursor travelling at the reveal edge, then blinking in place
f1.append(
    f'<rect x="{X0 + 2}" y="{TOP - 13}" width="8" height="15" fill="{GREEN}">'
    + f'<animate attributeName="x" dur="{LOOP}s" repeatCount="indefinite" calcMode="linear" '
    + slide.format(v=X0 + 2) + "/>"
    + '<animate attributeName="opacity" dur="1s" repeatCount="indefinite"'
    ' values="1;1;0;0" keyTimes="0;0.5;0.5;1"/></rect>')
f1.append("<g>" + appear(2.1)
          + t(X0, 2, "Froxlor Migrator", BRIGHT, "bold")
          + t(X0, 3, "Connecting to source and target panels…", MUTED)
          + "</g>")
f1.append("<g>" + appear(2.9)
          + t(X0, 5, "✓ source api ok", GREEN)
          + t(X0 + 170, 5, "✓ target api ok", GREEN)
          + t(X0 + 340, 5, "✓ 3 customers loaded", GREEN)
          + "</g>")
frame(0.0, 3.4, f1)

# F2: Step 1 - customers ---------------------------------------------------------
f2 = [
    t(X0, 0, "Step 1 — Source customer(s)", BRIGHT, "bold"),
    t(X0, 1, "Select customers to migrate — multiple selections run as a sequential batch.", MUTED),
    hl_row(3),
    t(X0, 3, "▸", BLUE) + t(X0 + 18, 3, "[x] custalpha — Alpha GmbH", BRIGHT) + t(560, 3, "(id 10001)", MUTED),
    t(X0 + 18, 4, "[ ] custbeta  — Beta Solutions", TEXT) + t(560, 4, "(id 10002)", MUTED),
    t(X0 + 18, 5, "[ ] custgamma — Gamma Shop", TEXT) + t(560, 5, "(id 10003)", MUTED),
    button(300, 8, "Next", "primary") + button(420, 8, "Quit"),
]
frame(3.4, 6.4, f2)

# F3: Step 2 - mode ---------------------------------------------------------------
f3 = [
    t(X0, 0, "Step 2 — Migration mode", BRIGHT, "bold"),
    t(X0, 1, "Whole customer migrates every resource; domain-only lets you pick.", MUTED),
    t(X0, 3, "(•)", BLUE) + t(X0 + 30, 3, "Whole customer (all domains, files, databases, mailboxes, settings)", BRIGHT),
    t(X0, 4, "( )", MUTED) + t(X0 + 30, 4, "Domain-only (choose individual resources)", TEXT),
    button(300, 8, "Next", "primary") + button(420, 8, "Back"),
]
frame(6.4, 9.0, f3)

# F4: Step 3 - domains -------------------------------------------------------------
f4 = [
    t(X0, 0, "Step 3 — Domains", BRIGHT, "bold"),
    t(X0, 1, "Select domains to migrate. Domains outside source_web_root are skipped.", MUTED),
    t(X0, 3, "[x] alpha.example", BRIGHT) + t(290, 3, "/var/customers/webs/alpha", MUTED) + t(590, 3, "php=8.3 ssl=1", MUTED),
    hl_row(4),
    t(X0, 4, "[x] shop.alpha.example", BRIGHT) + t(290, 4, "/var/customers/webs/shop", MUTED) + t(590, 4, "php=8.3 ssl=1", MUTED),
    t(X0, 5, "[ ] old.example.net", MUTED) + t(290, 5, "/srv/old (outside web root — skipped)", MUTED),
    button(300, 8, "Next", "primary") + button(420, 8, "Back"),
]
frame(9.0, 11.6, f4)

# F5: Step 4 - resources (tabs) ------------------------------------------------------
f5 = [
    t(X0, 0, "Step 4 — Resources", BRIGHT, "bold"),
    t(X0, 1, "Mail forwarders, sender aliases and SSH keys follow selections automatically.", MUTED),
    tab(150, 3, "Subdomains") + tab(272, 3, "Databases", active=True) + tab(382, 3, "Mailboxes") + tab(504, 3, "FTP accounts"),
    t(X0, 5, "[x] custalpha_wpdemo", BRIGHT) + t(320, 5, "WordPress demo", MUTED) + t(560, 5, "(mysql1)", MUTED),
    hl_row(6),
    t(X0, 6, "[ ] custalpha_old", TEXT) + t(320, 6, "Legacy archive", MUTED) + t(560, 6, "(mysql1)", MUTED),
    button(300, 9, "Next", "primary") + button(420, 9, "Back"),
]
frame(11.6, 14.2, f5)


# F5b: Step 5 - PHP & IP mappings -------------------------------------------------------
def select_box(row, label, w=460):
    y = TOP + row * LH - 15
    return (rect(X0 + 20, y, w, 24, "#21262d", rx=6, stroke=BORDER)
            + t(X0 + 32, row, label, BRIGHT)
            + t(X0 + 20 + w - 24, row, "▼", MUTED))


f5b = [
    t(X0, 0, "Step 5 — PHP & IP mappings", BRIGHT, "bold"),
    t(X0, 1, "Map source PHP settings and IP:port bindings to target IDs.", MUTED),
    t(X0, 3, "Source PHP setting id 3 ->", TEXT),
    select_box(4, "fpm 8.3 (/usr/bin/php8.3)"),
    t(X0, 6, "Source IP 192.0.2.10:80 ssl=1 (id 5) ->", TEXT),
    select_box(7, "198.51.100.20:80 ssl=1"),
    button(300, 10, "Next", "primary") + button(420, 10, "Back"),
]
frame(14.2, 16.8, f5b)

# F6: Step 6 - options ----------------------------------------------------------------
opts = [
    ("x", "Transfer website files (docroots via tar)"),
    ("x", "Transfer database schema + data"),
    ("x", "Transfer mailbox content via doveadm backup"),
    ("x", "Sync password hashes (customer, FTP, mailbox, htpasswd, DB)"),
    (" ", "Dry run (plan only, no changes)"),
]
f6 = [
    t(X0, 0, "Step 6 — Options", BRIGHT, "bold"),
    t(X0, 1, "Choose what gets transferred. Unchecked items are skipped entirely.", MUTED),
]
for i, (mark, label) in enumerate(opts):
    f6.append(t(X0, 3 + i, f"[{mark}] {label}", BRIGHT if mark == "x" else MUTED))
f6.append(button(300, 9, "Next", "primary") + button(420, 9, "Back"))
frame(16.8, 19.2, f6)

# F7: Step 7 - review -------------------------------------------------------------------
f7 = [
    t(X0, 0, "Step 7 — Review & start", BRIGHT, "bold"),
    t(X0, 1, "Verify the plan, then start the migration.", MUTED),
    t(X0, 3, "Item", MUTED) + t(220, 3, "Value", MUTED),
    rect(X0 - 6, TOP + 3 * LH - 6, 700, 1, BORDER),
    t(X0, 4, "Customer", TEXT) + t(220, 4, "custalpha (id 10001) → new customer", BRIGHT),
    t(X0, 5, "Domains", TEXT) + t(220, 5, "2 (+1 outside web root, skipped)", BRIGHT),
    t(X0, 6, "Resources", TEXT) + t(220, 6, "1 db · 4 mailboxes · 2 ftp · files+mail", BRIGHT),
    t(X0, 8, "Replay:", MUTED),
    t(X0 + 70, 8, "main.py --non-interactive --apply --source-customer custalpha …", CYAN),
    button(320, 10, "Start migration", "success") + button(470, 10, "Back"),
]
frame(19.2, 21.8, f7)

# F8: running -----------------------------------------------------------------------------
BAR_X, BAR_Y, BAR_W, BAR_H = X0, TOP + 2 * LH - 14, 620, 18
f8 = [
    t(X0, 0, "Running migration…", BRIGHT, "bold"),
    t(X0, 1, "DRY RUN — no changes will be written", YELLOW),
    rect(BAR_X, BAR_Y, BAR_W, BAR_H, "#21262d", rx=9, stroke=BORDER),
]
p_start, p_end = 22.0, 25.2
f8.append(
    f'<rect x="{BAR_X + 2}" y="{BAR_Y + 2}" width="60" height="{BAR_H - 4}" rx="7" fill="{BLUE_BG}">'
    f'<animate attributeName="width" dur="{LOOP}s" repeatCount="indefinite" calcMode="linear"'
    f' values="60;60;{BAR_W - 8};60" keyTimes="0;{kt(p_start):.5f};{kt(p_end):.5f};1"/></rect>')
for tt, pct_txt in [(22.3, "38%"), (23.4, "71%"), (24.8, "100%")]:
    f8.append(f'<text x="{BAR_X + BAR_W + 16}" y="{BAR_Y + 14}" fill="{MUTED}" font-size="{FS}">'
              + appear(tt) + pct_txt + "</text>")
log_lines = [
    (21.9, "[1/9] create customer on target", MUTED),
    (22.5, "[3/9] domains + ssl settings synced", MUTED),
    (23.2, "[5/9] files transferred (1.2 GiB via pzstd)", MUTED),
    (23.9, "[7/9] mailboxes synced via doveadm", MUTED),
    (24.6, "[9/9] password hashes synced", GREEN),
]
for i, (tt, line, col) in enumerate(log_lines):
    f8.append(f'<text x="{X0}" y="{TOP + (4 + i) * LH}" fill="{col}" font-size="{FS}">'
              + appear(tt, 0.25) + esc(line) + "</text>")
frame(21.8, 25.6, f8)

# F9: done --------------------------------------------------------------------------------
f9 = [
    t(X0, 0, "Migration completed", GREEN, "bold"),
    t(X0, 2, "Target customer id:", TEXT) + t(200, 2, "42", BRIGHT),
    t(X0, 4, "Source DB", MUTED) + t(300, 4, "Target DB", MUTED),
    rect(X0 - 6, TOP + 4 * LH - 6, 540, 1, BORDER),
    t(X0, 5, "custalpha_wpdemo", TEXT) + t(300, 5, "custalpha_wpdemo", BRIGHT),
    t(X0, 7, "Manifest:", TEXT) + t(100, 7, "output/manifests/custalpha-20261007-104210.json", CYAN),
    t(X0, 9, "Run `froxlor-migrator-verify` to check target parity.", MUTED),
    button(330, 11, "Migrate another customer", "primary") + button(490, 11, "Quit"),
]
frame(25.6, 28.0, f9)


# ---------------------------------------------------------------- svg ---------
def frame_fade(i: int, start: float, end: float) -> str:
    n = len(frames)
    if i == 0:
        pts = [(0, 1), (end - FADE, 1), (end, 0), (LOOP - FADE, 0), (LOOP, 1)]
    elif i == n - 1:
        pts = [(0, 0), (start, 0), (start + FADE, 1), (LOOP - FADE, 1), (LOOP, 0)]
    else:
        pts = [(0, 0), (start, 0), (start + FADE, 1), (end - FADE, 1), (end, 0), (LOOP, 0)]
    return fade(pts)


svg = [f'''<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}"
  font-family="{FONT}" role="img" aria-label="Animated demo of the froxlor-migrator Textual wizard">
<title>froxlor-migrator wizard demo</title>''']

# window
svg.append(rect(1, 1, W - 2, H - 2, BG, rx=R, stroke=BORDER))
# title bar (rounded top, square bottom)
svg.append(rect(1, 1, W - 2, TITLEBAR, PANEL, rx=R))
svg.append(rect(1, TITLEBAR - R + 2, W - 2, R - 1, PANEL))
for i, c in enumerate(("#ff5f57", "#febc2e", "#28c840")):
    svg.append(f'<circle cx="{22 + i * 20}" cy="{TITLEBAR / 2 + 1}" r="6" fill="{c}"/>')
svg.append(f'<text x="{W / 2}" y="{TITLEBAR / 2 + 5}" fill="{MUTED}" font-size="12" text-anchor="middle">'
           'froxlor-migrator - zsh</text>')

# textual-ish header strip
svg.append(rect(1, TITLEBAR, W - 2, HEADER, "#1a2332"))
svg.append(f'<text x="{W / 2}" y="{TITLEBAR + 17}" fill="{BRIGHT}" font-size="13" font-weight="bold" text-anchor="middle">'
           'Froxlor Migrator</text>')
svg.append(f'<text x="{W - 16}" y="{TITLEBAR + 17}" fill="{MUTED}" font-size="12" text-anchor="end">10:42</text>')

# footer strip
svg.append(rect(1, H - FOOTER - 1, W - 2, FOOTER, PANEL))
svg.append(f'<text x="16" y="{H - 8}" fill="{MUTED}" font-size="11">esc Back</text>')
svg.append(f'<text x="{W - 16}" y="{H - 8}" fill="{MUTED}" font-size="11" text-anchor="end">^Q Quit</text>')

# frames
for i, (s, e, body) in enumerate(frames):
    svg.append(f'<g opacity="0">{frame_fade(i, s, e)}{body}</g>')

svg.append("</svg>")

out = "\n".join(svg)
target = Path(__file__).with_name("wizard-demo.svg")
target.write_text(out)
print(f"wrote {target} ({len(out)} bytes)")
