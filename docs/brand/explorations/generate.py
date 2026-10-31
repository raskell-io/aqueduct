"""Generates explorations/index.html: candidate marks for Aqueduct, side by side."""
import math
import pathlib

HERE = pathlib.Path(__file__).resolve().parent


def semicircle_cut(cx, base_y, r):
    """A half-disc opening upward from a base line (an arch cut), as a subpath."""
    return f"M{cx - r:.2f} {base_y:.2f} A{r:.2f} {r:.2f} 0 0 1 {cx + r:.2f} {base_y:.2f} Z"


def concept_span():
    # One arch, one span: the essential gesture. A heavy arch carries a channel of light.
    return """
    <rect x="12" y="15" width="76" height="11" rx="5.5" fill="url(#water)"/>
    <path d="M20 88 V58 A30 30 0 0 1 80 58 V88" fill="none" stroke="currentColor" stroke-width="15"/>"""


def concept_tiers():
    # Three bars, cut by 4, 2 and 1 arches from the top down: the aqueduct as a fractal glyph.
    parts = []
    rows = [(24, 37, 4), (41, 60, 2), (64, 90, 1)]  # (top, bottom, arches)
    left, right = 12, 88
    for top, bottom, n in rows:
        d = f"M{left} {top} H{right} V{bottom} H{left} Z"
        bay = (right - left) / n
        r = min(bay * 0.34, (bottom - top) - 4)
        for i in range(n):
            cx = left + bay * (i + 0.5)
            d += " " + semicircle_cut(cx, bottom, r)
        parts.append(f'<path d="{d}" fill="currentColor" fill-rule="evenodd"/>')
    parts.append('<rect x="12" y="13" width="76" height="7" rx="3.5" fill="url(#water)"/>')
    return "\n    ".join(parts)


def concept_monogram():
    # An A built from an arch, crossed by a line of water that runs past it.
    return """
    <path d="M26 90 V46 A24 24 0 0 1 74 46 V90" fill="none" stroke="currentColor" stroke-width="14"/>
    <rect x="8" y="58" width="84" height="10" rx="5" fill="url(#water)"/>"""


def concept_keystone():
    # A ring of voussoirs; the keystone at the crown is water.
    parts = []
    cx, cy, ro, ri, n, gap = 50, 70, 40, 24, 7, 2.2
    for i in range(n):
        a0 = math.pi - (i / n) * math.pi
        a1 = math.pi - ((i + 1) / n) * math.pi
        g = math.radians(gap)
        a0 -= g / 2 if i > 0 else 0
        a1 += g / 2 if i < n - 1 else 0
        pts = [
            (cx + ro * math.cos(a0), cy - ro * math.sin(a0)),
            (cx + ro * math.cos(a1), cy - ro * math.sin(a1)),
            (cx + ri * math.cos(a1), cy - ri * math.sin(a1)),
            (cx + ri * math.cos(a0), cy - ri * math.sin(a0)),
        ]
        d = (
            f"M{pts[0][0]:.2f} {pts[0][1]:.2f} A{ro} {ro} 0 0 1 {pts[1][0]:.2f} {pts[1][1]:.2f} "
            f"L{pts[2][0]:.2f} {pts[2][1]:.2f} A{ri} {ri} 0 0 0 {pts[3][0]:.2f} {pts[3][1]:.2f} Z"
        )
        fill = "url(#water)" if i == n // 2 else "currentColor"
        parts.append(f'<path d="{d}" fill="{fill}"/>')
    parts.append('<rect x="10" y="70" width="16" height="18" fill="currentColor"/>')
    parts.append('<rect x="74" y="70" width="16" height="18" fill="currentColor"/>')
    return "\n    ".join(parts)


def concept_fractal():
    # An arch holding two arches, each holding two: recursion you can see at any size.
    parts = []

    def arch(x0, x1, base, depth):
        r = (x1 - x0) / 2
        cx = (x0 + x1) / 2
        w = [8.0, 5.0, 3.2][depth]
        inset = w / 2
        parts.append(
            f'<path d="M{x0 + inset:.2f} {base} A{r - inset:.2f} {r - inset:.2f} 0 0 1 {x1 - inset:.2f} {base}" '
            f'fill="none" stroke="currentColor" stroke-width="{w:.2f}"/>'
        )
        if depth < 2:
            mid = (x0 + x1) / 2
            arch(x0, mid, base, depth + 1)
            arch(mid, x1, base, depth + 1)

    arch(12, 88, 78, 0)
    parts.append('<rect x="10" y="80" width="80" height="7" rx="3.5" fill="url(#water)"/>')
    return "\n    ".join(parts)


def concept_channel():
    # The essay's title as two lines: the canal straight above, the river free below, an arch between.
    return """
    <rect x="10" y="22" width="80" height="12" rx="6" fill="url(#water)"/>
    <path d="M22 76 V60 A28 28 0 0 1 78 60 V76" fill="none" stroke="currentColor" stroke-width="10"/>
    <path d="M8 84 Q18 76 28 84 T48 84 T68 84 T92 84" fill="none" stroke="url(#river)" stroke-width="6" stroke-linecap="round"/>"""


def concept_grid():
    # The aqueduct on a pixel grid: modular, technical, unmistakable at 16 px.
    on = [
        "1111111",
        "1010101",
        "1111111",
        "1001001",
        "1001001",
        "1001001",
    ]
    parts = []
    size, gap, y0 = 11, 1.6, 24
    x0 = (100 - (7 * size + 6 * gap)) / 2
    for r, row in enumerate(on):
        for c, bit in enumerate(row):
            if bit == "1":
                parts.append(
                    f'<rect x="{x0 + c * (size + gap):.2f}" y="{y0 + r * (size + gap):.2f}" width="{size}" height="{size}" rx="2.4" fill="currentColor"/>'
                )
    parts.append(f'<rect x="{x0:.2f}" y="10" width="{7 * size + 6 * gap:.2f}" height="8" rx="2.4" fill="url(#water)"/>')
    return "\n    ".join(parts)


def concept_portal():
    # A solid tile with an arch cut through it, and light running across its top: an app-icon gesture.
    return """
    <path d="M14 30 H86 V90 H66 V64 A16 16 0 0 0 34 64 V90 H14 Z" fill="currentColor"/>
    <rect x="14" y="14" width="72" height="10" rx="3" fill="url(#water)"/>"""


CONCEPTS = [
    ("A", "Span", concept_span,
     "One arch, one channel. The essential gesture of the brand reduced to two strokes: a structure that carries, and what it carries. Calm and confident; reads at any size."),
    ("B", "Tiers", concept_tiers,
     "Three bars cut by four, two and one arches: the Pont du Gard as a glyph, and the fractal idea (each arch carries two) made geometric. The most distinctive; the most 'Aqueduct'."),
    ("C", "Monogram", concept_monogram,
     "An A built from an arch, crossed by a line of water that runs beyond it. A letter you can stamp on anything; the water line doubles as the A's crossbar."),
    ("D", "Keystone", concept_keystone,
     "A ring of voussoirs whose keystone is water. The stone that locks the arch is the thing it carries. Architectural and quiet."),
    ("E", "Fractal", concept_fractal,
     "An arch holding two arches, each holding two: recursion drawn as line. The most conceptual; best large, softer small."),
    ("F", "River and canal", concept_channel,
     "The essay's title as a picture: a straight channel above, a free river below, one arch between them."),
    ("G", "Grid", concept_grid,
     "The aqueduct on a pixel grid: modular and technical, built for 16 px, and easy to animate cell by cell."),
    ("H", "Portal", concept_portal,
     "A solid block with an arch cut through it and light along its top: the arch as negative space, like an app icon."),
]

PALETTES = {
    "night": {"tile": "#0b0f14", "fg": "#f2efe8", "w1": "#5ee7df", "w2": "#3b82f6", "r1": "#3b82f6", "r2": "#1d4ed8"},
}


def defs(prefix, w1, w2, r1, r2):
    return f"""<defs>
      <linearGradient id="{prefix}water" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="{w1}"/><stop offset="1" stop-color="{w2}"/></linearGradient>
      <linearGradient id="{prefix}river" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="{r1}"/><stop offset="1" stop-color="{r2}"/></linearGradient>
    </defs>"""


def svg(body, fg, prefix, size, tile=None, mono=False, w1="#5ee7df", w2="#3b82f6"):
    body = body.replace("url(#water)", fg if mono else f"url(#{prefix}water)").replace(
        "url(#river)", fg if mono else f"url(#{prefix}river)"
    )
    bg = f'<rect x="-14" y="-14" width="128" height="128" rx="28" fill="{tile}"/>' if tile else ""
    vb = "-14 -14 128 128" if tile else "4 4 92 92"
    return (
        f'<svg viewBox="{vb}" width="{size}" height="{size}" style="color:{fg}" xmlns="http://www.w3.org/2000/svg">'
        f"{defs(prefix, w1, w2, '#3b82f6', '#1d4ed8')}{bg}{body}</svg>"
    )


def card(letter, name, fn, text, n):
    body = fn()
    p = lambda k: f"c{n}{k}"
    icon = svg(body, "#f2efe8", p("i"), 168, tile="#0b0f14")
    icon_light_tile = svg(body, "#0b0f14", p("l"), 168, tile="#f4f1ea")
    on_light = svg(body, "#0b0f14", p("a"), 120)
    on_dark = svg(body, "#f2efe8", p("b"), 120)
    mono = svg(body, "#0b0f14", p("m"), 120, mono=True)
    smalls = "".join(
        f'<div class="small">{svg(body, "#f2efe8", p(f"s{s}"), s, tile="#0b0f14")}<span>{s}</span></div>' for s in (48, 32, 16)
    )
    lockup_sans = f'<div class="lockup light">{svg(body, "#0b0f14", p("u"), 56)}<span class="ws">aqueduct</span></div>'
    lockup_mono = f'<div class="lockup dark">{svg(body, "#f2efe8", p("v"), 56)}<span class="wm">AQUEDUCT</span></div>'
    return f"""
  <section class="card">
    <header><span class="letter">{letter}</span><h2>{name}</h2></header>
    <p>{text}</p>
    <div class="row">
      <figure>{icon}<figcaption>icon, dark tile</figcaption></figure>
      <figure>{icon_light_tile}<figcaption>icon, light tile</figcaption></figure>
      <figure class="paper">{on_light}<figcaption>on light</figcaption></figure>
      <figure class="ink">{on_dark}<figcaption>on dark</figcaption></figure>
      <figure class="paper">{mono}<figcaption>one colour</figcaption></figure>
      <figure class="smalls">{smalls}<figcaption>favicon sizes</figcaption></figure>
    </div>
    <div class="row">{lockup_sans}{lockup_mono}</div>
  </section>"""


def main():
    cards = "".join(card(l, n, f, t, i) for i, (l, n, f, t) in enumerate(CONCEPTS))
    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Aqueduct: mark explorations</title>
<style>
  @font-face {{ font-family: Geist; src: url(Geist-Variable.woff2) format("woff2"); font-weight: 100 900; }}
  @font-face {{ font-family: "Geist Mono"; src: url(GeistMono-Variable.woff2) format("woff2"); font-weight: 100 900; }}
  :root {{ --ink: #0b0f14; --paper: #f4f1ea; --muted: #6b7280; }}
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; font-family: Geist, system-ui, sans-serif; background: #e9e6df; color: var(--ink); }}
  main {{ max-width: 1240px; margin: 0 auto; padding: 48px 32px 96px; }}
  h1 {{ font-weight: 600; font-size: 34px; letter-spacing: -0.02em; margin: 0 0 8px; }}
  .intro {{ color: #374151; max-width: 820px; line-height: 1.55; }}
  .palette {{ display: flex; gap: 10px; margin: 18px 0 36px; flex-wrap: wrap; }}
  .swatch {{ display: flex; align-items: center; gap: 8px; font: 13px "Geist Mono", monospace; background: #fff; padding: 6px 10px 6px 6px; border-radius: 8px; }}
  .swatch i {{ width: 22px; height: 22px; border-radius: 5px; display: inline-block; }}
  .card {{ background: #fff; border-radius: 18px; padding: 28px; margin: 0 0 28px; box-shadow: 0 1px 2px rgba(0,0,0,.06); }}
  .card header {{ display: flex; align-items: baseline; gap: 12px; }}
  .letter {{ font: 600 14px "Geist Mono", monospace; background: var(--ink); color: #fff; border-radius: 6px; padding: 3px 8px; }}
  h2 {{ margin: 0; font-weight: 600; font-size: 22px; letter-spacing: -0.01em; }}
  .card p {{ color: #4b5563; max-width: 860px; line-height: 1.5; margin: 10px 0 18px; }}
  .row {{ display: flex; gap: 18px; flex-wrap: wrap; align-items: flex-end; margin-bottom: 18px; }}
  figure {{ margin: 0; display: flex; flex-direction: column; align-items: center; gap: 8px; }}
  figure.paper > svg, figure.ink > svg {{ padding: 24px; border-radius: 14px; width: 168px; height: 168px; }}
  figure.paper > svg {{ background: var(--paper); }}
  figure.ink > svg {{ background: var(--ink); }}
  figcaption {{ font: 12px "Geist Mono", monospace; color: var(--muted); }}
  .smalls {{ flex-direction: column; }}
  .smalls .small {{ display: inline-flex; align-items: center; gap: 8px; }}
  figure.smalls {{ flex-direction: column; align-items: flex-start; }}
  figure.smalls > .small {{ display: flex; }}
  .small span {{ font: 11px "Geist Mono", monospace; color: var(--muted); }}
  .lockup {{ display: flex; align-items: center; gap: 16px; padding: 22px 30px; border-radius: 14px; }}
  .lockup.light {{ background: var(--paper); color: var(--ink); }}
  .lockup.dark {{ background: var(--ink); color: #f2efe8; }}
  .ws {{ font-family: Geist; font-weight: 560; font-size: 46px; letter-spacing: -0.035em; }}
  .wm {{ font-family: "Geist Mono"; font-weight: 500; font-size: 30px; letter-spacing: 0.22em; }}
  .current {{ display:flex; align-items:center; gap:18px; background:#fff; border-radius:18px; padding:20px 28px; margin-bottom:28px; }}
  .current img {{ height: 64px; }}
</style></head>
<body><main>
  <h1>Aqueduct: mark explorations</h1>
  <p class="intro">Eight directions for a more modern, abstract and lasting mark, each built from the same idea:
  a structure that carries, and what it carries. Every concept is shown as an icon on dark and light tiles, alone on
  light and dark, in one colour (the test of a mark that will last), at favicon sizes, and in a lockup with two wordmark
  options: Geist, lowercase and tight, and Geist Mono, uppercase and wide. The water uses one gradient everywhere, so it
  becomes the brand's signature; every mark also works without it.</p>
  <div class="palette">
    <span class="swatch"><i style="background:#0b0f14"></i>Night #0b0f14</span>
    <span class="swatch"><i style="background:#f2efe8;outline:1px solid #ddd"></i>Bone #f2efe8</span>
    <span class="swatch"><i style="background:linear-gradient(90deg,#5ee7df,#3b82f6)"></i>Water #5ee7df → #3b82f6</span>
    <span class="swatch"><i style="background:#1d4ed8"></i>River #1d4ed8</span>
  </div>
  <div class="current"><img src="../aqueduct-icon.svg" alt="current icon"><div><strong>Current mark</strong>, for comparison: the illustrated Pont du Gard (PR #15).</div></div>
  {cards}
</main></body></html>
"""
    (HERE / "index.html").write_text(html)
    print("wrote", HERE / "index.html")


if __name__ == "__main__":
    main()
