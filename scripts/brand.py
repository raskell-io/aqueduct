"""Generates Aqueduct's brand marks from code, so they stay consistent:

    python3 scripts/brand.py

The mark is a fractal arch: one arch carrying two, each carrying two, the line thinning as the arches get
smaller, all standing on a line of water. Each pair shares its parent's piers, as in a real arcade. It is the
Pont du Gard reduced to its idea (structure that carries, repeated at every scale) and the essay's fractal
thread: the same loop at every scale.

- docs/brand/aqueduct-icon.svg                   the icon: the mark on a night tile, rounded
- docs/brand/aqueduct-icon-square.svg            the same, square (home screens round it themselves)
- docs/brand/aqueduct-mark-{light,dark}.svg      the mark alone; light for dark backgrounds, dark for light
- docs/brand/aqueduct-mark-mono-{light,dark}.svg the mark in one colour, for print, stamps and embroidery
- docs/brand/aqueduct-wordmark-{light,dark}.svg  "aqueduct" in Syne, as paths
- docs/brand/aqueduct-lockup-{light,dark}.svg    mark and wordmark side by side
- docs/brand/aqueduct-{168,180,192,512}.png      the icon as PNG (rendered with headless Chrome)

The wordmark needs fontTools (pip install fonttools). The PNGs need Google Chrome.
"""
import io
import pathlib
import shutil
import subprocess
import tempfile

from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont
from fontTools.varLib import instancer

ROOT = pathlib.Path(__file__).resolve().parent.parent
BRAND = ROOT / "docs" / "brand"

NIGHT = "#0b0f14"   # the tile; the mark and wordmark on light backgrounds
BONE = "#f2efe8"    # the mark and wordmark on dark backgrounds
WATER = ("#5ee7df", "#3b82f6")  # the line of water, left to right: the brand's signature

# The mark on a 100 grid: arches from x 12 to 88 standing on y 78, water below.
LEFT, RIGHT, BASE = 12.0, 88.0, 78.0
WEIGHTS = (8.0, 5.0, 3.2)  # line weight per level: one arch, two, four
WATER_BAR = (10.0, 80.0, 80.0, 7.0)  # x, y, width, height
BOX = (8.0, 38.0, 84.0, 51.0)  # the mark's bounds with a little air: x, y, width, height


def arches():
    """The fractal arch as stroked paths: each arch carries two that share its piers."""
    paths = []

    def arch(a, b, depth):
        w = WEIGHTS[depth]
        r = (b - a) / 2 - w / 2
        paths.append(
            f'<path d="M{a + w / 2:g} {BASE:g} A{r:g} {r:g} 0 0 1 {b - w / 2:g} {BASE:g}" '
            f'fill="none" stroke="{{ink}}" stroke-width="{w:g}"/>'
        )
        if depth + 1 < len(WEIGHTS):
            m = (a + b) / 2
            arch(a, m, depth + 1)
            arch(m, b, depth + 1)

    arch(LEFT, RIGHT, 0)
    return "".join(paths)


def mark_body(ink, water, gradient_id="water"):
    """The mark's shapes. `water` is a colour, or None for the gradient."""
    x, y, w, h = WATER_BAR
    fill = water or f"url(#{gradient_id})"
    return arches().replace("{ink}", ink) + (
        f'<rect x="{x:g}" y="{y:g}" width="{w:g}" height="{h:g}" rx="{h / 2:g}" fill="{fill}"/>'
    )


def gradient(gradient_id="water"):
    a, b = WATER
    return (
        f'<defs><linearGradient id="{gradient_id}" x1="0" y1="0" x2="1" y2="0">'
        f'<stop offset="0" stop-color="{a}"/><stop offset="1" stop-color="{b}"/></linearGradient></defs>'
    )


def icon(rounded=True):
    """The mark centred on a night tile, 512 x 512."""
    size, scale = 512, 4.3
    bx, by, bw, bh = BOX
    tx = size / 2 - (bx + bw / 2) * scale
    ty = size / 2 - (by + bh / 2) * scale
    radius = f' rx="{size * 0.22:g}"' if rounded else ""
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" width="{size}" height="{size}">\n'
        "  <title>Aqueduct</title>\n"
        f"  {gradient()}\n"
        f'  <rect width="{size}" height="{size}"{radius} fill="{NIGHT}"/>\n'
        f'  <g transform="translate({tx:g} {ty:g}) scale({scale:g})">{mark_body(BONE, None)}</g>\n'
        "</svg>\n"
    )


def mark(ink, mono=False):
    bx, by, bw, bh = BOX
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{bx:g} {by:g} {bw:g} {bh:g}" role="img" aria-label="Aqueduct">'
        f"{'' if mono else gradient()}{mark_body(ink, ink if mono else None)}</svg>\n"
    )


def wordmark_paths():
    """'aqueduct' in Syne Bold as one SVG path, with its width and vertical metrics."""
    font = TTFont(BRAND / "fonts" / "Syne.ttf")
    font = instancer.instantiateVariableFont(font, {"wght": 700})
    buf = io.BytesIO()
    font.save(buf)
    font = TTFont(io.BytesIO(buf.getvalue()))
    glyphs = font.getGlyphSet()
    cmap = font.getBestCmap()
    upm = font["head"].unitsPerEm
    os2 = font["OS/2"]
    pen = SVGPathPen(glyphs)
    x = 0.0
    tracking = -0.02 * upm
    for ch in "aqueduct":
        name = cmap[ord(ch)]
        glyphs[name].draw(TransformPen(pen, (1, 0, 0, -1, x, 0)))
        x += glyphs[name].width + tracking
    return pen.getCommands(), x - tracking, os2.sTypoAscender, -os2.sTypoDescender, os2.sxHeight


def wordmark(ink, with_mark=False):
    d, width, ascent, descent, x_height = wordmark_paths()
    top, height = -ascent, ascent + descent
    text = f'<path fill="{ink}" d="{d}"/>'
    if not with_mark:
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 {top:g} {width:g} {height:g}" role="img" aria-label="Aqueduct">'
            f"{text}</svg>\n"
        )
    # Lockup: the mark is 1.65 x-heights tall and centred on the x-height, a third of its width from the word.
    bx, by, bw, bh = BOX
    scale = 1.65 * x_height / bh
    mark_w = bw * scale
    gap = mark_w * 0.32
    mark_top = -x_height / 2 - bh * scale / 2
    total = mark_w + gap + width
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 {top:g} {total:g} {height:g}" role="img" aria-label="Aqueduct">'
        f"{gradient()}"
        f'<g transform="translate(0 {mark_top:g}) scale({scale:g}) translate({-bx:g} {-by:g})">{mark_body(ink, None)}</g>'
        f'<g transform="translate({mark_w + gap:g} 0)">{text}</g></svg>\n'
    )


def chrome():
    for candidate in ("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "google-chrome", "chromium"):
        if pathlib.Path(candidate).exists() or shutil.which(candidate):
            return candidate
    raise SystemExit("PNG export needs Google Chrome or Chromium")


def png(svg_path, out, size):
    """Renders an SVG with headless Chrome, which draws gradients and strokes exactly as browsers do."""
    with tempfile.TemporaryDirectory() as tmp:
        page = pathlib.Path(tmp) / "page.html"
        page.write_text(
            f'<html><body style="margin:0;background:transparent">'
            f'<img src="{svg_path.as_uri()}" width="{size}" height="{size}"></body></html>'
        )
        subprocess.run(
            [chrome(), "--headless", "--disable-gpu", "--hide-scrollbars", "--default-background-color=00000000",
             f"--window-size={size},{size}", f"--screenshot={out}", page.as_uri()],
            check=True, capture_output=True,
        )


def main():
    BRAND.mkdir(parents=True, exist_ok=True)
    for old in BRAND.glob("aqueduct-*"):
        old.unlink()
    (BRAND / "aqueduct-icon.svg").write_text(icon())
    square = BRAND / "aqueduct-icon-square.svg"
    square.write_text(icon(rounded=False))
    (BRAND / "aqueduct-mark-light.svg").write_text(mark(BONE))
    (BRAND / "aqueduct-mark-dark.svg").write_text(mark(NIGHT))
    (BRAND / "aqueduct-mark-mono-light.svg").write_text(mark(BONE, mono=True))
    (BRAND / "aqueduct-mark-mono-dark.svg").write_text(mark(NIGHT, mono=True))
    (BRAND / "aqueduct-wordmark-light.svg").write_text(wordmark(BONE))
    (BRAND / "aqueduct-wordmark-dark.svg").write_text(wordmark(NIGHT))
    (BRAND / "aqueduct-lockup-light.svg").write_text(wordmark(BONE, with_mark=True))
    (BRAND / "aqueduct-lockup-dark.svg").write_text(wordmark(NIGHT, with_mark=True))
    png(BRAND / "aqueduct-icon.svg", BRAND / "aqueduct-168.png", 168)
    for size in (180, 192, 512):
        png(square, BRAND / f"aqueduct-{size}.png", size)
    print("wrote", ", ".join(p.name for p in sorted(BRAND.iterdir()) if p.is_file()))


if __name__ == "__main__":
    main()
