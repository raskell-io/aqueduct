"""Generates Aqueduct's brand marks from code, so they stay consistent:

    python3 scripts/brand.py

The mark is an aqueduct in three tiers of arches, like the Pont du Gard: two great arches, four above
them, eight at the top, each arch carrying two above it. A straight channel of water runs along the
top: the canal, the procedure that carries what was found. A river winds through the lowest arches:
the rugged world agents explore. The River and the Canal, in one picture.

- docs/brand/aqueduct-icon.svg                   the icon: the mark on a dark tile, rounded
- docs/brand/aqueduct-icon-square.svg            the same, square (home screens round it themselves)
- docs/brand/aqueduct-mark-{light,dark}.svg      the mark alone; light for dark backgrounds, dark for light
- docs/brand/aqueduct-wordmark-{light,dark}.svg  "aqueduct" in Geist, as paths
- docs/brand/aqueduct-lockup-{light,dark}.svg    mark and wordmark side by side
- docs/brand/aqueduct-{168,180,192,512}.png      the icon as PNG (needs ImageMagick)

The wordmark needs fontTools (pip install fonttools brotli).
"""
import io
import pathlib
import subprocess

from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont
from fontTools.varLib import instancer

ROOT = pathlib.Path(__file__).resolve().parent.parent
BRAND = ROOT / "docs" / "brand"

NIGHT = "#0f1419"      # the tile, and the mark on light backgrounds
STONE = "#e8d6ae"      # limestone: the mark on dark backgrounds, and the wordmark there
CHANNEL = "#5db4e8"    # the water in the channel: clear, straight, carried
RIVER = "#2f7fbf"      # the river below: deeper, winding

# The structure on a 512 grid.
LEFT, RIGHT = 72, 440
CHANNEL_TOP, CHANNEL_BOTTOM = 104, 118
TIERS = [  # (top, bottom, arches); bottom tier last
    (122, 176, 8),
    (176, 264, 4),
    (264, 416, 2),
]
PIER = 0.17  # pier width as a share of a bay


def arches(top, bottom, count, band):
    """Subpaths for a tier's arch openings, cut from the tier with the even-odd rule."""
    width = RIGHT - LEFT
    pier = PIER * width / count
    opening = (width - (count + 1) * pier) / count
    radius = opening / 2
    centre = top + band + radius
    paths = []
    for i in range(count):
        x = LEFT + pier + i * (opening + pier)
        paths.append(
            f"M{x:.2f} {bottom} V{centre:.2f} A{radius:.2f} {radius:.2f} 0 0 1 {x + opening:.2f} {centre:.2f} V{bottom} Z"
        )
    return paths


def structure_path():
    parts = [f"M{LEFT} {TIERS[0][0]} H{RIGHT} V{TIERS[-1][1]} H{LEFT} Z"]
    for top, bottom, count in TIERS:
        parts += arches(top, bottom, count, band=(bottom - top) * 0.11)
    return " ".join(parts)


def river_path():
    """A winding line through the lowest arches."""
    y, amplitude, waves = 398, 9, 4
    x0, x1 = LEFT - 24, RIGHT + 24
    step = (x1 - x0) / (waves * 2)
    d = f"M{x0} {y}"
    for i in range(waves * 2):
        cx = x0 + step * (i + 0.5)
        cy = y - amplitude if i % 2 == 0 else y + amplitude
        d += f" Q{cx:.1f} {cy} {x0 + step * (i + 1):.1f} {y}"
    return d


def mark_group(stone):
    """The mark on the 512 grid: river behind, the arches, the channel on top."""
    return (
        f'<path d="{river_path()}" fill="none" stroke="{RIVER}" stroke-width="12" stroke-linecap="round"/>'
        f'<path d="{structure_path()}" fill="{stone}" fill-rule="evenodd"/>'
        f'<rect x="{LEFT}" y="{CHANNEL_TOP}" width="{RIGHT - LEFT}" height="{CHANNEL_BOTTOM - CHANNEL_TOP}" rx="3" fill="{CHANNEL}"/>'
    )


def icon(rounded=True):
    radius = f' rx="{512 * 0.22:g}"' if rounded else ""
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" width="512" height="512">\n'
        "  <title>Aqueduct</title>\n"
        f'  <rect width="512" height="512"{radius} fill="{NIGHT}"/>\n'
        f"  {mark_group(STONE)}\n"
        "</svg>\n"
    )


def mark(stone):
    """The mark alone, cropped to its drawing."""
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="40 96 432 330" role="img" aria-label="Aqueduct">'
        f"{mark_group(stone)}</svg>\n"
    )


def wordmark_paths():
    """'aqueduct' in Geist as one SVG path, plus its width and metrics."""
    font = TTFont(BRAND / "fonts" / "Geist-Variable.woff2")
    font = instancer.instantiateVariableFont(font, {"wght": 540})
    buf = io.BytesIO()
    font.flavor = None
    font.save(buf)
    font = TTFont(io.BytesIO(buf.getvalue()))
    glyphs = font.getGlyphSet()
    cmap = font.getBestCmap()
    upm = font["head"].unitsPerEm
    ascent = font["OS/2"].sTypoAscender
    descent = -font["OS/2"].sTypoDescender
    pen = SVGPathPen(glyphs)
    x = 0
    tracking = upm * 0.004
    for ch in "aqueduct":
        name = cmap[ord(ch)]
        glyphs[name].draw(TransformPen(pen, (1, 0, 0, -1, x, 0)))
        x += glyphs[name].width + tracking
    return pen.getCommands(), x - tracking, upm, ascent, descent


def wordmark(color, stone=None):
    d, width, upm, ascent, descent = wordmark_paths()
    top = -ascent
    height = ascent + descent * 0.9
    text = f'<path fill="{color}" d="{d}"/>'
    if stone is None:
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 {top:g} {width:g} {height:g}" role="img" aria-label="Aqueduct">'
            f"{text}</svg>\n"
        )
    # Lockup: the mark at the height of the text, then the word.
    s = height * 1.05
    scale = s / 330
    pad = s * 0.28
    mark_width = 432 * scale
    total = mark_width + pad + width
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 {top:g} {total:g} {height:g}" role="img" aria-label="Aqueduct">'
        f'<g transform="translate(0 {top - (s - height) / 2:g}) scale({scale:g}) translate(-40 -96)">{mark_group(stone)}</g>'
        f'<g transform="translate({mark_width + pad:g} 0)">{text}</g></svg>\n'
    )


def png(svg_path, out, size):
    subprocess.run(
        ["magick", "-background", "none", "-density", "300", str(svg_path), "-resize", f"{size}x{size}", str(out)],
        check=True,
    )


def main():
    BRAND.mkdir(parents=True, exist_ok=True)
    (BRAND / "aqueduct-icon.svg").write_text(icon())
    square = BRAND / "aqueduct-icon-square.svg"
    square.write_text(icon(rounded=False))
    (BRAND / "aqueduct-mark-light.svg").write_text(mark(STONE))
    (BRAND / "aqueduct-mark-dark.svg").write_text(mark(NIGHT))
    (BRAND / "aqueduct-wordmark-light.svg").write_text(wordmark(STONE))
    (BRAND / "aqueduct-wordmark-dark.svg").write_text(wordmark(NIGHT))
    (BRAND / "aqueduct-lockup-light.svg").write_text(wordmark(STONE, stone=STONE))
    (BRAND / "aqueduct-lockup-dark.svg").write_text(wordmark(NIGHT, stone=NIGHT))
    png(BRAND / "aqueduct-icon.svg", BRAND / "aqueduct-168.png", 168)
    for size in (180, 192, 512):
        png(square, BRAND / f"aqueduct-{size}.png", size)
    print("wrote", ", ".join(p.name for p in sorted(BRAND.iterdir()) if p.is_file()))


if __name__ == "__main__":
    main()
