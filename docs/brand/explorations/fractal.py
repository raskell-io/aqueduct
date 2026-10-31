"""Generates fractal.html: refinements of the Fractal mark, and a typeface study for the wordmark."""
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
NIGHT, BONE = "#0b0f14", "#f2efe8"


def arch_stroke(x0, x1, base, w, color="currentColor"):
    r = (x1 - x0) / 2
    i = w / 2
    return (
        f'<path d="M{x0 + i:.2f} {base} A{r - i:.2f} {r - i:.2f} 0 0 1 {x1 - i:.2f} {base}" '
        f'fill="none" stroke="{color}" stroke-width="{w:.2f}"/>'
    )


def fractal_lines(levels, weights, base=78, x0=12, x1=88, water_bar=True, water_level=None):
    parts = []

    def go(a, b, depth):
        color = "url(#water)" if water_level is not None and depth == water_level else "currentColor"
        w = weights[depth]
        parts.append(arch_stroke(a, b, base, w, color))
        if depth + 1 < levels:
            # The two children share the parent's piers and meet on a shared middle pier, as in an arcade.
            m = (a + b) / 2
            go(a, m, depth + 1)
            go(m, b, depth + 1)

    go(x0, x1, 0)
    if water_bar:
        parts.append(f'<rect x="{x0 - 2}" y="{base + 2}" width="{x1 - x0 + 4}" height="7" rx="3.5" fill="url(#water)"/>')
    return "\n".join(parts)


def fractal_solid(levels=3, base=80, x0=10, x1=90):
    """Nested half-discs on one base, filled with the even-odd rule: disc, holes, discs."""
    d = []

    def go(a, b, depth):
        r = (b - a) / 2
        d.append(f"M{a:.2f} {base} A{r:.2f} {r:.2f} 0 0 1 {b:.2f} {base} Z")
        if depth + 1 < levels:
            gap = r * 0.12
            inner_a, inner_b = a + gap, b - gap
            m = (inner_a + inner_b) / 2
            go(inner_a, m - gap / 2, depth + 1)
            go(m + gap / 2, inner_b, depth + 1)

    go(x0, x1, 0)
    return (
        f'<path d="{" ".join(d)}" fill="currentColor" fill-rule="evenodd"/>'
        f'<rect x="{x0}" y="{base + 3}" width="{x1 - x0}" height="7" rx="3.5" fill="url(#water)"/>'
    )


VARIANTS = [
    ("F1", "Three levels, thinning line", lambda: fractal_lines(3, [8.0, 5.0, 3.2]),
     "The direction you chose, as it stands: one arch, two, four, the line thinning as the arches get smaller."),
    ("F2", "Two levels", lambda: fractal_lines(2, [9.0, 6.5]),
     "One arch carrying two. Less detail, so it stays crisp at 16 px; the recursion is implied rather than shown."),
    ("F3", "Monoline", lambda: fractal_lines(3, [5.5, 5.5, 5.5]),
     "Every arch drawn with the same line: more technical and more even, like a diagram."),
    ("F4", "Solid", lambda: fractal_solid(3),
     "The arches as filled shapes: a half-disc, two holes, four half-discs again. Bold, works as a stamp and at 16 px."),
    ("F5", "Water inside", lambda: fractal_lines(3, [8.0, 5.0, 3.6], water_bar=False, water_level=2),
     "No base line: the smallest arches are the water, as if what the structure carries runs through its finest parts."),
    ("F6", "Four levels", lambda: fractal_lines(4, [8.0, 5.0, 3.2, 2.0]),
     "One level deeper, for large use (a hero image, print, a T-shirt); paired with F1 or F2 for small sizes."),
]

FONTS = [
    # (name, file, css weight, extra css, note)
    ("Geist", "Geist-Variable.woff2", 560, "letter-spacing:-0.035em", "neutral, contemporary; the family of Kishi"),
    ("Geist Mono", "GeistMono-Variable.woff2", 500, "letter-spacing:-0.02em", "engineered, code-adjacent"),
    ("Inter", "fonts/Inter.ttf", 620, "letter-spacing:-0.04em;font-variation-settings:'opsz' 32", "the default of modern software; very readable"),
    ("IBM Plex Sans", "fonts/IBMPlexSans.ttf", 600, "letter-spacing:-0.02em", "engineered and institutional: the bank-and-insurer register"),
    ("Space Grotesk", "fonts/SpaceGrotesk.ttf", 600, "letter-spacing:-0.03em", "technical with character; quirky a and q"),
    ("Manrope", "fonts/Manrope.ttf", 700, "letter-spacing:-0.035em", "geometric, calm, open"),
    ("Sora", "fonts/Sora.ttf", 600, "letter-spacing:-0.04em", "geometric and slightly futuristic"),
    ("Outfit", "fonts/Outfit.ttf", 550, "letter-spacing:-0.03em", "round geometric; echoes the arches"),
    ("Unbounded", "fonts/Unbounded.ttf", 500, "letter-spacing:-0.03em", "wide and futuristic; strong as a logotype"),
    ("Syne", "fonts/Syne.ttf", 700, "letter-spacing:-0.02em", "expressive, art-school wide"),
    ("JetBrains Mono", "fonts/JetBrainsMono.ttf", 600, "letter-spacing:-0.03em", "a coder's mono; very legible"),
    ("Fraunces", "fonts/Fraunces.ttf", 520, "letter-spacing:-0.03em;font-variation-settings:'opsz' 144,'SOFT' 40", "a soft serif with warmth; literary, like the essay"),
    ("Instrument Serif", "fonts/InstrumentSerif.ttf", 400, "letter-spacing:-0.02em", "elegant display serif; editorial"),
    ("Cinzel", "fonts/Cinzel.ttf", 600, "letter-spacing:0.02em", "Roman inscriptional capitals: the aqueduct's own era"),
]


def defs(prefix):
    return (
        f'<defs><linearGradient id="{prefix}w" x1="0" y1="0" x2="1" y2="0">'
        f'<stop offset="0" stop-color="#5ee7df"/><stop offset="1" stop-color="#3b82f6"/></linearGradient></defs>'
    )


def svg(body, fg, prefix, size, tile=None, mono=False, crop=None):
    body = body.replace("url(#water)", fg if mono else f"url(#{prefix}w)")
    bg = f'<rect x="-14" y="-14" width="128" height="128" rx="28" fill="{tile}"/>' if tile else ""
    vb = "-14 -14 128 128" if tile else (crop or "4 26 92 66")
    w = size
    h = size if tile else round(size * float(vb.split()[3]) / float(vb.split()[2]))
    return (
        f'<svg viewBox="{vb}" width="{w}" height="{h}" style="color:{fg}" xmlns="http://www.w3.org/2000/svg">'
        f"{defs(prefix)}{bg}{body}</svg>"
    )


def variant_card(code, name, fn, text, n):
    body = fn()
    p = lambda k: f"v{n}{k}"
    smalls = "".join(
        f'<div class="small">{svg(body, BONE, p(f"s{s}"), s, tile=NIGHT)}<span>{s}</span></div>' for s in (48, 32, 16)
    )
    return f"""
  <section class="card">
    <header><span class="letter">{code}</span><h2>{name}</h2></header>
    <p>{text}</p>
    <div class="row">
      <figure>{svg(body, BONE, p("i"), 168, tile=NIGHT)}<figcaption>icon, dark tile</figcaption></figure>
      <figure>{svg(body, NIGHT, p("l"), 168, tile="#f4f1ea")}<figcaption>icon, light tile</figcaption></figure>
      <figure class="paper">{svg(body, NIGHT, p("a"), 150)}<figcaption>on light</figcaption></figure>
      <figure class="ink">{svg(body, BONE, p("b"), 150)}<figcaption>on dark</figcaption></figure>
      <figure class="paper">{svg(body, NIGHT, p("m"), 150, mono=True)}<figcaption>one colour</figcaption></figure>
      <figure class="smalls">{smalls}<figcaption>favicon sizes</figcaption></figure>
    </div>
  </section>"""


def font_card(name, file, weight, extra, note, n):
    mark = fractal_lines(3, [8.0, 5.0, 3.2])
    fam = f"f{n}"
    style = f"font-family:'{fam}';font-weight:{weight};{extra}"
    return f"""
  <section class="card type">
    <header><span class="letter">{n + 1:02d}</span><h2>{name}</h2><span class="note">{note}</span></header>
    <div class="row">
      <div class="lockup light">{svg(mark, NIGHT, f"t{n}a", 78)}<span style="{style};font-size:58px">aqueduct</span></div>
      <div class="lockup dark">{svg(mark, BONE, f"t{n}b", 78)}<span style="{style};font-size:58px">Aqueduct</span></div>
    </div>
    <div class="row">
      <div class="lockup light small-lockup">{svg(mark, NIGHT, f"t{n}c", 44)}<span style="{style};font-size:26px;letter-spacing:0.24em;text-transform:uppercase">aqueduct</span></div>
      <div class="sample" style="{style};font-size:22px">Durable execution for procedures that agents write and people approve.</div>
    </div>
  </section>"""


def main():
    faces = "\n".join(
        f"@font-face {{ font-family: 'f{i}'; src: url('{f[1]}'); font-weight: 100 900; }}" for i, f in enumerate(FONTS)
    )
    variants = "".join(variant_card(*v, i) for i, v in enumerate(VARIANTS))
    fonts = "".join(font_card(*f, i) for i, f in enumerate(FONTS))
    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Aqueduct: the Fractal mark and its typeface</title>
<style>
  {faces}
  @font-face {{ font-family: Geist; src: url(Geist-Variable.woff2); font-weight: 100 900; }}
  @font-face {{ font-family: "Geist Mono"; src: url(GeistMono-Variable.woff2); font-weight: 100 900; }}
  :root {{ --ink: {NIGHT}; --paper: #f4f1ea; --muted: #6b7280; }}
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; font-family: Geist, system-ui, sans-serif; background: #e9e6df; color: var(--ink); }}
  main {{ max-width: 1240px; margin: 0 auto; padding: 48px 32px 96px; }}
  h1 {{ font-weight: 600; font-size: 34px; letter-spacing: -0.02em; margin: 0 0 8px; }}
  h3 {{ font-weight: 600; font-size: 24px; margin: 48px 0 6px; letter-spacing: -0.01em; }}
  .intro {{ color: #374151; max-width: 860px; line-height: 1.55; }}
  .card {{ background: #fff; border-radius: 18px; padding: 26px 28px; margin: 0 0 22px; box-shadow: 0 1px 2px rgba(0,0,0,.06); }}
  .card header {{ display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap; }}
  .letter {{ font: 600 14px "Geist Mono", monospace; background: var(--ink); color: #fff; border-radius: 6px; padding: 3px 8px; }}
  h2 {{ margin: 0; font-weight: 600; font-size: 21px; letter-spacing: -0.01em; }}
  .note {{ color: var(--muted); font-size: 15px; }}
  .card p {{ color: #4b5563; max-width: 860px; line-height: 1.5; margin: 10px 0 16px; }}
  .row {{ display: flex; gap: 18px; flex-wrap: wrap; align-items: center; margin-top: 16px; }}
  figure {{ margin: 0; display: flex; flex-direction: column; align-items: center; gap: 8px; }}
  figure.paper > svg, figure.ink > svg {{ padding: 30px 9px; border-radius: 14px; width: 168px; height: 168px; }}
  figure.paper > svg {{ background: var(--paper); }}
  figure.ink > svg {{ background: var(--ink); }}
  figcaption {{ font: 12px "Geist Mono", monospace; color: var(--muted); }}
  figure.smalls {{ align-items: flex-start; }}
  .small {{ display: flex; align-items: center; gap: 8px; }}
  .small span {{ font: 11px "Geist Mono", monospace; color: var(--muted); }}
  .lockup {{ display: flex; align-items: center; gap: 18px; padding: 20px 30px; border-radius: 14px; }}
  .lockup.light {{ background: var(--paper); color: var(--ink); }}
  .lockup.dark {{ background: var(--ink); color: {BONE}; }}
  .small-lockup {{ padding: 16px 24px; }}
  .sample {{ color: #374151; max-width: 520px; line-height: 1.35; }}
</style></head>
<body><main>
  <h1>Aqueduct: the Fractal mark and its typeface</h1>
  <p class="intro">You chose the Fractal direction. Below, six refinements of it, each at every size and in one colour,
  then the same mark (F1) set against fourteen typefaces, all under the SIL Open Font License: neutral and engineered
  (Geist, Inter, IBM Plex Sans), technical (Geist Mono, JetBrains Mono, Space Grotesk), geometric and round (Manrope,
  Sora, Outfit), wide and expressive (Unbounded, Syne), and serif (Fraunces, Instrument Serif, and Cinzel, which is
  cut from Roman inscriptions, the aqueduct's own era). Each is shown lowercase, in title case, and in spaced capitals.</p>
  <h3>The mark</h3>
  {variants}
  <h3>The typeface</h3>
  {fonts}
</main></body></html>
"""
    (HERE / "fractal.html").write_text(html)
    print("wrote", HERE / "fractal.html")


if __name__ == "__main__":
    main()
