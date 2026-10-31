# Brand

The mark is a fractal arch: one arch carrying two, each carrying two, the line thinning as the arches get
smaller, all standing on a line of water. Each pair shares its parent's piers, as in a real arcade. It is the
Pont du Gard reduced to its idea (a structure that carries, repeated at every scale), and the thread that runs
through the essay *The River and the Canal*: the same loop at every scale. The line of water underneath is
what the structure exists to carry.

The wordmark is "aqueduct" in lowercase Syne.

| File | Use |
|---|---|
| `aqueduct-icon.svg` | the icon: the mark on a night tile, rounded |
| `aqueduct-icon-square.svg` | the same, square, for home-screen icons (the OS rounds them) |
| `aqueduct-mark-light.svg`, `aqueduct-mark-dark.svg` | the mark alone: light for dark backgrounds, dark for light ones |
| `aqueduct-mark-mono-light.svg`, `aqueduct-mark-mono-dark.svg` | the mark in one colour, for print, stamps and embroidery |
| `aqueduct-wordmark-light.svg`, `aqueduct-wordmark-dark.svg` | "aqueduct" in Syne Bold, as paths |
| `aqueduct-lockup-light.svg`, `aqueduct-lockup-dark.svg` | the mark and the wordmark together |
| `aqueduct-168.png` | the icon at 168 px, for project cards |
| `aqueduct-180.png`, `aqueduct-192.png`, `aqueduct-512.png` | square icons for home screens and stores |

## Colour

| Token | Hex | Role |
|---|---|---|
| Night | `#0b0f14` | the tile; the mark and wordmark on light backgrounds |
| Bone | `#f2efe8` | the mark and wordmark on dark backgrounds |
| Water | `#5ee7df` → `#3b82f6` | the line of water, a left-to-right gradient: the brand's signature |

The mark always works in one colour too (the `mono` files): the water line then takes the colour of the
arches.

## Type

[Syne](https://gitlab.com/bonjour-monde/fonderie/syne-typeface) (SIL Open Font License, by Bonjour Monde),
bundled in `fonts/` with its licence. The wordmark is set in lowercase at weight 700 with tracking of
−0.02 em, and converted to paths, so it needs no font where it is shown.

In the lockup the mark is 1.65 x-heights tall, centred on the x-height, and set a third of its width away
from the word.

## Small sizes

The mark has three levels of arches. At 16 px the smallest arches merge into texture while the silhouette
(the great arch on its line of water) stays clear.

## Regenerating

```sh
python3 scripts/brand.py     # needs fontTools (pip install fonttools) and Google Chrome for the PNGs
```

The marks are drawn in code, so a colour, a line weight or the number of levels is a change to
`scripts/brand.py`, and every file follows.
