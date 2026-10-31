# Brand

The mark is an aqueduct in three tiers of arches, like the
[Pont du Gard](https://en.wikipedia.org/wiki/Pont_du_Gard): two great arches, four above them, eight at
the top, so that each arch carries two above it. A straight channel of water runs along the top: the canal,
the procedure that carries what was found. A river winds through the lowest arches: the rugged world that
agents explore. It is the essay *The River and the Canal* in one picture.

| File | Use |
|---|---|
| `aqueduct-icon.svg` | the icon: the mark on a dark tile, rounded |
| `aqueduct-icon-square.svg` | the same, square, for home-screen icons (the OS rounds them) |
| `aqueduct-mark-light.svg`, `aqueduct-mark-dark.svg` | the mark alone: light for dark backgrounds, dark for light ones |
| `aqueduct-wordmark-light.svg`, `aqueduct-wordmark-dark.svg` | "aqueduct" in Geist, as paths |
| `aqueduct-lockup-light.svg`, `aqueduct-lockup-dark.svg` | the mark and the wordmark together |
| `aqueduct-168.png` | the icon at 168 px, for project cards |
| `aqueduct-180.png`, `aqueduct-192.png`, `aqueduct-512.png` | square icons for home screens and stores |

## Colour

| Token | Hex | Role |
|---|---|---|
| Night | `#0f1419` | the tile; the mark and the wordmark on light backgrounds |
| Stone | `#e8d6ae` | limestone: the mark and the wordmark on dark backgrounds |
| Channel | `#5db4e8` | the water carried along the top |
| River | `#2f7fbf` | the river below |

## Type

Geist (SIL Open Font License, by Vercel), bundled in `fonts/` with its licence. The wordmark is set in
lowercase at weight 540 and converted to paths, so it needs no font where it is shown.

## Regenerating

```sh
python3 scripts/brand.py     # needs fontTools (pip install fonttools brotli) and ImageMagick
```

The marks are drawn in code, so changing a colour, the number of arches or the proportions is a change to
`scripts/brand.py`, and every file follows.
