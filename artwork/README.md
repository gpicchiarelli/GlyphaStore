# GlyphaStore artwork

This directory is the canonical home of the GlyphaStore visual identity.

## Production source

For Apple platforms, build the app icon in Icon Composer from the three SVG layers under
`apple/icon-composer/layers/`, in this order:

1. `glyphastore-background-layer.svg`
2. `glyphastore-g-layer.svg`
3. `glyphastore-record-layer.svg`

Keep the canvas square and let Icon Composer apply platform masks, materials, shadows, highlights
and appearance variants. The SVGs deliberately contain no baked mask, glow or drop shadow.

## Directory layout

- `brand/master/`: deterministic vector master.
- `brand/concept/`: generated visual-development reference; not a production layer.
- `apple/icon-composer/layers/`: separate 1024 × 1024 SVG layers for Icon Composer.
- `apple/raster/`: flat compatibility exports generated from the vector master.

## Raster exports

The modern Apple delivery path is the Icon Composer project, with the 1024 × 1024 master used for
App Store artwork. The smaller PNGs are compatibility assets for older asset catalogs, macOS icon
sets, documentation and third-party packaging.

| Pixels | Typical use |
|---:|---|
| 1024 | App Store / master raster |
| 512, 256, 128, 64, 32, 16 | macOS compatibility |
| 180, 167, 152, 120, 87, 80, 76, 60, 58, 40, 29, 20 | iPhone/iPad legacy slots |
| 192, 136, 114 | additional iPad compatibility slots |

All PNGs are square, RGB/RGBA, full-bleed and intentionally have sharp canvas corners. Do not add
rounded corners: Apple applies the final platform mask.

Artwork in this directory is part of GlyphaStore and is licensed under BSD-3-Clause with the
project copyright ([LICENSE](../LICENSE), [docs/legal/licensing.md](../docs/legal/licensing.md)).
Apple documentation links below are external materials under Apple’s copyright.

## Identity

**GlyphaStore** combines *glyph* — a mark carved into a surface — with *store* — durable,
addressable memory. The brand idea is **a mark that persists**: *il glifo della memoria*.

The abstract `G` consists of three immutable Segment plates. The copper tile at its centre is one
exact Record, small and addressable. Keep the wordmark separate from the app icon and preserve the
capital `G` and `S` in editorial use.

Apple references: [App icons](https://developer.apple.com/design/human-interface-guidelines/app-icons),
[Icon Composer](https://developer.apple.com/icon-composer/), and
[Creating your app icon using Icon Composer](https://developer.apple.com/documentation/xcode/creating-your-app-icon-using-icon-composer).
