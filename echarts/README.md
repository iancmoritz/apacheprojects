# echarts-playground

Apache ECharts 6.1.0 as a playground: the `option` on the left is JavaScript you can edit, and the
chart on the right redraws as you type. Twelve presets cover the chart types people ask about (line,
bar, donut, scatter, heatmap, force graph, treemap, candlestick, world map, 3D surface on WebGL, a
streaming series and a morphing multi-frame dataset), and the interactive features -- `dataZoom`,
brush selection, tooltips, drill-down, `universalTransition`, light/dark themes, PNG and SVG export
-- are wired to the real ECharts APIs rather than screenshots of them.

ECharts is **not vendored**: `echarts@6.1.0` and `echarts-gl@2.1.0` come from npm and are bundled
here, together with the country boundaries the map preset needs, which are generated at build time
from `world-atlas` (Natural Earth 1:110m). Nothing is fetched from a CDN and there is no server side:
the page is static files, everything -- evaluating the option, generating the data, laying out and
painting the chart -- happens in the visitor's tab.

This directory builds the `/echarts/` route of the site the repository root assembles (the project
list at `/` is the root's; the dev server here serves it too). The page is `echarts/index.html`; every
asset this project emits at the origin root lives under `/_echarts/`, so nothing collides with
`airflow/`'s `/sw.js`, `/pyodide/*` and `/wheels/*`.

```text
echarts/index.html  +  src/main.ts
  |                        owns the one chart instance, the controls and the two exporters
  |
  +-- src/editor.ts       CodeJar + Prism, and `new Function` to evaluate the option
  +-- src/presets.ts      the gallery: each entry is the source that lands in the editor
  +-- src/behaviours.ts   the stream driver and the drill-down option builder
  +-- src/theme.ts        the two registered ECharts themes
  +-- src/runtime.ts      what is in the initial bundle, and which import() a chunk comes from
        |
        +-- initial   core + line + bar + grid/axis, tooltip, legend, dataZoom, brush, toolbox,
        |             dataset/transform, labelLayout, universalTransition, canvas renderer
        +-- chunks/extras.ts  pie, scatter, heatmap, graph, treemap, sunburst, candlestick,
        |                     boxplot, visualMap, calendar
        +-- chunks/geo.ts     map series + geo component + public/_echarts/world.geo.json
        +-- chunks/gl.ts      echarts-gl (surface/scatter/bar/line 3D, grid3D)
        +-- chunks/svg.ts     the SVG renderer, loaded the first time you export SVG
```

## Run it yourself

Requirements: Node `^20.19 || >=22.12` and any current browser (WebGL only for the 3D preset).

```bash
git clone https://github.com/iancmoritz/apacheprojects.git
cd apacheprojects/echarts
npm install
npm run dev     # generates public/_echarts/world.geo.json, then Vite on :5174
```

Then open <http://localhost:5174/echarts/> for the playground, or <http://localhost:5174/> for the
project list.

Static bundle instead of the dev server:

```bash
npm run build   # asset generation + tsc --noEmit + vite build into dist/
```

`dist/` is the route (`dist/echarts/index.html` plus `dist/_echarts/...`), so any static host can
serve it. Deployment is driven from the repository root — `../vercel.json` and
[the root README](../README.md#deploy-to-vercel) — which merges this `dist/` into the site.

Other scripts:

| command | what it does |
| --- | --- |
| `npm run assets` | regenerates `public/_echarts/world.geo.json` from the `world-atlas` TopoJSON (gitignored) |
| `npm run typecheck` | `tsc --noEmit` |

## How the hard parts work

- **The editor is the source of truth.** The option is JavaScript, not JSON, because half of what
  makes an ECharts option interesting is functions (`tooltip.formatter`, `label.formatter`) and
  generated data. `evaluate()` compiles the pane with `new Function` and calls it with a seeded
  `random(seed)` in scope, so presets carry their own 400-day series or 2 000 samples as code that
  runs in the tab instead of a literal blob. Anything that throws -- a syntax error mid-keystroke, an
  option ECharts rejects -- is caught and printed under the editor, and the last good chart stays on
  screen. Rendering is debounced 350 ms.
- **A preset may evaluate to an array.** If the pane returns `[option, option, ...]` the page treats
  them as frames and adds the frame stepper and auto-play. That is how the transition preset shows
  `universalTransition` morphing bars into donut slices with the same `series.id`.
- **Code splitting is driven by the option, not by the button.** `bundlesFor()` walks the evaluated
  option's series types (and the components a few of them imply) and returns the chunks the draw needs;
  `render()` awaits those `import()`s before it touches the chart. Typing `type: 'sunburst'` by hand
  fetches the extras chunk exactly like clicking a preset does, and the badge under the chart says
  which chunks the tab has.
- **A chart instance snapshots its registry.** ECharts collects the registered series types, layouts
  and processors when `init()` runs, so a chunk that arrives afterwards is invisible to the existing
  instance -- the first symptom was pie's label layout crashing on a `viewRect` its layout stage never
  produced. After any chunk load the page disposes the instance and re-creates it, which is also what
  a theme switch has to do (a theme is bound at `init`).
- **Boundaries at build time.** `scripts/build-geo.mjs` converts `world-atlas`'s `countries-110m.json`
  (TopoJSON) to a GeoJSON `FeatureCollection`, keeping only `properties.name` and geometry, and writes
  it to `public/_echarts/world.geo.json` -- 177 features, 424 KiB, gitignored. `chunks/geo.ts` fetches
  that file from this origin (a static asset, same as a JS chunk) and calls
  `echarts.registerMap('world', ...)`.
- **SVG export from a canvas page.** The page draws on canvas, and ECharts can only serialise what an
  SVG-painter instance drew, so the SVG button loads the SVG renderer chunk, mounts an offscreen
  instance of the same size with the same option, calls `renderToSVGString()` and disposes it. That
  option is drawn with `animation: false` -- serialising happens immediately, so an animated option
  would otherwise be caught part-drawn, bars still at zero height -- and with the theme's
  `backgroundColor` baked in, because an SVG has no canvas behind it to inherit one from. PNG is
  `getDataURL()` at `pixelRatio: 2` on the live instance, over the same background.
- **Small ECharts manners the page has to observe.** `resize` and `setOption` must not be called
  inside an ECharts pass, so the `ResizeObserver` defers by 60 ms; and a brush in the option is not
  armed until a cursor is taken, so `draw()` dispatches `takeGlobalCursor` and a visitor can drag over
  the scatter immediately.

## Numbers

Measured in Chrome 133 on this machine against the assembled production build (`npm run build` at the
repository root, then `npm run preview`, gzip on, localhost):

| | |
| --- | --- |
| navigation to first chart painted | **~400 ms** (399-433 ms over four runs) |
| initial transfer | **263 KiB** gzipped over 10 requests (781 KiB uncompressed): the 2.6 KiB page and nine JS chunks |
| `setOption` for a preset | 6-35 ms typical; 88 ms for the 3D surface, 91 ms for the first draw of the line preset (400 days x 2 series) |
| extras chunk (pie, scatter, heatmap, graph, treemap, candlestick) | +52 KiB gzipped, on first use |
| geo chunk (map series + boundaries) | +157 KiB gzipped, of which 143 KiB is `world.geo.json` |
| gl chunk (echarts-gl) | +124 KiB gzipped |
| everything this route emits | 1.9 MB on disk (1.5 MB of JS), plus 6.6 MB of source maps |

So a visitor who stays on the default line preset downloads 263 KiB, and only pays for the map or the
GPU chunk if they ask for them.

## Limitations

- **The editor is a small editor.** CodeJar (~2 KB) with Prism highlighting: no completion, no
  diagnostics, no ECharts option schema, and auto-closing brackets are switched off because CodeJar
  inserts a closer without stepping over the one you then type.
- **The option pane runs the code you type**, in your own tab, with only `random` supplied. That is
  the point of the page, but it means a preset is code, not data -- there is nothing to sandbox
  against a visitor pasting something hostile into their own browser, and nothing is persisted or
  sent anywhere.
- **The 3D preset needs WebGL** and is the largest chunk; on software rendering (a headless CI
  browser, a VM) it draws, but slowly, and Chrome logs a software-WebGL fallback warning.
- **The map is 1:110m Natural Earth**, matched to ECharts by country name, so the small states are
  simplified away and only the ~20 countries the preset names carry values; the rest render as "no
  data" on purpose.
- **Drill-down is a demonstration.** Clicking a bar generates that category's month-by-month
  breakdown in the tab (there is no dataset behind the page to drill into) and morphs into it with
  `universalTransition`; **Back** returns to the preset.
- **Themes are two hand-written ECharts themes**, not the upstream theme collection, and switching
  re-creates the chart instance -- an in-flight brush selection or a zoom window is not carried over.
