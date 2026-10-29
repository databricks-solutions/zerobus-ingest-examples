# Vendored front-end assets

Served by the connector itself (`sparkplug_connector/web/static/ui/vendor/`); nothing is loaded from a CDN (plant networks are often air-gapped). No build step:
the files below are the packages' published distribution files, copied unchanged.

| File | Package | Version | Published | License | SHA-256 |
| --- | --- | --- | --- | --- | --- |
| `alpine-csp.min.js` | [`@alpinejs/csp`](https://www.npmjs.com/package/@alpinejs/csp) (`dist/cdn.min.js`) | 3.17.4 | 2026-09-21 | MIT | `0d18d7f8d7910e2e0212f0f056b12f50bebc3abb7d88d2f7c7cb4c336fe4519a` |
| `pico.min.css` | [`@picocss/pico`](https://www.npmjs.com/package/@picocss/pico) (`css/pico.min.css`) | 2.1.1 | 2025-03-15 | MIT | `fbc9a63fc9fc9f72d12fd7fc9806e11fa9f77ae4f9cad146b27003a1119ba3db` |

The npm tarballs matched the registry's published SHA-1 (`e679eb03…4013` for `csp-3.17.4.tgz`,
`f2c4573b…9f8e` for `pico-2.1.1.tgz`). Sizes: Alpine 72 KB (24 KB gzipped), Pico 83 KB (12 KB gzipped).

The Alpine **CSP build** evaluates `x-*` expressions with its own parser instead of `new Function`, so the console
runs under `script-src 'self'` without `unsafe-eval`. Its expressions are limited (no arrow functions, template
literals or statements), so page logic lives in `js/*.js` components registered with `Alpine.data`.

Icons in the pages are simple inline SVGs drawn for this project.

To update: download the package tarball from the npm registry, check its `dist.shasum`/`integrity`, copy the file
above, and update this table (`shasum -a 256 <file>`). Prefer releases at least two weeks old.
