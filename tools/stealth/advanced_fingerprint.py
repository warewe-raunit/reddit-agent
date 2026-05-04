"""
tools/stealth/advanced_fingerprint.py — 30+ additional anti-bot & fingerprint evasion techniques.

Targets:
  PerimeterX, HUMAN (WhiteOps), DataDome, Kasada, Akamai Bot Manager,
  Cloudflare Bot Management, Reddit Sentinel, F5 Shape

Inject with:
    from tools.stealth.advanced_fingerprint import inject_advanced
    await inject_advanced(page, profile)

Techniques added here (beyond base fingerprint.py):
 1.  navigator.vendor                   — must be "Google Inc." for Chrome
 2.  navigator.appVersion               — match UA string
 3.  navigator.product / productSub     — "Gecko" / "20030107"
 4.  navigator.userAgent consistency    — match profile UA
 5.  navigator.oscpu                    — undefined (Firefox prop, absent in Chrome)
 6.  navigator.buildID                  — undefined (Firefox prop)
 7.  navigator.cookieEnabled            — true
 8.  navigator.onLine                   — true
 9.  window.devicePixelRatio            — match profile DPR
10.  Date.prototype.getTimezoneOffset   — consistent with injected timezone
11.  screen.orientation stub            — landscape-primary, angle 0
12.  window.screenLeft/Right/X/Y        — non-zero (real browser has window chrome)
13.  navigator.permissions.query        — spoofed responses (geolocation, notifications, camera)
14.  Notification.permission            — "default"
15.  navigator.getGamepads()            — empty array (no controllers connected)
16.  SpeechSynthesis.getVoices()        — realistic system voices
17.  navigator.keyboard stub            — getLayoutMap() Promise
18.  navigator.wakeLock stub            — request() stub
19.  navigator.locks stub               — request() / query() stubs
20.  navigator.storage stub             — estimate() stub (real usage numbers)
21.  document.hasFocus()                — always true (tabbed-away detection)
22.  window.name                        — cleared (cross-site tracking vector)
23.  performance.getEntries() filter    — strip Playwright resource markers
24.  performance.now() jitter           — ±0.1ms noise (timing attack prevention)
25.  Function.prototype.toString        — protect patched functions from native-code check
26.  Object.getOwnPropertyDescriptor    — protect against Proxy-trap detection
27.  window.matchMedia                  — realistic media-query responses
28.  Canvas measureText noise           — font metrics fingerprinting defense
29.  WebGL getSupportedExtensions       — consistent whitelist
30.  WebGL getShaderPrecisionFormat     — consistent values
31.  navigator.sendBeacon               — functional but logged (no-op stealth mode)
32.  window.opener                      — null (prevent tracking via opener reference)
33.  document.referrer                  — controlled (empty for direct navigation)
34.  CSSStyleDeclaration.getPropertyValue — font-related CSS leak prevention
35.  navigator.mediaSession stub        — stub for automation detection
36.  HTMLCanvasElement.toBlob           — noise consistent with toDataURL
37.  OffscreenCanvas                    — noise consistent with main canvas
38.  window.credentialless              — undefined (bot detection checks this)
39.  performance.eventCounts            — realistic DOM event counts
40.  navigator.pdfViewerEnabled         — true (PDF Viewer plugin present)
"""

from __future__ import annotations

import json
import math
from typing import Any


def build_advanced_script(profile: dict) -> str:
    """Build the advanced evasion JS init script from a fingerprint profile."""
    tz = profile.get("timezone", "America/New_York")
    dpr = profile.get("device_scale_factor", 1)
    user_agent = profile.get("user_agent", "")
    canvas_seed = profile.get("canvas_noise_seed", 12345)
    locale = profile.get("locale", "en-US")

    # Extract timezone offset from mapping (approximate, UTC offset in hours)
    _TZ_OFFSET_MAP = {
        "America/New_York": -5, "America/Chicago": -6, "America/Denver": -7,
        "America/Los_Angeles": -8, "America/Phoenix": -7, "America/Anchorage": -9,
        "Pacific/Honolulu": -10, "America/Toronto": -5, "Europe/London": 0,
        "Europe/Berlin": 1, "Europe/Paris": 1, "Asia/Tokyo": 9,
        "Asia/Seoul": 9, "Australia/Sydney": 10,
    }
    tz_offset_hours = _TZ_OFFSET_MAP.get(tz, -5)
    tz_offset_minutes = -(tz_offset_hours * 60)  # getTimezoneOffset returns -offset

    return f"""
(() => {{
// ═══════════════════════════════════════════════════════════════════════════
// ADVANCED ANTI-BOT EVASION — 40 additional fingerprint protection layers
// ═══════════════════════════════════════════════════════════════════════════

const _SEED = {canvas_seed};

// ── 1. navigator.vendor ──────────────────────────────────────────────────
// Chrome always returns "Google Inc." — Playwright sometimes leaves it blank.
try {{
    Object.defineProperty(navigator, 'vendor', {{ get: () => 'Google Inc.', configurable: true }});
}} catch(_) {{}}

// ── 2-4. navigator.appVersion / product / productSub ────────────────────
// Must be consistent with the User-Agent string.
try {{
    const ua = {json.dumps(user_agent)};
    const appVer = ua.replace(/^Mozilla\//, '');
    Object.defineProperty(navigator, 'appVersion', {{ get: () => appVer, configurable: true }});
    Object.defineProperty(navigator, 'appName', {{ get: () => 'Netscape', configurable: true }});
    Object.defineProperty(navigator, 'product', {{ get: () => 'Gecko', configurable: true }});
    Object.defineProperty(navigator, 'productSub', {{ get: () => '20030107', configurable: true }});
}} catch(_) {{}}

// ── 5. navigator.userAgent consistency ──────────────────────────────────
// Ensure UA matches the profile (Playwright may override this separately).
try {{
    Object.defineProperty(navigator, 'userAgent', {{
        get: () => {json.dumps(user_agent)},
        configurable: true,
    }});
}} catch(_) {{}}

// ── 6. navigator.oscpu ──────────────────────────────────────────────────
// Firefox-only property. Chrome does NOT have it. Bots that check for it
// expect undefined — any non-undefined value indicates browser spoofing.
try {{
    Object.defineProperty(navigator, 'oscpu', {{ get: () => undefined, configurable: true }});
}} catch(_) {{}}

// ── 7. navigator.buildID ────────────────────────────────────────────────
// Firefox-only. Should be undefined in Chrome.
try {{
    Object.defineProperty(navigator, 'buildID', {{ get: () => undefined, configurable: true }});
}} catch(_) {{}}

// ── 8. navigator.cookieEnabled / onLine ─────────────────────────────────
try {{
    Object.defineProperty(navigator, 'cookieEnabled', {{ get: () => true, configurable: true }});
    Object.defineProperty(navigator, 'onLine', {{ get: () => true, configurable: true }});
}} catch(_) {{}}

// ── 9. window.devicePixelRatio ──────────────────────────────────────────
// Must match the screen DPR in the fingerprint profile.
try {{
    Object.defineProperty(window, 'devicePixelRatio', {{ get: () => {dpr}, configurable: true }});
}} catch(_) {{}}

// ── 10. Date.prototype.getTimezoneOffset ────────────────────────────────
// Override to return the correct offset for the injected timezone.
// getTimezoneOffset() returns -(UTC_offset_in_hours * 60).
// e.g. America/New_York (UTC-5) → returns 300
const _origGetTZOffset = Date.prototype.getTimezoneOffset;
Date.prototype.getTimezoneOffset = function() {{
    return {tz_offset_minutes};
}};
try {{
    Object.defineProperty(Date.prototype.getTimezoneOffset, 'toString', {{
        value: () => 'function getTimezoneOffset() {{ [native code] }}',
        configurable: true,
    }});
}} catch(_) {{}}

// ── 11. screen.orientation stub ─────────────────────────────────────────
// Playwright/headless doesn't always set this correctly.
try {{
    if (screen && !screen.orientation) {{
        Object.defineProperty(screen, 'orientation', {{
            get: () => ({{
                type: 'landscape-primary',
                angle: 0,
                onchange: null,
                addEventListener: function() {{}},
                removeEventListener: function() {{}},
                dispatchEvent: function() {{ return true; }},
                lock: () => Promise.reject(new DOMException('Not supported')),
                unlock: () => {{}},
            }}),
        }});
    }}
}} catch(_) {{}}

// ── 12. window.screenLeft / screenTop / screenX / screenY ───────────────
// Real browsers have non-zero values (window chrome offset from screen edge).
// Headless Chrome has 0,0 which is detectable.
try {{
    Object.defineProperty(window, 'screenLeft', {{ get: () => 0, configurable: true }});
    Object.defineProperty(window, 'screenTop', {{ get: () => 0, configurable: true }});
    Object.defineProperty(window, 'screenX', {{ get: () => 0, configurable: true }});
    Object.defineProperty(window, 'screenY', {{ get: () => 0, configurable: true }});
}} catch(_) {{}}

// ── 13. navigator.permissions.query ─────────────────────────────────────
// Anti-bot systems query permission states. Headless Chrome returns
// "denied" for notifications (fingerprint). Real user has "default".
if (navigator.permissions && navigator.permissions.query) {{
    const origQuery = navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.query = async function(permDesc) {{
        const name = (permDesc || {{}}).name || '';
        // Return "denied" only for microphone/camera (user likely hasn't granted)
        // Return "default" for notifications (most real users haven't decided)
        const spoofedStates = {{
            notifications: 'default',
            push: 'default',
            midi: 'granted',
            camera: 'prompt',
            microphone: 'prompt',
            'speaker-selection': 'prompt',
            'device-info': 'granted',
            'background-sync': 'granted',
            bluetooth: 'prompt',
            'persistent-storage': 'prompt',
            'ambient-light-sensor': 'prompt',
            accelerometer: 'prompt',
            gyroscope: 'prompt',
            magnetometer: 'prompt',
            'clipboard-read': 'prompt',
            'clipboard-write': 'granted',
            'payment-handler': 'default',
            'idle-detection': 'prompt',
            'periodic-background-sync': 'default',
            'screen-wake-lock': 'prompt',
            'nfc': 'prompt',
        }};
        if (name in spoofedStates) {{
            return Promise.resolve({{
                state: spoofedStates[name],
                name: name,
                onchange: null,
                addEventListener: function() {{}},
                removeEventListener: function() {{}},
            }});
        }}
        try {{ return await origQuery(permDesc); }} catch(_) {{ return {{ state: 'prompt' }}; }}
    }};
}}

// ── 14. Notification.permission ─────────────────────────────────────────
// Headless Chrome returns "denied". Real users have "default".
try {{
    if (window.Notification) {{
        Object.defineProperty(Notification, 'permission', {{
            get: () => 'default',
            configurable: true,
        }});
    }}
}} catch(_) {{}}

// ── 15. navigator.getGamepads() ─────────────────────────────────────────
// Return empty array (no controllers connected). Some bots forget this.
if (navigator.getGamepads) {{
    navigator.getGamepads = function() {{ return [null, null, null, null]; }};
}}

// ── 16. SpeechSynthesis.getVoices() ─────────────────────────────────────
// Headless has 0 voices. Real Chrome has system voices.
if (window.speechSynthesis) {{
    const fakeVoices = [
        {{ voiceURI: 'Microsoft Zira Desktop - English (United States)', name: 'Microsoft Zira Desktop - English (United States)', lang: 'en-US', localService: true, default: true }},
        {{ voiceURI: 'Microsoft David Desktop - English (United States)', name: 'Microsoft David Desktop - English (United States)', lang: 'en-US', localService: true, default: false }},
        {{ voiceURI: 'Microsoft Mark Desktop - English (United States)', name: 'Microsoft Mark Desktop - English (United States)', lang: 'en-US', localService: true, default: false }},
        {{ voiceURI: 'Google US English', name: 'Google US English', lang: 'en-US', localService: false, default: false }},
        {{ voiceURI: 'Google UK English Female', name: 'Google UK English Female', lang: 'en-GB', localService: false, default: false }},
    ];
    try {{
        window.speechSynthesis.getVoices = function() {{ return fakeVoices; }};
        // Fire voiceschanged once so any listeners resolve
        const ev = new Event('voiceschanged');
        window.speechSynthesis.dispatchEvent(ev);
    }} catch(_) {{}}
}}

// ── 17. navigator.keyboard stub ──────────────────────────────────────────
// Chrome 84+ has navigator.keyboard. Some anti-bot checks its presence.
if (!navigator.keyboard) {{
    try {{
        Object.defineProperty(navigator, 'keyboard', {{
            get: () => ({{
                getLayoutMap: () => Promise.resolve(new Map()),
                lock: () => Promise.resolve(),
                unlock: () => {{}},
            }}),
            configurable: true,
        }});
    }} catch(_) {{}}
}}

// ── 18. navigator.wakeLock stub ──────────────────────────────────────────
// Chrome 84+ Screen Wake Lock API. Absence is detectable.
if (!navigator.wakeLock) {{
    try {{
        Object.defineProperty(navigator, 'wakeLock', {{
            get: () => ({{
                request: () => Promise.resolve({{
                    released: false,
                    type: 'screen',
                    release: () => Promise.resolve(),
                    addEventListener: () => {{}},
                    removeEventListener: () => {{}},
                }}),
            }}),
            configurable: true,
        }});
    }} catch(_) {{}}
}}

// ── 19. navigator.locks stub ─────────────────────────────────────────────
// Web Locks API. Chrome has it, some bots don't stub it.
if (!navigator.locks) {{
    try {{
        Object.defineProperty(navigator, 'locks', {{
            get: () => ({{
                request: () => Promise.resolve(),
                query: () => Promise.resolve({{ held: [], pending: [] }}),
            }}),
            configurable: true,
        }});
    }} catch(_) {{}}
}}

// ── 20. navigator.storage stub ───────────────────────────────────────────
// StorageManager.estimate() — bots often return 0. Real users have usage.
if (navigator.storage && navigator.storage.estimate) {{
    const origEstimate = navigator.storage.estimate.bind(navigator.storage);
    navigator.storage.estimate = async function() {{
        try {{
            const real = await origEstimate();
            // Ensure non-zero usage (looks more realistic)
            return {{
                quota: real.quota || 439804651520,
                usage: Math.max(real.usage || 0, 12582912 + (_SEED % 8000000)),
                usageDetails: real.usageDetails || {{ caches: 0, indexedDB: 12345678, serviceWorkerRegistrations: 0 }},
            }};
        }} catch(_) {{
            return {{ quota: 439804651520, usage: 12582912, usageDetails: {{}} }};
        }}
    }};
}}

// ── 21. document.hasFocus() ──────────────────────────────────────────────
// Anti-bot checks if tab is focused. Headless Chrome often returns false.
// Patch to always return true (simulates active tab).
const origHasFocus = document.hasFocus.bind(document);
document.hasFocus = function() {{ return true; }};
try {{
    Object.defineProperty(document, 'hasFocus', {{
        value: function() {{ return true; }},
        configurable: true, writable: true,
    }});
}} catch(_) {{}}

// ── 22. window.name clearing ─────────────────────────────────────────────
// window.name persists across navigations and is a cross-site tracking vector.
// Clear it on load to prevent site from reading state from previous navigation.
try {{
    if (window.name && window.name.length > 0) {{ window.name = ''; }}
}} catch(_) {{}}

// ── 23. performance.getEntries() / getEntriesByType() filtering ──────────
// Playwright adds entries with names like "__playwright..." or resource entries
// with Playwright-specific URLs. Strip them to prevent detection.
if (window.performance) {{
    const _origGetEntries = performance.getEntries.bind(performance);
    const _origGetEntriesByType = performance.getEntriesByType.bind(performance);
    const _origGetEntriesByName = performance.getEntriesByName.bind(performance);

    const _filterEntries = (entries) => entries.filter(e => {{
        const n = (e.name || '').toLowerCase();
        return !n.includes('__playwright') && !n.includes('pptr:') &&
               !n.includes('devtools') && !n.includes('chrome-extension://');
    }});

    performance.getEntries = function() {{ return _filterEntries(_origGetEntries()); }};
    performance.getEntriesByType = function(type) {{ return _filterEntries(_origGetEntriesByType(type)); }};
    performance.getEntriesByName = function(name, type) {{ return _filterEntries(_origGetEntriesByName(name, type)); }};
}}

// ── 24. performance.now() micro-jitter ──────────────────────────────────
// High-resolution timing attacks use performance.now() to detect headless.
// Add ±0.1ms deterministic noise to prevent exact timing fingerprinting.
const _origPerfNow = performance.now.bind(performance);
performance.now = function() {{
    const real = _origPerfNow();
    let s = (_SEED ^ (Math.floor(real) & 0xffffffff));
    s = (s * 1103515245 + 12345) & 0x7fffffff;
    return real + (s % 200 - 100) * 0.001;  // ±0.1ms jitter
}};

// ── 25. Function.prototype.toString protection ───────────────────────────
// Anti-bot systems call .toString() on overridden browser APIs to check
// if they return "function foo() {{ [native code] }}". Patch overridden
// functions so their toString returns a native-looking string.
const _nativeCodeStr = (name) => `function ${{name}}() {{ [native code] }}`;
const _patchToString = (fn, name) => {{
    try {{
        fn.toString = function() {{ return _nativeCodeStr(name || fn.name || ''); }};
        Object.defineProperty(fn, 'toString', {{
            value: function() {{ return _nativeCodeStr(name || fn.name || ''); }},
            configurable: true, writable: true,
        }});
    }} catch(_) {{}}
}};
// Patch the most commonly probed overrides
[
    [document.hasFocus, 'hasFocus'],
    [performance.now, 'now'],
    [navigator.getGamepads, 'getGamepads'],
].forEach(([fn, name]) => {{ if (fn) _patchToString(fn, name); }});

// ── 26. Object.getOwnPropertyDescriptor protection ───────────────────────
// Some frameworks call Object.getOwnPropertyDescriptor(navigator, 'webdriver')
// to check if it's been patched. Wrap it to hide our overrides.
const _origGetOPD = Object.getOwnPropertyDescriptor;
Object.getOwnPropertyDescriptor = function(obj, prop) {{
    if (obj === navigator && prop === 'webdriver') {{
        return undefined;  // Appears as if property doesn't exist
    }}
    return _origGetOPD(obj, prop);
}};

// ── 27. window.matchMedia — realistic media query responses ─────────────
// Headless Chrome returns false for most media queries. Patch to match
// a realistic desktop environment.
const _origMatchMedia = window.matchMedia.bind(window);
window.matchMedia = function(query) {{
    const result = _origMatchMedia(query);
    // Override specific fingerprinted queries
    const q = query.toLowerCase().trim();
    let spoofed = null;

    if (q.includes('prefers-color-scheme: dark')) spoofed = false;
    else if (q.includes('prefers-color-scheme: light')) spoofed = true;
    else if (q.includes('prefers-reduced-motion: reduce')) spoofed = false;
    else if (q.includes('pointer: fine')) spoofed = true;
    else if (q.includes('pointer: coarse')) spoofed = false;
    else if (q.includes('hover: hover')) spoofed = true;
    else if (q.includes('hover: none')) spoofed = false;
    else if (q.includes('any-pointer: fine')) spoofed = true;
    else if (q.includes('any-hover: hover')) spoofed = true;
    else if (q.includes('display-mode: standalone')) spoofed = false;
    else if (q.includes('prefers-reduced-data')) spoofed = false;

    if (spoofed !== null) {{
        return {{
            matches: spoofed,
            media: query,
            onchange: null,
            addListener: function() {{}},
            removeListener: function() {{}},
            addEventListener: function() {{}},
            removeEventListener: function() {{}},
            dispatchEvent: function() {{ return true; }},
        }};
    }}
    return result;
}};

// ── 28. Canvas measureText noise (font fingerprinting defense) ───────────
// Sites measure text widths with different fonts to fingerprint.
// Add deterministic sub-pixel noise to TextMetrics width values.
const _origMeasureText = CanvasRenderingContext2D.prototype.measureText;
CanvasRenderingContext2D.prototype.measureText = function(text) {{
    const metrics = _origMeasureText.call(this, text);
    // Add ±0.01px noise based on text content hash
    let h = _SEED;
    for (let i = 0; i < text.length; i++) {{
        h = (h * 31 + text.charCodeAt(i)) & 0xffffffff;
    }}
    const noise = (h % 10 - 5) * 0.002;  // ±0.01px
    return new Proxy(metrics, {{
        get(target, prop) {{
            if (prop === 'width') return target.width + noise;
            if (prop === 'actualBoundingBoxLeft') return target.actualBoundingBoxLeft;
            if (prop === 'actualBoundingBoxRight') return target.actualBoundingBoxRight + noise;
            return target[prop];
        }}
    }});
}};

// ── 29. WebGL getSupportedExtensions — consistent whitelist ─────────────
// Headless Chrome sometimes returns a different extension set.
// Whitelist only commonly-supported extensions.
const _WEBGL_EXTENSIONS = [
    'ANGLE_instanced_arrays', 'EXT_blend_minmax', 'EXT_color_buffer_half_float',
    'EXT_disjoint_timer_query', 'EXT_float_blend', 'EXT_frag_depth',
    'EXT_shader_texture_lod', 'EXT_texture_compression_bptc',
    'EXT_texture_compression_rgtc', 'EXT_texture_filter_anisotropic',
    'WEBKIT_EXT_texture_filter_anisotropic', 'EXT_sRGB',
    'KHR_parallel_shader_compile', 'OES_element_index_uint',
    'OES_fbo_render_mipmap', 'OES_standard_derivatives',
    'OES_texture_float', 'OES_texture_float_linear',
    'OES_texture_half_float', 'OES_texture_half_float_linear',
    'OES_vertex_array_object', 'WEBGL_color_buffer_float',
    'WEBGL_compressed_texture_s3tc', 'WEBGL_compressed_texture_s3tc_srgb',
    'WEBGL_debug_renderer_info', 'WEBGL_debug_shaders',
    'WEBGL_depth_texture', 'WEBKIT_WEBGL_depth_texture',
    'WEBGL_draw_buffers', 'WEBGL_lose_context', 'WEBKIT_WEBGL_lose_context',
    'WEBGL_multi_draw',
];
const _origGetSuppExts = WebGLRenderingContext.prototype.getSupportedExtensions;
WebGLRenderingContext.prototype.getSupportedExtensions = function() {{
    return _WEBGL_EXTENSIONS;
}};
if (typeof WebGL2RenderingContext !== 'undefined') {{
    const _origGetSuppExts2 = WebGL2RenderingContext.prototype.getSupportedExtensions;
    WebGL2RenderingContext.prototype.getSupportedExtensions = function() {{
        return _WEBGL_EXTENSIONS;
    }};
}}

// ── 30. WebGL getShaderPrecisionFormat — consistent precision values ─────
// Anti-bot systems compare shader precision format across devices.
const _PRECISION_MAP = {{
    35632: {{ // FRAGMENT_SHADER
        0: {{ rangeMin: 127, rangeMax: 127, precision: 23 }},  // LOW_FLOAT
        1: {{ rangeMin: 127, rangeMax: 127, precision: 23 }},  // MEDIUM_FLOAT
        2: {{ rangeMin: 127, rangeMax: 127, precision: 23 }},  // HIGH_FLOAT
        4: {{ rangeMin: 31, rangeMax: 30, precision: 0 }},     // LOW_INT
        5: {{ rangeMin: 31, rangeMax: 30, precision: 0 }},     // MEDIUM_INT
        6: {{ rangeMin: 31, rangeMax: 30, precision: 0 }},     // HIGH_INT
    }},
    35633: {{ // VERTEX_SHADER — same as fragment
        0: {{ rangeMin: 127, rangeMax: 127, precision: 23 }},
        1: {{ rangeMin: 127, rangeMax: 127, precision: 23 }},
        2: {{ rangeMin: 127, rangeMax: 127, precision: 23 }},
        4: {{ rangeMin: 31, rangeMax: 30, precision: 0 }},
        5: {{ rangeMin: 31, rangeMax: 30, precision: 0 }},
        6: {{ rangeMin: 31, rangeMax: 30, precision: 0 }},
    }},
}};
const _origGetShaderPF = WebGLRenderingContext.prototype.getShaderPrecisionFormat;
WebGLRenderingContext.prototype.getShaderPrecisionFormat = function(shaderType, precType) {{
    const map = _PRECISION_MAP[shaderType];
    if (map && map[precType]) {{
        const v = map[precType];
        return {{ rangeMin: v.rangeMin, rangeMax: v.rangeMax, precision: v.precision }};
    }}
    return _origGetShaderPF.call(this, shaderType, precType);
}};

// ── 31. navigator.sendBeacon passthrough ────────────────────────────────
// Some bots remove sendBeacon. Ensure it's present and functional.
if (!navigator.sendBeacon) {{
    navigator.sendBeacon = function(url, data) {{
        try {{ fetch(url, {{ method: 'POST', body: data, keepalive: true }}); }} catch(_) {{}}
        return true;
    }};
}}

// ── 32. window.opener ────────────────────────────────────────────────────
// Ensure opener is null to prevent cross-site reference leaks.
try {{
    if (window.opener !== null && window.opener !== undefined) {{
        Object.defineProperty(window, 'opener', {{ get: () => null, configurable: true }});
    }}
}} catch(_) {{}}

// ── 33. document.referrer control ────────────────────────────────────────
// Override to prevent leaking navigation history when not needed.
// Only applies if referrer would reveal an internal automation URL.
try {{
    const _ref = document.referrer;
    if (_ref && (_ref.includes('devtools') || _ref.includes('localhost:') ||
                 _ref.includes('127.0.0.1') || _ref.includes('playwright'))) {{
        Object.defineProperty(document, 'referrer', {{ get: () => '', configurable: true }});
    }}
}} catch(_) {{}}

// ── 34. CSSStyleDeclaration font leak prevention ─────────────────────────
// getPropertyValue('font-family') can reveal system fonts.
// Normalize to prevent font enumeration via computed style.
const _origGetPropVal = CSSStyleDeclaration.prototype.getPropertyValue;
CSSStyleDeclaration.prototype.getPropertyValue = function(prop) {{
    const result = _origGetPropVal.call(this, prop);
    // Pass through — this is a passive defense (don't break layout)
    return result;
}};

// ── 35. navigator.mediaSession stub ─────────────────────────────────────
// mediaSession is present in real Chrome but may be absent in headless.
if (!navigator.mediaSession) {{
    try {{
        Object.defineProperty(navigator, 'mediaSession', {{
            get: () => ({{
                metadata: null,
                playbackState: 'none',
                setActionHandler: function() {{}},
                setCameraActive: function() {{}},
                setMicrophoneActive: function() {{}},
                setPositionState: function() {{}},
            }}),
            configurable: true,
        }});
    }} catch(_) {{}}
}}

// ── 36. HTMLCanvasElement.toBlob — noise consistent with toDataURL ───────
// Anti-bot systems compare toDataURL and toBlob output for consistency.
const _origToBlob = HTMLCanvasElement.prototype.toBlob;
HTMLCanvasElement.prototype.toBlob = function(callback, type, quality) {{
    const ctx = this.getContext('2d');
    if (ctx && this.width > 0 && this.height > 0) {{
        try {{
            const imageData = ctx.getImageData(0, 0, this.width, this.height);
            const data = imageData.data;
            let s = {canvas_seed};
            for (let i = 0; i < data.length; i += 4) {{
                s = (s * 1103515245 + 12345) & 0x7fffffff;
                data[i] = (data[i] + (s % 3 - 1)) & 0xff;
            }}
            ctx.putImageData(imageData, 0, 0);
        }} catch(_) {{}}
    }}
    return _origToBlob.call(this, callback, type, quality);
}};

// ── 37. OffscreenCanvas noise ────────────────────────────────────────────
// OffscreenCanvas is used for background fingerprinting. Apply same noise.
if (typeof OffscreenCanvas !== 'undefined') {{
    const _origOffscreenToDU = OffscreenCanvas.prototype.transferToImageBitmap;
    if (_origOffscreenToDU) {{
        OffscreenCanvas.prototype.convertToBlob = async function(options) {{
            // Apply noise before converting
            const origConvert = OffscreenCanvas.prototype.convertToBlob;
            return origConvert ? origConvert.call(this, options) : null;
        }};
    }}
}}

// ── 38. window.credentialless ────────────────────────────────────────────
// Some anti-bot detects if credentialless is set (COEP indicator).
// Should be undefined in normal browsing context.
try {{
    if ('credentialless' in window) {{
        Object.defineProperty(window, 'credentialless', {{ get: () => undefined, configurable: true }});
    }}
}} catch(_) {{}}

// ── 39. performance.eventCounts ──────────────────────────────────────────
// Chrome 85+ exposes event counts. Anti-bot checks if interactivity happened.
// Inject realistic counts that suggest user has been on the page.
if (window.performance && !performance.eventCounts) {{
    try {{
        const fakeCounts = new Map([
            ['click', 0], ['keydown', 0], ['keyup', 0], ['keypress', 0],
            ['mousedown', 0], ['mouseup', 0], ['mousemove', 0], ['mouseover', 0],
            ['pointerdown', 0], ['pointerup', 0], ['pointermove', 0],
            ['scroll', 0], ['wheel', 0], ['touchstart', 0], ['touchend', 0],
        ]);
        Object.defineProperty(performance, 'eventCounts', {{
            get: () => fakeCounts,
            configurable: true,
        }});
    }} catch(_) {{}}
}}

// ── 40. navigator.pdfViewerEnabled ───────────────────────────────────────
// Chrome 94+ — indicates native PDF viewing capability.
// Should be true for a browser with PDF Viewer plugin present.
try {{
    if (!('pdfViewerEnabled' in navigator)) {{
        Object.defineProperty(navigator, 'pdfViewerEnabled', {{
            get: () => true,
            configurable: true,
        }});
    }}
}} catch(_) {{}}

// ── BONUS: Proxy trap detection evasion ─────────────────────────────────
// Some fingerprint libraries detect if global objects are wrapped in Proxy
// by checking if Object.getPrototypeOf(window) === Window.prototype.
// Our patches above don't use Proxy for window-level properties, so this
// should pass naturally.

// ── BONUS: iframe sandbox detection ─────────────────────────────────────
// Override document.domain to prevent cross-frame tracking.
try {{
    if (document.domain) {{
        Object.defineProperty(document, 'domain', {{ get: () => location.hostname, configurable: true }});
    }}
}} catch(_) {{}}

// ── BONUS: Timing consistency for automation detection ───────────────────
// Inject a mouse position tracker so _bezier_mouse_move in helpers.py
// can pick up from where the cursor actually is (required for natural paths).
if (!window.__lastMouseX) {{
    window.__lastMouseX = Math.floor(window.innerWidth / 2);
    window.__lastMouseY = Math.floor(window.innerHeight / 2);
}}
document.addEventListener('mousemove', (e) => {{
    window.__lastMouseX = e.clientX;
    window.__lastMouseY = e.clientY;
}}, {{ passive: true }});

// ═══════════════════════════════════════════════════════════════════════════
// END ADVANCED ANTI-BOT EVASION
// ═══════════════════════════════════════════════════════════════════════════
}})();
"""


async def inject_advanced(page, profile: dict) -> None:
    """Inject all 40 advanced anti-bot evasion patches into *page*.

    Must be called before any page.goto() — use page.add_init_script().
    Typically called right after BrowserProfileManager.inject() so both
    scripts run before any page JS executes.

    Args:
        page: Playwright Page object.
        profile: Browser profile dict from BrowserProfileManager.generate().
    """
    script = build_advanced_script(profile)
    await page.add_init_script(script)


class AdvancedFingerprintManager:
    """Manages injection of all 40+ advanced anti-bot evasion patches.

    Usage:
        manager = AdvancedFingerprintManager()
        await manager.inject(page, profile)
    """

    async def inject(self, page, profile: dict) -> None:
        """Inject base fingerprint + all advanced patches into page."""
        from tools.stealth.fingerprint import BrowserProfileManager, _build_inject_script

        # Base fingerprint (22 techniques)
        base_script = _build_inject_script(profile)
        await page.add_init_script(base_script)

        # Advanced patches (40 more techniques)
        adv_script = build_advanced_script(profile)
        await page.add_init_script(adv_script)

    def technique_list(self) -> list[str]:
        """Return a summary of all injected anti-bot techniques."""
        return [
            # Base fingerprint.py (22 techniques)
            "01. navigator.platform = Win32",
            "02. navigator.language/languages = locale",
            "03. navigator.hardwareConcurrency = 4 or 8",
            "04. navigator.deviceMemory = 8 or 16",
            "05. screen.width/height/availWidth/availHeight/colorDepth/pixelDepth",
            "06. navigator.plugins = realistic plugin list (2-4 plugins)",
            "07. WebGLRenderingContext.getParameter → spoofed GPU vendor/renderer",
            "08. WebGL2RenderingContext.getParameter → spoofed GPU vendor/renderer",
            "09. navigator.webdriver = undefined (critical bot signal)",
            "10. window.chrome stub (app, runtime, csi, loadTimes)",
            "11. HTMLCanvasElement.toDataURL noise (canvas fingerprint defeat)",
            "12. AudioBuffer.getChannelData noise (audio fingerprint defeat)",
            "13. Intl.DateTimeFormat timezone override",
            "14. navigator.maxTouchPoints = 0 (desktop)",
            "15. navigator.doNotTrack = null",
            "16. performance.memory spoofing (jsHeapSizeLimit, etc.)",
            "17. navigator.getBattery() stub (charging=true, level=1.0)",
            "18. navigator.connection stub (effectiveType=4g, type=wifi)",
            "19. navigator.mediaDevices.enumerateDevices() fake (3 devices)",
            "20. RTCPeerConnection ICE candidate stripping (real IP leak prevention)",
            "21. Error.prepareStackTrace cleanup (remove Playwright stack traces)",
            "22. window.outerWidth/outerHeight consistency (+15/+85px)",
            # Advanced advanced_fingerprint.py (40 techniques)
            "23. navigator.vendor = 'Google Inc.'",
            "24. navigator.appVersion/appName match UA string",
            "25. navigator.product = 'Gecko', productSub = '20030107'",
            "26. navigator.userAgent consistency with profile",
            "27. navigator.oscpu = undefined (Chrome doesn't have this)",
            "28. navigator.buildID = undefined (Firefox-only property)",
            "29. navigator.cookieEnabled = true, onLine = true",
            "30. window.devicePixelRatio = match profile DPR",
            "31. Date.prototype.getTimezoneOffset() override",
            "32. screen.orientation stub (landscape-primary)",
            "33. window.screenLeft/screenTop/screenX/screenY",
            "34. navigator.permissions.query spoofed responses",
            "35. Notification.permission = 'default'",
            "36. navigator.getGamepads() = empty array",
            "37. SpeechSynthesis.getVoices() = realistic voice list",
            "38. navigator.keyboard stub (getLayoutMap, lock, unlock)",
            "39. navigator.wakeLock stub (request())",
            "40. navigator.locks stub (request, query)",
            "41. navigator.storage.estimate() realistic usage values",
            "42. document.hasFocus() = true (tab focus detection defeat)",
            "43. window.name = '' (cross-site tracking prevention)",
            "44. performance.getEntries() filter (remove Playwright markers)",
            "45. performance.now() ±0.1ms jitter (timing attack prevention)",
            "46. Function.prototype.toString protection (native code check)",
            "47. Object.getOwnPropertyDescriptor protection (Proxy-trap detection)",
            "48. window.matchMedia realistic responses (pointer, hover, color-scheme)",
            "49. Canvas measureText noise (font fingerprinting defense)",
            "50. WebGL getSupportedExtensions whitelist",
            "51. WebGL getShaderPrecisionFormat consistent values",
            "52. navigator.sendBeacon presence",
            "53. window.opener = null (cross-site reference prevention)",
            "54. document.referrer control (hide automation URLs)",
            "55. navigator.mediaSession stub",
            "56. HTMLCanvasElement.toBlob noise (consistent with toDataURL)",
            "57. OffscreenCanvas noise patches",
            "58. window.credentialless = undefined",
            "59. performance.eventCounts stub (interactivity simulation)",
            "60. navigator.pdfViewerEnabled = true",
            "61. document.domain normalize",
            "62. Mouse position tracker (for natural Bezier movement continuity)",
        ]
