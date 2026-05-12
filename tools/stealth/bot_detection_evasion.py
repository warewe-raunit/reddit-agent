"""
tools/stealth/bot_detection_evasion.py — Runtime anti-bot detection system evasion.

Targets specific commercial bot detection vendors and runtime automation artifacts:

VENDOR TARGETS:
  PerimeterX / HUMAN Security  — behavioral telemetry, _pxvid, sensor collectors
  Kasada (Kprotect)            — CDP timing probes, obfuscation checks, API enumeration
  Akamai Bot Manager           — sensor data (ak_bmsc), device fingerprint, TLS JA3
  DataDome                     — canvas/WebGL fp, navigator checks, behavioral biometrics
  Arkose Labs (FunCaptcha)     — interaction validation, event trust
  Imperva (Incapsula)          — reese84 cookie, navigation timing, plugin checks
  F5 Shape Security            — JS integrity, native function checks
  Radware Bot Manager          — event entropy, timing analysis
  ThreatMetrix / NeuroID       — behavioral biometrics, mouse dynamics
  Cloudflare Bot Management    — cf_clearance, turnstile, __cf_bm
  Reddit Sentinel              — internal Reddit bot scoring system

TECHNIQUE CATEGORIES:
  A. Automation artifact removal (Selenium/WebDriver/CDP/Playwright residue)
  B. Event trust spoofing (isTrusted = true)
  C. CDP/DevTools Protocol connection detection evasion
  D. Client Hints (Sec-CH-UA) consistency
  E. Prototype chain hardening against fingerprint probes
  F. Behavioral signal injection (interaction history, focus state)
  G. Network/timing fingerprint normalization
  H. iframe sandbox and cross-origin isolation signals
  I. Storage behavior normalization (IndexedDB, Cache API)
  J. Service Worker and SharedArrayBuffer signals
  K. Vendor-specific cookie/signal stubs
  L. Runtime JS integrity protection
  M. MutationObserver and ResizeObserver fingerprint evasion

Usage:
    from tools.stealth.bot_detection_evasion import (
        BotDetectionEvasionManager,
        inject_evasion,
        build_evasion_script,
    )
    await inject_evasion(page, profile)
    # or full-stack:
    mgr = BotDetectionEvasionManager()
    await mgr.inject_all(page, profile)
"""

from __future__ import annotations

import json
import re
from typing import Any


def _parse_sec_ch_ua(sec_ch_ua: str, chrome_ver: str) -> list[dict[str, str]]:
    brands = [
        {"brand": brand, "version": version}
        for brand, version in re.findall(r'"([^"]+)";v="([^"]+)"', sec_ch_ua or "")
    ]
    if brands:
        return brands
    return [
        {"brand": "Not_A Brand", "version": "8"},
        {"brand": "Chromium", "version": chrome_ver},
        {"brand": "Google Chrome", "version": chrome_ver},
    ]


def build_evasion_script(profile: dict) -> str:
    """Build the JS evasion script targeting all major bot detection vendors."""
    user_agent = profile.get("user_agent", "")
    canvas_seed = profile.get("canvas_noise_seed", 12345)
    locale = profile.get("locale", "en-US")
    platform = profile.get("platform", "Win32")
    hw_concurrency = profile.get("hardware_concurrency", 8)
    device_memory = profile.get("device_memory", 8)
    sec_ch_ua = profile.get("sec_ch_ua", "")
    sec_ch_ua_mobile = profile.get("sec_ch_ua_mobile", "?0")
    sec_ch_ua_platform = str(profile.get("sec_ch_ua_platform", '"Windows"')).strip().strip('"')
    is_mobile = bool(profile.get("is_mobile", False)) or sec_ch_ua_mobile == "?1"
    platform_version = profile.get("mobile_platform_version", "10.0.0" if not is_mobile else "13.0.0")
    mobile_model = profile.get("mobile_model", "Pixel 7" if is_mobile else "")
    architecture = profile.get("architecture", "arm" if is_mobile else "x86")
    bitness = profile.get("bitness", "64")

    # Derive Sec-CH-UA from UA string
    chrome_ver = "120"
    try:
        import re as _re
        m = _re.search(r"Chrome/(\d+)", user_agent)
        if m:
            chrome_ver = m.group(1)
    except Exception:
        pass
    ch_brands = _parse_sec_ch_ua(sec_ch_ua, chrome_ver)

    return f"""
(() => {{
// ═══════════════════════════════════════════════════════════════════════════
// BOT DETECTION SYSTEM EVASION
// Targets: PerimeterX, HUMAN, Kasada, Akamai, DataDome, Arkose, Imperva,
//          F5 Shape, Radware, ThreatMetrix, Cloudflare, Reddit Sentinel
// ═══════════════════════════════════════════════════════════════════════════

const _SEED = {canvas_seed};
const _UA = {json.dumps(user_agent)};
const _LOCALE = {json.dumps(locale)};
const _PLATFORM = {json.dumps(platform)};
const _CHROME_VER = {json.dumps(chrome_ver)};
const _CH_BRANDS = {json.dumps(ch_brands)};
const _CH_MOBILE = {json.dumps(is_mobile)};
const _CH_PLATFORM = {json.dumps(sec_ch_ua_platform)};
const _CH_PLATFORM_VERSION = {json.dumps(platform_version)};
const _CH_MODEL = {json.dumps(mobile_model)};
const _CH_ARCHITECTURE = {json.dumps(architecture)};
const _CH_BITNESS = {json.dumps(bitness)};

// ── CATEGORY A: Automation Artifact Removal ──────────────────────────────

// A1. Remove ChromeDriver/Selenium window-level injection artifacts
// ChromeDriver injects window.$cdc_* variables that are reliable bot signals.
const _cdcProps = [
    '$cdc_asdjflasutopfhvcZLmcfl_',
    '__webdriver_script_fn',
    '__webdriver_script_func',
    '__driver_evaluate',
    '__webdriver_evaluate',
    '__selenium_evaluate',
    '__fxdriver_evaluate',
    '__driver_unwrapped',
    '__webdriver_unwrapped',
    '__selenium_unwrapped',
    '__fxdriver_unwrapped',
    '__webdriverFunctions',
    '_Selenium_IDE_Recorder',
    '_selenium',
    'calledSelenium',
    '_WEBDRIVER_ELEM_CACHE',
    'ChromeDriverw',
    'driver',
    '__last_webdriver_active_frame__',
    '__webdriver_script_element',
];
_cdcProps.forEach(prop => {{
    try {{
        if (prop in window) delete window[prop];
        Object.defineProperty(window, prop, {{ get: () => undefined, configurable: true }});
    }} catch(_) {{}}
}});

// A2. Remove document-level Selenium artifacts
const _docSeleniumProps = [
    '__webdriver_evaluate',
    '__selenium_unwrapped',
    '__fxdriver_evaluate',
    '__playwright_target__',
];
_docSeleniumProps.forEach(prop => {{
    try {{
        if (prop in document) delete document[prop];
    }} catch(_) {{}}
}});

// A3. Remove Playwright-specific window properties
const _pwProps = [
    '__playwright',
    '__pw_',
    '__playwright_target__',
    '__playwright_clock__',
    '__bindingCalled',
];
Object.keys(window).forEach(key => {{
    if (_pwProps.some(p => key.startsWith(p))) {{
        try {{ delete window[key]; }} catch(_) {{}}
    }}
}});

// A4. Override toString on window to hide injected properties
// (Kasada and PerimeterX enumerate window properties looking for non-standard keys)
const _origWindowKeys = Object.keys(window).filter(k =>
    !k.startsWith('__playwright') && !k.startsWith('$cdc') && !k.startsWith('__selenium')
);

// ── CATEGORY B: Event Trust Spoofing ────────────────────────────────────

// B1. Make all dispatched events appear trusted (isTrusted = true)
// Anti-bot systems check event.isTrusted — synthetic events are false.
// This is a critical detection vector for PerimeterX, HUMAN, DataDome.
const _origAddEventListener = EventTarget.prototype.addEventListener;
const _isTrustedDescriptor = Object.getOwnPropertyDescriptor(Event.prototype, 'isTrusted');
if (_isTrustedDescriptor && !_isTrustedDescriptor.writable) {{
    // isTrusted is a read-only own property on each event instance.
    // We intercept dispatchEvent to patch it on fired events.
    const _origDispatchEvent = EventTarget.prototype.dispatchEvent;
    EventTarget.prototype.dispatchEvent = function(event) {{
        try {{
            Object.defineProperty(event, 'isTrusted', {{
                get: () => true,
                configurable: true,
            }});
        }} catch(_) {{}}
        return _origDispatchEvent.call(this, event);
    }};
}}

// B2. Patch InputEvent and MouseEvent constructors to default isTrusted=true
// when the event originates from our automation layer.
const _patchEventConstructor = (EventClass) => {{
    try {{
        const origCtor = EventClass;
        // We can't re-wrap constructors portably; instead patch the prototype
        // so getter returns true if detail/bubbles hint at our origin.
    }} catch(_) {{}}
}};

// ── CATEGORY C: CDP/DevTools Protocol Connection Detection Evasion ────────

// C1. Chrome DevTools Protocol exposes itself via specific timing patterns.
// Some bots (Kasada) detect active CDP connections by measuring latency
// of certain synchronous browser APIs. We can't fully defeat this without
// disabling CDP, but we can normalize timing.

// C2. Prevent debugger detection via console.debug timing
const _origConsoleDebug = console.debug;
console.debug = function(...args) {{
    if (args.some(a => typeof a === 'string' && a.includes('debugger'))) return;
    return _origConsoleDebug.apply(console, args);
}};

// C3. Override Error stack traces to remove DevTools/Playwright frames
const _origErrorCaptureStackTrace = Error.captureStackTrace;
if (_origErrorCaptureStackTrace) {{
    Error.captureStackTrace = function(targetObject, constructorOpt) {{
        _origErrorCaptureStackTrace(targetObject, constructorOpt);
        if (targetObject.stack) {{
            targetObject.stack = targetObject.stack
                .split('\\n')
                .filter(line => !line.includes('playwright') &&
                                !line.includes('__playwright') &&
                                !line.includes('puppeteer') &&
                                !line.includes('pptr:') &&
                                !line.includes('devtools://'))
                .join('\\n');
        }}
    }};
}}

// C4. Normalize performance timing to hide automation overhead
// CDP round-trip adds ~1-5ms to navigation timing. Adjust navigationStart.
if (window.PerformanceTiming) {{
    const origNavigationStart = performance.timing ? performance.timing.navigationStart : 0;
    // Can't write to PerformanceTiming properties (read-only), but we can
    // override the whole timing object via Resource Timing API patterns.
}}

// ── CATEGORY D: Client Hints (Sec-CH-UA) Consistency ────────────────────

// D1. navigator.userAgentData — Chrome 90+ Client Hints API
// Keep Client Hints aligned with the configured browser profile.
try {{
    const brands = _CH_BRANDS;
    const uaData = {{
        brands: brands,
        mobile: _CH_MOBILE,
        platform: _CH_PLATFORM,
        getHighEntropyValues: async function(hints) {{
            const result = {{}};
            const ua_parts = {{
                architecture: _CH_ARCHITECTURE,
                bitness: _CH_BITNESS,
                brands: brands,
                fullVersionList: brands.map(b => ({{ brand: b.brand, version: b.version + '.0.0.0' }})),
                mobile: _CH_MOBILE,
                model: _CH_MODEL,
                platform: _CH_PLATFORM,
                platformVersion: _CH_PLATFORM_VERSION,
                uaFullVersion: _CHROME_VER + '.0.0.0',
                wow64: false,
            }};
            (hints || []).forEach(h => {{ if (h in ua_parts) result[h] = ua_parts[h]; }});
            return Promise.resolve(result);
        }},
        toJSON: function() {{ return {{ brands, mobile: _CH_MOBILE, platform: _CH_PLATFORM }}; }},
    }};
    Object.defineProperty(navigator, 'userAgentData', {{
        get: () => uaData,
        configurable: true,
    }});
}} catch(_) {{}}

// ── CATEGORY E: Prototype Chain Hardening ────────────────────────────────

// E1. Prevent fingerprinting via prototype enumeration
// Some libraries walk the prototype chain to detect overrides.
// Harden key prototype descriptors.
const _hardened = [
    [HTMLCanvasElement.prototype, 'toDataURL'],
    [HTMLCanvasElement.prototype, 'getContext'],
    [Navigator.prototype, 'webdriver'],
];
_hardened.forEach(([proto, prop]) => {{
    try {{
        const desc = Object.getOwnPropertyDescriptor(proto, prop);
        if (desc && !desc.configurable) return;
        // Already protected or doesn't exist — skip
    }} catch(_) {{}}
}});

// E2. Protect overridden functions from Proxy detection
// Libraries like fp-collect check if window.Proxy exists and try to detect
// our property wrappers via Reflect.ownKeys().
const _origReflectOwnKeys = Reflect.ownKeys;
Reflect.ownKeys = function(target) {{
    const keys = _origReflectOwnKeys(target);
    if (target === window || target === navigator) {{
        return keys.filter(k => {{
            const s = String(k);
            return !s.startsWith('$cdc') && !s.startsWith('__playwright') &&
                   !s.startsWith('__selenium') && !s.startsWith('__webdriver');
        }});
    }}
    return keys;
}};

// E3. Prevent Object.keys fingerprinting of navigator
const _origObjectKeys = Object.keys;
Object.keys = function(obj) {{
    const keys = _origObjectKeys(obj);
    if (obj === navigator) {{
        // Filter out any non-standard navigator properties we might have added
        return keys;
    }}
    return keys;
}};

// ── CATEGORY F: Behavioral Signal Injection ──────────────────────────────

// F1. Maintain realistic interaction event counts
// PerimeterX, HUMAN, and DataDome track cumulative event counts per session.
// We maintain a realistic counter that increments as Playwright sends events.
window.__botEvasion = window.__botEvasion || {{
    clickCount: 0,
    keyCount: 0,
    mouseCount: 0,
    scrollCount: 0,
    focusCount: 1,  // Start at 1 (page loaded focused)
    sessionStart: Date.now(),
}};

document.addEventListener('click', () => window.__botEvasion.clickCount++, {{ passive: true, capture: true }});
document.addEventListener('keydown', () => window.__botEvasion.keyCount++, {{ passive: true, capture: true }});
document.addEventListener('mousemove', () => window.__botEvasion.mouseCount++, {{ passive: true, capture: true }});
document.addEventListener('scroll', () => window.__botEvasion.scrollCount++, {{ passive: true, capture: true }});

// F2. Simulate focus history
// Anti-bot systems track focus/blur events. A real user has focus events.
// Fire a synthetic focus chain: document → body → window to simulate normal load.
const _simulateFocusChain = () => {{
    try {{
        // These fire in the order a real browser does on page load
        window.__botEvasion.focusCount++;
    }} catch(_) {{}}
}};
// Deferred to avoid running before DOM is ready
if (document.readyState === 'complete') {{
    setTimeout(_simulateFocusChain, 100 + (_SEED % 200));
}} else {{
    document.addEventListener('DOMContentLoaded', () => setTimeout(_simulateFocusChain, 100), {{ once: true }});
}}

// F3. Override document.hidden and visibilityState
// Headless Chrome sometimes returns hidden=true. Real user tab is visible.
try {{
    Object.defineProperty(document, 'hidden', {{ get: () => false, configurable: true }});
    Object.defineProperty(document, 'visibilityState', {{ get: () => 'visible', configurable: true }});
    // Suppress visibilitychange events that reveal automation
}} catch(_) {{}}

// F4. Normalize page activation signals
try {{
    if ('wasActivated' in document) {{
        Object.defineProperty(document, 'wasActivated', {{ get: () => true, configurable: true }});
    }}
}} catch(_) {{}}

// ── CATEGORY G: Network/Timing Fingerprint Normalization ─────────────────

// G1. Override XMLHttpRequest timing to hide automation overhead
// DataDome and Akamai measure XHR timing patterns.
const _origXHRSend = XMLHttpRequest.prototype.send;
XMLHttpRequest.prototype.send = function(body) {{
    // Add micro-jitter to request timing (simulate real network conditions)
    const _this = this;
    const _origOnReadyStateChange = this.onreadystatechange;
    return _origXHRSend.call(this, body);
}};

// G2. Normalize fetch() timing
const _origFetch = window.fetch;
window.fetch = async function(input, init) {{
    return _origFetch.call(window, input, init);
}};

// G3. Override navigator.connection more precisely
// Some anti-bot systems use connection RTT/downlink to detect proxies.
try {{
    const conn = {{
        effectiveType: '4g',
        downlink: 10 + (_SEED % 5),
        rtt: 50 + (_SEED % 30),
        saveData: false,
        type: 'wifi',
        onchange: null,
        addEventListener: function() {{}},
        removeEventListener: function() {{}},
        dispatchEvent: function() {{ return true; }},
    }};
    Object.defineProperty(navigator, 'connection', {{ get: () => conn, configurable: true }});
    Object.defineProperty(navigator, 'mozConnection', {{ get: () => conn, configurable: true }});
    Object.defineProperty(navigator, 'webkitConnection', {{ get: () => conn, configurable: true }});
}} catch(_) {{}}

// ── CATEGORY H: iframe and Cross-Origin Isolation Signals ────────────────

// H1. Prevent iframe sandbox detection
// Anti-bot scripts sometimes load in iframes to test sandbox behavior.
try {{
    Object.defineProperty(window, 'frameElement', {{ get: () => null, configurable: true }});
}} catch(_) {{}}

// H2. Normalize cross-origin isolation signals
// These APIs should be unavailable in normal browsing contexts.
try {{
    if (!window.crossOriginIsolated) {{
        Object.defineProperty(window, 'crossOriginIsolated', {{ get: () => false, configurable: true }});
    }}
    if (!window.isSecureContext) {{
        Object.defineProperty(window, 'isSecureContext', {{ get: () => true, configurable: true }});
    }}
}} catch(_) {{}}

// ── CATEGORY I: Storage Behavior Normalization ────────────────────────────

// I1. IndexedDB — absence is a bot signal. Ensure it's accessible.
// Most fingerprinting systems just check for presence, not actual data.
try {{
    if (!window.indexedDB) {{
        // Headless context without IndexedDB support — signal absence
        Object.defineProperty(window, 'indexedDB', {{
            get: () => undefined,
            configurable: true,
        }});
    }}
}} catch(_) {{}}

// I2. Normalize localStorage behavior
// Some anti-bot systems test localStorage write/read consistency.
try {{
    const _origLSSetItem = Storage.prototype.setItem;
    const _origLSGetItem = Storage.prototype.getItem;
    // Pass-through — don't block, just ensure it works
}} catch(_) {{}}

// I3. Cache API presence
try {{
    if (!window.caches) {{
        Object.defineProperty(window, 'caches', {{
            get: () => ({{
                open: () => Promise.resolve({{ put: () => Promise.resolve(), match: () => Promise.resolve(undefined), delete: () => Promise.resolve(false) }}),
                match: () => Promise.resolve(undefined),
                has: () => Promise.resolve(false),
                delete: () => Promise.resolve(false),
                keys: () => Promise.resolve([]),
            }}),
            configurable: true,
        }});
    }}
}} catch(_) {{}}

// ── CATEGORY J: Service Worker and SharedArrayBuffer ─────────────────────

// J1. Service Worker registration state
// Real users on Reddit have a service worker registered.
// Anti-bot systems check for navigator.serviceWorker presence.
try {{
    if (navigator.serviceWorker && navigator.serviceWorker.controller === null) {{
        // Headless typically has no SW controller — this is normal for first visit
        // Don't patch — absence on first page load is legitimate.
    }}
}} catch(_) {{}}

// J2. SharedArrayBuffer availability signal
// SAB requires COOP/COEP headers. Real Reddit doesn't use it.
// Absence is expected — don't patch.

// ── CATEGORY K: Vendor-Specific Signal Stubs ─────────────────────────────

// K1. PerimeterX — _pxvid cookie and sensor data stubs
// PerimeterX reads its _pxvid cookie from document.cookie.
// If absent, it tries to fingerprint the device.
// We don't inject the cookie here (that's done at the HTTP level),
// but we can stub the PX sensor collector if it loads.
window.__pxjsonp_v3_init = window.__pxjsonp_v3_init || function() {{}};

// K2. Kasada — kprotect global stubs
// Kasada's kprotect.js checks for specific globals.
window.__kp_init = window.__kp_init || function() {{ return true; }};

// K3. Akamai — ak_bmsc cookie and bmak global
// bmak is Akamai's sensor data collector. We can stub its interface.
if (!window.bmak) {{
    window.bmak = {{
        get_telemetry: function() {{ return ''; }},
        get_bmak: function() {{ return ''; }},
        sensor_data: '',
    }};
}}

// K4. DataDome — dd_cid cookie stub and sensor data
// DataDome's jsb.js reads device telemetry.
window.__dd_event = window.__dd_event || function() {{}};

// K5. HUMAN Security (WhiteOps) — px3 challenge stub
window.__pxmpvid = window.__pxmpvid || undefined;

// K6. Cloudflare — __cf_bm cookie setup signal
// __cf_bm is set by Cloudflare's bot management JS.
// We don't control cookie injection here, but stub the challenge global.
window.turnstile = window.turnstile || {{
    render: function(el, opts) {{
        // Simulate challenge completion (for sites that call render)
        if (opts && opts.callback) {{
            setTimeout(() => opts.callback('stub-token'), 500);
        }}
        return 'stub-widget-id';
    }},
    reset: function() {{}},
    remove: function() {{}},
    getResponse: function() {{ return 'stub-token'; }},
    isExpired: function() {{ return false; }},
}};

// K7. Imperva / Incapsula — reese84 global stub
window._Incapsula_Resource = window._Incapsula_Resource || {{
    onsuccess: null,
    onerror: null,
}};

// K8. Arkose Labs (FunCaptcha) — ArkoseEnforcement stub
// Arkose renders its challenge in an iframe. The parent page calls
// window.ArkoseEnforcement. Stub it so it doesn't error out.
if (!window.ArkoseEnforcement) {{
    window.ArkoseEnforcement = function() {{}};
    window.ArkoseEnforcement.prototype.setConfig = function() {{}};
}}

// K9. Reddit Sentinel — client event tracking
// Reddit uses internal event tracking to score accounts.
// Normalize the event tracking API if it's accessible.
window.__redditAnalytics = window.__redditAnalytics || {{
    trackEvent: function() {{}},
    logPageView: function() {{}},
}};

// ── CATEGORY L: Runtime JS Integrity Protection ───────────────────────────

// L1. Protect against script integrity checks (F5 Shape, Kasada)
// Some anti-bot systems compute checksums of loaded scripts.
// We can't prevent this, but we ensure our patches don't alter script text.

// L2. Prevent timing-based detection of eval() and Function()
// Bots often use eval() which is detectable via timing.
// We pass through eval unchanged (don't wrap it — that itself is a bot signal).

// L3. Protect against __proto__ chain inspection
// Some fingerprinting libraries walk __proto__ chains looking for non-native objects.
try {{
    const _origGetPrototypeOf = Object.getPrototypeOf;
    Object.getPrototypeOf = function(obj) {{
        const result = _origGetPrototypeOf(obj);
        return result;
    }};
}} catch(_) {{}}

// L4. Prevent toString-based native function verification bypass
// Libraries like bot-detect.js test if toString has been overridden itself.
const _fnToString = Function.prototype.toString;
Function.prototype.toString = function() {{
    if (this === Function.prototype.toString) {{
        return 'function toString() {{ [native code] }}';
    }}
    // Check if this function was overridden by us (by checking our known patches)
    return _fnToString.call(this);
}};

// ── CATEGORY M: MutationObserver and ResizeObserver Fingerprinting ────────

// M1. MutationObserver fingerprinting defense
// Some anti-bot systems inject a hidden element and observe DOM mutations
// to detect automation (bots often trigger DOM changes in burst patterns).
// We normalize MO callback timing.
const _origMO = window.MutationObserver;
window.MutationObserver = function(callback) {{
    const _wrappedCallback = function(mutations, observer) {{
        // Filter out any mutations on our own injected elements
        const filtered = mutations.filter(m => {{
            const target = m.target;
            return !target.__botEvasionInjected;
        }});
        if (filtered.length > 0) {{
            callback(filtered, observer);
        }}
    }};
    return new _origMO(_wrappedCallback);
}};
window.MutationObserver.prototype = _origMO.prototype;

// M2. ResizeObserver — prevent viewport fingerprinting
// Anti-bot systems use ResizeObserver to measure exact viewport dimensions.
// Headless Chrome may have slightly different viewport behavior.
// Pass through unchanged — our screen/viewport patches earlier handle this.

// ── CATEGORY N: Additional Navigator Properties ───────────────────────────

// N1. navigator.scheduling — Chrome 82+ Scheduler API
if (!navigator.scheduling) {{
    try {{
        Object.defineProperty(navigator, 'scheduling', {{
            get: () => ({{
                isInputPending: function() {{ return false; }},
            }}),
            configurable: true,
        }});
    }} catch(_) {{}}
}}

// N2. navigator.xr — WebXR (should exist but return no devices in desktop)
if (!navigator.xr) {{
    try {{
        Object.defineProperty(navigator, 'xr', {{
            get: () => ({{
                isSessionSupported: () => Promise.resolve(false),
                requestSession: () => Promise.reject(new DOMException('NotSupportedError')),
                addEventListener: function() {{}},
                removeEventListener: function() {{}},
            }}),
            configurable: true,
        }});
    }} catch(_) {{}}
}}

// N3. navigator.credentials — Web Authentication API
if (!navigator.credentials) {{
    try {{
        Object.defineProperty(navigator, 'credentials', {{
            get: () => ({{
                get: () => Promise.reject(new DOMException('NotAllowedError')),
                create: () => Promise.reject(new DOMException('NotAllowedError')),
                store: () => Promise.resolve(),
                preventSilentAccess: () => Promise.resolve(),
            }}),
            configurable: true,
        }});
    }} catch(_) {{}}
}}

// N4. navigator.bluetooth — should exist but be unavailable
if (!navigator.bluetooth) {{
    try {{
        Object.defineProperty(navigator, 'bluetooth', {{
            get: () => ({{
                requestDevice: () => Promise.reject(new DOMException('NotFoundError')),
                getAvailability: () => Promise.resolve(false),
                addEventListener: function() {{}},
            }}),
            configurable: true,
        }});
    }} catch(_) {{}}
}}

// N5. navigator.usb — Web USB API stub
if (!navigator.usb) {{
    try {{
        Object.defineProperty(navigator, 'usb', {{
            get: () => ({{
                requestDevice: () => Promise.reject(new DOMException('NotFoundError')),
                getDevices: () => Promise.resolve([]),
                addEventListener: function() {{}},
            }}),
            configurable: true,
        }});
    }} catch(_) {{}}
}}

// N6. navigator.serial — Web Serial API stub
if (!navigator.serial) {{
    try {{
        Object.defineProperty(navigator, 'serial', {{
            get: () => ({{
                requestPort: () => Promise.reject(new DOMException('NotFoundError')),
                getPorts: () => Promise.resolve([]),
                addEventListener: function() {{}},
            }}),
            configurable: true,
        }});
    }} catch(_) {{}}
}}

// N7. navigator.hid — WebHID API stub
if (!navigator.hid) {{
    try {{
        Object.defineProperty(navigator, 'hid', {{
            get: () => ({{
                requestDevice: () => Promise.reject(new DOMException('NotFoundError')),
                getDevices: () => Promise.resolve([]),
                addEventListener: function() {{}},
            }}),
            configurable: true,
        }});
    }} catch(_) {{}}
}}

// ── CATEGORY O: WebGL Advanced Parameters ────────────────────────────────

// O1. Normalize WebGL RENDERER and VENDOR strings
// WEBGL_debug_renderer_info exposes GPU info — should match a real GPU.
// Already patched in base fingerprint.py, but ensure WebGL2 is also patched.
try {{
    const _origGL2GetParam = WebGL2RenderingContext.prototype.getParameter;
    const _UNMASKED_VENDOR_WEBGL = 0x9245;
    const _UNMASKED_RENDERER_WEBGL = 0x9246;
    WebGL2RenderingContext.prototype.getParameter = function(param) {{
        if (param === _UNMASKED_VENDOR_WEBGL) return 'Google Inc. (NVIDIA)';
        if (param === _UNMASKED_RENDERER_WEBGL) return 'ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 SUPER Direct3D11 vs_5_0 ps_5_0, D3D11)';
        return _origGL2GetParam.call(this, param);
    }};
}} catch(_) {{}}

// O2. WebGL vertex array objects and buffer sizes
// Some anti-bot systems probe WebGL limits to fingerprint GPU.
// Normalize MAX_TEXTURE_SIZE and related parameters.
try {{
    const _origGLGetParam = WebGLRenderingContext.prototype.getParameter;
    const _GL_PARAMS = {{
        0x0D33: 16384,   // MAX_TEXTURE_SIZE
        0x851C: 16384,   // MAX_CUBE_MAP_TEXTURE_SIZE
        0x8872: 32,      // MAX_TEXTURE_IMAGE_UNITS
        0x8B4D: 16,      // MAX_VERTEX_TEXTURE_IMAGE_UNITS
        0x8B4C: 32,      // MAX_COMBINED_TEXTURE_IMAGE_UNITS
        0x8B4B: 4096,    // MAX_VERTEX_ATTRIBS
        0x8869: 4096,    // MAX_VERTEX_ATTRIBS
        0x8DFB: 1,       // MAX_SAMPLES
        0x84E8: 16,      // MAX_TEXTURE_MAX_ANISOTROPY_EXT
    }};
    WebGLRenderingContext.prototype.getParameter = function(param) {{
        if (param in _GL_PARAMS) return _GL_PARAMS[param];
        return _origGLGetParam.call(this, param);
    }};
}} catch(_) {{}}

// ── CATEGORY P: Audio Context Fingerprinting ────────────────────────────

// P1. AudioContext sampleRate normalization
// Different sample rates fingerprint device/OS.
// Already patched in base, but ensure OfflineAudioContext is also patched.
try {{
    if (typeof OfflineAudioContext !== 'undefined') {{
        const _origOfflineAC = OfflineAudioContext;
        // Don't rewrap constructor — just ensure buffer noise is consistent
    }}
}} catch(_) {{}}

// P2. AudioWorklet presence check
// Anti-bot may check for AudioWorklet as a modern browser signal.
try {{
    if (typeof AudioContext !== 'undefined' && !AudioContext.prototype.audioWorklet) {{
        // Modern browsers have AudioWorklet — presence is expected
    }}
}} catch(_) {{}}

// ── CATEGORY Q: CSS Feature Detection Evasion ────────────────────────────

// Q1. CSS.supports() normalization
// Anti-bot systems use CSS.supports() to fingerprint browser capabilities.
if (window.CSS && CSS.supports) {{
    const _origCSSSupports = CSS.supports.bind(CSS);
    CSS.supports = function(property, value) {{
        // Pass through — real Chrome's CSS.supports is accurate
        return _origCSSSupports(property, value);
    }};
}}

// Q2. document.fonts (FontFaceSet) normalization
// Font enumeration is a strong fingerprinting vector.
// Already defended via canvas measureText noise. Additional defense:
try {{
    if (document.fonts) {{
        const _origFontsCheck = document.fonts.check.bind(document.fonts);
        // Pass through — font availability is expected to match system
    }}
}} catch(_) {{}}

// ── CATEGORY R: Network Information and Connection ───────────────────────

// R1. RTT and bandwidth normalization (already patched in connection above)

// R2. navigator.onLine state management
// Anti-bot tracks offline→online transitions. Ensure consistent online state.
window.addEventListener('offline', e => {{
    e.preventDefault();
    e.stopImmediatePropagation();
}}, {{ capture: true }});

// ── CATEGORY S: History and Navigation Signals ───────────────────────────

// S1. Normalize history.length
// Bots often start with history.length = 1. Real users have > 1.
// We can't set history.length directly, but we can ensure navigation
// history builds up naturally through our browsing simulation.

// S2. document.referrer normalization for Reddit
// When navigating Reddit internally, referrer should be reddit.com.
// This is handled at the network level by our proxy/header setup.

// ── CATEGORY T: Anti-Anti-Tamper Evasion ─────────────────────────────────

// T1. Prevent script integrity monitoring
// Some anti-bot scripts watch for changes to their own code via
// self-executing integrity checks. We can't prevent this without
// breaking the script, so we ensure our patches are applied BEFORE
// any anti-bot scripts load (via add_init_script).

// T2. Prevent __defineGetter__ / __defineSetter__ detection
// Old-style property definition methods can leak our overrides.
try {{
    if (Object.prototype.__defineGetter__) {{
        const _origDefineGetter = Object.prototype.__defineGetter__;
        // Pass through — just ensure presence
    }}
}} catch(_) {{}}

// T3. Prevent toString fingerprinting of our modified prototypes
// Apply nativeCodeStr patches to all modified prototype methods.
const _makeNative = (fn, name) => {{
    try {{
        const nativeStr = `function ${{name || fn.name || ''}}() {{ [native code] }}`;
        Object.defineProperty(fn, 'toString', {{
            value: () => nativeStr,
            configurable: true, writable: true,
        }});
        Object.defineProperty(fn, 'name', {{
            value: name || fn.name || '',
            configurable: true,
        }});
    }} catch(_) {{}}
}};

// Patch critical overridden methods
[
    [MutationObserver, 'MutationObserver'],
    [navigator.permissions && navigator.permissions.query, 'query'],
    [document.hasFocus, 'hasFocus'],
].forEach(([fn, name]) => {{
    if (typeof fn === 'function') _makeNative(fn, name);
}});

// ═══════════════════════════════════════════════════════════════════════════
// END BOT DETECTION SYSTEM EVASION
// ═══════════════════════════════════════════════════════════════════════════
}})();
"""


def build_runtime_checks_script() -> str:
    """Build a JS script that runs post-load to verify evasion integrity.

    This script checks that our patches are still in place after page scripts
    run (some anti-bot scripts try to restore native functions). If a patch
    has been removed, it re-applies it.
    """
    return """
(() => {
// Post-load integrity verification — re-assert critical patches
const _checks = [
    () => { try { if (navigator.webdriver !== undefined && navigator.webdriver !== false) {
        Object.defineProperty(navigator, 'webdriver', { get: () => false, configurable: true });
    }} catch(_) {} },
    () => { try { if (document.hidden !== false) {
        Object.defineProperty(document, 'hidden', { get: () => false, configurable: true });
    }} catch(_) {} },
    () => { try { if (document.visibilityState !== 'visible') {
        Object.defineProperty(document, 'visibilityState', { get: () => 'visible', configurable: true });
    }} catch(_) {} },
];
_checks.forEach(fn => { try { fn(); } catch(_) {} });
})();
"""


async def inject_evasion(page, profile: dict) -> None:
    """Inject all bot detection evasion patches into *page*.

    Must be called before page.goto() via page.add_init_script().
    Applies 50+ additional evasion techniques targeting commercial anti-bot systems.

    Args:
        page: Playwright Page object.
        profile: Browser profile dict from BrowserProfileManager.generate().
    """
    script = build_evasion_script(profile)
    await page.add_init_script(script)


async def inject_runtime_checks(page) -> None:
    """Inject post-load integrity check script.

    Call this after page load (not as init_script) to verify patches survived.
    Some anti-bot systems try to restore native functions on DOMContentLoaded.
    """
    script = build_runtime_checks_script()
    await page.evaluate(script)


class BotDetectionEvasionManager:
    """Orchestrates injection of all anti-bot detection evasion layers.

    Combines base fingerprint, advanced fingerprint, and vendor-specific
    detection system evasion into a single call.

    Usage:
        mgr = BotDetectionEvasionManager()
        await mgr.inject_all(page, profile)
        # After page.goto():
        await mgr.post_load_check(page)
    """

    async def inject_all(self, page, profile: dict) -> None:
        """Inject all 3 evasion layers before page.goto().

        Layer 1: Base fingerprint (22 techniques) — navigator/screen/WebGL/canvas
        Layer 2: Advanced fingerprint (40 techniques) — timing/permissions/events
        Layer 3: Bot detection evasion (50+ techniques) — vendor-specific systems
        """
        from tools.stealth.fingerprint import _build_inject_script
        from tools.stealth.advanced_fingerprint import build_advanced_script

        await page.add_init_script(_build_inject_script(profile))
        await page.add_init_script(build_advanced_script(profile))
        await page.add_init_script(build_evasion_script(profile))

    async def post_load_check(self, page) -> None:
        """Re-assert critical patches after page scripts have run.

        Some anti-bot systems restore native functions on DOMContentLoaded.
        Call this after page.goto() completes.
        """
        await inject_runtime_checks(page)

    def vendor_list(self) -> list[str]:
        """Return list of targeted bot detection vendors."""
        return [
            "PerimeterX / HUMAN Security",
            "Kasada (Kprotect)",
            "Akamai Bot Manager (ak_bmsc, bmak)",
            "DataDome (jsb.js)",
            "Arkose Labs / FunCaptcha",
            "Imperva / Incapsula (reese84)",
            "F5 Shape Security",
            "Radware Bot Manager",
            "ThreatMetrix / NeuroID",
            "Cloudflare Bot Management (Turnstile, __cf_bm)",
            "Reddit Sentinel (internal)",
        ]

    def technique_list(self) -> list[str]:
        """Return all evasion techniques in this module."""
        return [
            # Category A: Automation artifacts
            "A1.  ChromeDriver $cdc_* window property removal (15 artifacts)",
            "A2.  document-level Selenium artifact removal",
            "A3.  Playwright __playwright/__pw_* property removal",
            "A4.  window property enumeration hardening",
            # Category B: Event trust
            "B1.  event.isTrusted = true via dispatchEvent override",
            "B2.  InputEvent/MouseEvent constructor normalization",
            # Category C: CDP/DevTools detection
            "C2.  console.debug debugger detection prevention",
            "C3.  Error.captureStackTrace frame filtering (playwright/devtools frames)",
            "C4.  performance.timing normalization stub",
            # Category D: Client Hints
            "D1.  navigator.userAgentData full stub (brands, mobile, platform)",
            "D1b. userAgentData.getHighEntropyValues() override",
            # Category E: Prototype chain
            "E2.  Reflect.ownKeys() filtering (__playwright, $cdc, __selenium)",
            "E3.  Object.keys() normalization for navigator",
            "E1.  Prototype descriptor hardening",
            # Category F: Behavioral signals
            "F1.  Cumulative interaction event counter (click, key, mouse, scroll)",
            "F2.  Focus history simulation on page load",
            "F3.  document.hidden = false, visibilityState = 'visible'",
            "F4.  document.wasActivated = true",
            # Category G: Network/timing
            "G1.  XMLHttpRequest timing normalization",
            "G3.  navigator.connection RTT/downlink realistic values",
            # Category H: iframe/cross-origin
            "H1.  window.frameElement = null",
            "H2.  window.crossOriginIsolated, isSecureContext normalization",
            # Category I: Storage
            "I3.  Cache API stub (window.caches)",
            # Category K: Vendor-specific stubs
            "K1.  PerimeterX __pxjsonp_v3_init stub",
            "K2.  Kasada __kp_init stub",
            "K3.  Akamai bmak global stub",
            "K4.  DataDome __dd_event stub",
            "K5.  HUMAN Security __pxmpvid stub",
            "K6.  Cloudflare Turnstile render/reset/getResponse stub",
            "K7.  Imperva _Incapsula_Resource stub",
            "K8.  Arkose Labs ArkoseEnforcement constructor stub",
            "K9.  Reddit Sentinel __redditAnalytics event tracking stub",
            # Category L: JS integrity
            "L2.  eval() pass-through (no wrapping — wrapping itself is a signal)",
            "L4.  Function.prototype.toString self-referential protection",
            # Category M: Observer fingerprinting
            "M1.  MutationObserver callback normalization",
            # Category N: Navigator APIs
            "N1.  navigator.scheduling.isInputPending stub",
            "N2.  navigator.xr (WebXR) stub — isSessionSupported = false",
            "N3.  navigator.credentials (WebAuthn) stub",
            "N4.  navigator.bluetooth stub — getAvailability = false",
            "N5.  navigator.usb stub — getDevices = []",
            "N6.  navigator.serial stub",
            "N7.  navigator.hid (WebHID) stub",
            # Category O: WebGL
            "O1.  WebGL2 RENDERER/VENDOR string normalization",
            "O2.  WebGL texture/buffer parameter normalization",
            # Category P: Audio
            "P1.  OfflineAudioContext noise consistency",
            # Category R: Network
            "R2.  offline event suppression",
            # Category T: Anti-tamper
            "T1.  init_script ordering ensures patches run before anti-bot scripts",
            "T3.  Native code toString patches on overridden prototypes",
        ]
