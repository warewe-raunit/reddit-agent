"""
tools/stealth/fingerprint.py — BrowserProfileManager

Manages per-client browser profiles for consistent browser environments.
Each client gets a unique, deterministic browser profile that persists
across sessions. Profiles are injected via page.addInitScript() before
any navigation occurs.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from playwright.async_api import Page

# ---------------------------------------------------------------------------
# Reference pools — sourced from real-world browser telemetry
# ---------------------------------------------------------------------------

_WEBGL_RENDERERS: list[str] = [
    "ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (NVIDIA, NVIDIA GeForce GTX 1060 6GB Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (NVIDIA, NVIDIA GeForce RTX 2060 Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (AMD, AMD Radeon RX 580 Direct3D11 vs_5_0 ps_5_0, D3D11)",
    "ANGLE (AMD, AMD Radeon RX 5700 XT Direct3D11 vs_5_0 ps_5_0, D3D11)",
]

_SCREEN_RESOLUTIONS: list[tuple[int, int]] = [
    (1920, 1080),
    (1536, 864),
    (1440, 900),
    (1366, 768),
]

_PLUGIN_POOL: list[dict[str, str]] = [
    {"name": "PDF Viewer", "filename": "internal-pdf-viewer", "description": "Portable Document Format"},
    {"name": "Chrome PDF Viewer", "filename": "internal-pdf-viewer", "description": "Portable Document Format"},
    {"name": "Chromium PDF Viewer", "filename": "internal-pdf-viewer", "description": "Portable Document Format"},
    {"name": "Microsoft Edge PDF Viewer", "filename": "internal-pdf-viewer", "description": "Portable Document Format"},
    {"name": "WebKit built-in PDF", "filename": "internal-pdf-viewer", "description": "Portable Document Format"},
    {"name": "Chrome PDF Plugin", "filename": "internal-pdf-viewer", "description": "Portable Document Format"},
    {"name": "Native Client", "filename": "internal-nacl-plugin", "description": ""},
    {"name": "Widevine Content Decryption Module", "filename": "widevinecdmadapter.dll", "description": ""},
]

_OPTIONAL_FONTS: list[str] = [
    "Cambria", "Constantia", "Lucida Bright", "Palatino Linotype",
    "Book Antiqua", "Garamond", "Century Gothic", "Calibri Light",
    "Candara", "Franklin Gothic Medium",
]

_BASE_FONTS: list[str] = [
    "Arial", "Arial Black", "Comic Sans MS", "Courier New", "Georgia",
    "Impact", "Microsoft Sans Serif", "Segoe UI", "Tahoma",
    "Times New Roman", "Trebuchet MS", "Verdana",
]

_USER_AGENT_TEMPLATES: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
]

_SEC_CH_UA_MAP: dict[str, str] = {
    "136": '"Google Chrome";v="136", "Chromium";v="136", "Not.A/Brand";v="24"',
    "135": '"Google Chrome";v="135", "Chromium";v="135", "Not.A/Brand";v="24"',
    "134": '"Google Chrome";v="134", "Chromium";v="134", "Not:A-Brand";v="24"',
    "133": '"Google Chrome";v="133", "Chromium";v="133", "Not?A_Brand";v="24"',
}

_DEFAULT_SEC_CH_UA = _SEC_CH_UA_MAP["133"]

_COLLISION_KEYS: list[str] = [
    "webgl_renderer", "screen_resolution", "hardware_concurrency",
    "device_memory", "canvas_noise_seed",
]

_TIMEZONE_LOCALE_MAP: dict[str, str] = {
    "America/New_York": "en-US",
    "America/Chicago": "en-US",
    "America/Denver": "en-US",
    "America/Los_Angeles": "en-US",
    "America/Phoenix": "en-US",
    "America/Anchorage": "en-US",
    "Pacific/Honolulu": "en-US",
    "America/Toronto": "en-CA",
    "Europe/London": "en-GB",
    "Europe/Berlin": "de-DE",
    "Europe/Paris": "fr-FR",
    "Asia/Tokyo": "ja-JP",
    "Asia/Seoul": "ko-KR",
    "Australia/Sydney": "en-AU",
}


def _deterministic_int(account_id: str, salt: str) -> int:
    digest = hashlib.sha256(f"{account_id}:{salt}".encode()).hexdigest()
    return int(digest[:16], 16)


def _pick_from_pool(pool: list[Any], account_id: str, salt: str) -> Any:
    return pool[_deterministic_int(account_id, salt) % len(pool)]


def _pick_n_from_pool(pool: list[Any], n: int, account_id: str, salt: str) -> list[Any]:
    indices: list[int] = []
    attempt = 0
    while len(indices) < n and attempt < n * 10:
        idx = _deterministic_int(account_id, f"{salt}_{attempt}") % len(pool)
        if idx not in indices:
            indices.append(idx)
        attempt += 1
    return [pool[i] for i in indices]


@dataclass
class BrowserProfileManager:
    """Generate, inject, and compare deterministic browser fingerprint profiles."""

    def generate(self, account_id: str, timezone: str = "America/New_York") -> dict:
        """Return a deterministic browser profile dict for *account_id*."""
        canvas_noise_seed = _deterministic_int(account_id, "canvas") % (2**32)
        webgl_renderer = _pick_from_pool(_WEBGL_RENDERERS, account_id, "webgl")
        width, height = _pick_from_pool(_SCREEN_RESOLUTIONS, account_id, "screen")
        hardware_concurrency = _pick_from_pool([4, 8], account_id, "cores")
        device_memory = _pick_from_pool([8, 16], account_id, "mem")
        device_scale_factor = _pick_from_pool([1, 1, 1, 2], account_id, "dpr")

        plugin_count = 2 + (_deterministic_int(account_id, "plugcount") % 3)
        plugins = _pick_n_from_pool(_PLUGIN_POOL, plugin_count, account_id, "plugins")

        optional_font_count = 2 + (_deterministic_int(account_id, "fontcount") % 2)
        optional_fonts = _pick_n_from_pool(_OPTIONAL_FONTS, optional_font_count, account_id, "fonts")
        fonts = _BASE_FONTS + optional_fonts

        locale = _TIMEZONE_LOCALE_MAP.get(timezone, "en-US")

        user_agent = _pick_from_pool(_USER_AGENT_TEMPLATES, account_id, "ua")
        import re as _re_mod
        _chrome_ver_match = _re_mod.search(r"Chrome/(\d+)", user_agent)
        _chrome_major = _chrome_ver_match.group(1) if _chrome_ver_match else "131"
        sec_ch_ua = _SEC_CH_UA_MAP.get(_chrome_major, _DEFAULT_SEC_CH_UA)

        return {
            "account_id": account_id,
            "canvas_noise_seed": canvas_noise_seed,
            "webgl_renderer": webgl_renderer,
            "webgl_vendor": "Google Inc. (Intel)" if "Intel" in webgl_renderer
                else "Google Inc. (NVIDIA)" if "NVIDIA" in webgl_renderer
                else "Google Inc. (AMD)",
            "screen_resolution": {"width": width, "height": height},
            "timezone": timezone,
            "locale": locale,
            "plugins": plugins,
            "platform": "Win32",
            "fonts": fonts,
            "hardware_concurrency": hardware_concurrency,
            "device_memory": device_memory,
            "device_scale_factor": device_scale_factor,
            "user_agent": user_agent,
            "sec_ch_ua": sec_ch_ua,
            "sec_ch_ua_platform": '"Windows"',
            "sec_ch_ua_mobile": "?0",
        }

    async def inject(self, page: Page, profile: dict) -> None:
        """Inject fingerprint overrides into *page* via addInitScript.
        Must be called before any page.goto().
        """
        script = _build_inject_script(profile)
        await page.add_init_script(script)

    def check_collision(self, new_profile: dict, existing: list[dict]) -> bool:
        """Return True if new_profile is too similar to any in existing (>90% match)."""
        for other in existing:
            matches = sum(
                1 for key in _COLLISION_KEYS
                if new_profile.get(key) == other.get(key)
            )
            if len(_COLLISION_KEYS) > 0 and (matches / len(_COLLISION_KEYS)) > 0.9:
                return True
        return False


def _build_inject_script(profile: dict) -> str:
    """Build the JS string that overrides browser fingerprint properties."""
    screen = profile["screen_resolution"]
    width = screen["width"]
    height = screen["height"]
    platform = profile["platform"]
    locale = profile["locale"]
    hw_concurrency = profile["hardware_concurrency"]
    dev_memory = profile["device_memory"]
    webgl_vendor = profile["webgl_vendor"]
    webgl_renderer = profile["webgl_renderer"]
    canvas_seed = profile["canvas_noise_seed"]
    plugins_json = json.dumps(profile["plugins"])
    timezone = profile["timezone"]

    return f"""
(() => {{
    // --- Navigator overrides ---------------------------------------------------
    Object.defineProperty(navigator, 'platform', {{ get: () => {json.dumps(platform)} }});
    Object.defineProperty(navigator, 'language', {{ get: () => {json.dumps(locale)} }});
    Object.defineProperty(navigator, 'languages', {{ get: () => [{json.dumps(locale)}, 'en'] }});
    Object.defineProperty(navigator, 'hardwareConcurrency', {{ get: () => {hw_concurrency} }});
    Object.defineProperty(navigator, 'deviceMemory', {{ get: () => {dev_memory} }});

    // --- Screen overrides ------------------------------------------------------
    Object.defineProperty(screen, 'width', {{ get: () => {width} }});
    Object.defineProperty(screen, 'height', {{ get: () => {height} }});
    Object.defineProperty(screen, 'availWidth', {{ get: () => {width} }});
    Object.defineProperty(screen, 'availHeight', {{ get: () => {height - 40} }});
    Object.defineProperty(screen, 'colorDepth', {{ get: () => 24 }});
    Object.defineProperty(screen, 'pixelDepth', {{ get: () => 24 }});

    // --- Plugins override ------------------------------------------------------
    const pluginData = {plugins_json};
    const fakePlugins = {{
        length: pluginData.length,
        item: (i) => pluginData[i] || null,
        namedItem: (name) => pluginData.find(p => p.name === name) || null,
        refresh: () => {{}},
        [Symbol.iterator]: function* () {{ for (const p of pluginData) yield p; }}
    }};
    for (let i = 0; i < pluginData.length; i++) {{ fakePlugins[i] = pluginData[i]; }}
    Object.defineProperty(navigator, 'plugins', {{ get: () => fakePlugins }});

    // --- WebGL overrides -------------------------------------------------------
    const getParamOrig = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {{
        const UNMASKED_VENDOR = 0x9245;
        const UNMASKED_RENDERER = 0x9246;
        if (param === UNMASKED_VENDOR) return {json.dumps(webgl_vendor)};
        if (param === UNMASKED_RENDERER) return {json.dumps(webgl_renderer)};
        return getParamOrig.call(this, param);
    }};
    if (typeof WebGL2RenderingContext !== 'undefined') {{
        const getParam2Orig = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(param) {{
            const UNMASKED_VENDOR = 0x9245;
            const UNMASKED_RENDERER = 0x9246;
            if (param === UNMASKED_VENDOR) return {json.dumps(webgl_vendor)};
            if (param === UNMASKED_RENDERER) return {json.dumps(webgl_renderer)};
            return getParam2Orig.call(this, param);
        }};
    }}

    // --- navigator.webdriver (critical bot signal) ------------------------------
    Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined, configurable: true }});
    if (Object.getOwnPropertyDescriptor(Navigator.prototype, 'webdriver')) {{
        Object.defineProperty(Navigator.prototype, 'webdriver', {{ get: () => undefined, configurable: true }});
    }}

    // --- window.chrome stub ----------------------------------------------------
    if (!window.chrome) {{
        window.chrome = {{
            app: {{
                isInstalled: false,
                getDetails: function() {{ return null; }},
                getIsInstalled: function() {{ return false; }},
            }},
            runtime: {{
                connect: function() {{ return undefined; }},
                sendMessage: function() {{ return undefined; }},
            }},
            csi: function() {{ return {{}}; }},
            loadTimes: function() {{
                return {{
                    commitLoadTime: Date.now() / 1000 - 1.2,
                    connectionInfo: 'h2',
                    finishDocumentLoadTime: Date.now() / 1000 - 0.3,
                    finishLoadTime: Date.now() / 1000 - 0.1,
                    firstPaintAfterLoadTime: 0,
                    firstPaintTime: Date.now() / 1000 - 0.8,
                    navigationType: 'Other',
                    npnNegotiatedProtocol: 'h2',
                    requestTime: Date.now() / 1000 - 1.5,
                    startLoadTime: Date.now() / 1000 - 1.5,
                    wasAlternateProtocolAvailable: false,
                    wasFetchedViaSpdy: true,
                    wasNpnNegotiated: true,
                }};
            }},
        }};
    }}

    // --- Canvas noise (per-account pixel noise defeats canvas fingerprinting) ---
    const canvasSeed = {canvas_seed};
    const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function(type, quality) {{
        const ctx = this.getContext('2d');
        if (ctx && this.width > 0 && this.height > 0) {{
            try {{
                const imageData = ctx.getImageData(0, 0, this.width, this.height);
                const data = imageData.data;
                let s = canvasSeed;
                for (let i = 0; i < data.length; i += 4) {{
                    s = (s * 1103515245 + 12345) & 0x7fffffff;
                    data[i] = (data[i] + (s % 3 - 1)) & 0xff;
                }}
                ctx.putImageData(imageData, 0, 0);
            }} catch(e) {{}}
        }}
        return origToDataURL.call(this, type, quality);
    }};

    // --- AudioContext fingerprint noise (defeats Reddit/DataDome audio hash) ----
    const audioSeed = canvasSeed ^ 0xDEADBEEF;
    const origGetChannelData = AudioBuffer.prototype.getChannelData;
    AudioBuffer.prototype.getChannelData = function(channel) {{
        const data = origGetChannelData.call(this, channel);
        if (this.__noised) return data;
        this.__noised = true;
        let s = audioSeed;
        for (let i = 0; i < data.length; i += 100) {{
            s = (s * 1103515245 + 12345) & 0x7fffffff;
            data[i] += (s % 3 - 1) * 0.0000001;
        }}
        return data;
    }};

    // --- Timezone override (Intl) -----------------------------------------------
    const tz = {json.dumps(timezone)};
    const OrigDateTimeFormat = Intl.DateTimeFormat;
    const newDTF = function(locales, options) {{
        options = Object.assign({{}}, options || {{}});
        options.timeZone = options.timeZone || tz;
        return new OrigDateTimeFormat(locales, options);
    }};
    newDTF.prototype = OrigDateTimeFormat.prototype;
    newDTF.supportedLocalesOf = OrigDateTimeFormat.supportedLocalesOf;
    Object.defineProperty(Intl, 'DateTimeFormat', {{ value: newDTF, writable: true, configurable: true }});

    // --- Other navigator properties --------------------------------------------
    Object.defineProperty(navigator, 'maxTouchPoints', {{ get: () => 0 }});
    Object.defineProperty(navigator, 'doNotTrack', {{ get: () => null }});

    // --- performance.memory (Chrome fingerprint check) ------------------------
    if (window.performance) {{
        Object.defineProperty(performance, 'memory', {{
            get: () => ({{
                jsHeapSizeLimit: 2172649472,
                totalJSHeapSize: 35839897 + (canvasSeed % 5000000),
                usedJSHeapSize: 28723145 + (canvasSeed % 3000000),
            }}),
        }});
    }}

    // --- Battery API stub (deprecated but fingerprinted) ----------------------
    if (navigator.getBattery) {{
        navigator.getBattery = function() {{
            return Promise.resolve({{
                charging: true, chargingTime: 0, dischargingTime: Infinity, level: 1.0,
                addEventListener: function() {{}}, removeEventListener: function() {{}},
                dispatchEvent: function() {{ return true; }},
                onchargingchange: null, onchargingtimechange: null,
                ondischargingtimechange: null, onlevelchange: null,
            }});
        }};
    }}

    // --- Network Connection API ------------------------------------------------
    if (!navigator.connection) {{
        Object.defineProperty(navigator, 'connection', {{
            get: () => ({{ effectiveType: '4g', rtt: 50, downlink: 10, saveData: false, type: 'wifi',
                           addEventListener: function() {{}}, removeEventListener: function() {{}} }}),
        }});
    }}

    // --- mediaDevices.enumerateDevices() fake ---------------------------------
    if (navigator.mediaDevices && navigator.mediaDevices.enumerateDevices) {{
        navigator.mediaDevices.enumerateDevices = function() {{
            return Promise.resolve([
                {{ deviceId: 'default', kind: 'audioinput', label: '', groupId: 'default' }},
                {{ deviceId: 'default', kind: 'audiooutput', label: '', groupId: 'default' }},
                {{ deviceId: 'default', kind: 'videoinput', label: '', groupId: 'default' }},
            ]);
        }};
    }}

    // --- WebRTC ICE candidate stripping (prevent real IP leak) ----------------
    const OrigRTCPeerConnection = window.RTCPeerConnection || window.webkitRTCPeerConnection;
    if (OrigRTCPeerConnection) {{
        const wrappedRTC = function(config, constraints) {{
            config = config || {{}};
            config.iceTransportPolicy = 'relay';
            const pc = new OrigRTCPeerConnection(config, constraints);
            const origAddEventListener = pc.addEventListener.bind(pc);
            pc.addEventListener = function(type, listener, options) {{
                if (type === 'icecandidate') {{
                    const wrapped = function(event) {{
                        if (event.candidate && event.candidate.candidate) {{
                            const c = event.candidate.candidate;
                            if (c.includes('srflx') || c.includes('prflx') || c.includes('host')) return;
                        }}
                        listener.call(this, event);
                    }};
                    return origAddEventListener(type, wrapped, options);
                }}
                return origAddEventListener(type, listener, options);
            }};
            return pc;
        }};
        wrappedRTC.prototype = OrigRTCPeerConnection.prototype;
        wrappedRTC.generateCertificate = OrigRTCPeerConnection.generateCertificate;
        window.RTCPeerConnection = wrappedRTC;
        if (window.webkitRTCPeerConnection) window.webkitRTCPeerConnection = wrappedRTC;
    }}

    // --- Error.stack cleanup (remove Playwright evaluation traces) -------------
    if (typeof Error.prepareStackTrace === 'undefined' || Error.prepareStackTrace === null) {{
        Error.prepareStackTrace = function(err, structuredStackTrace) {{
            const filtered = structuredStackTrace.filter(function(frame) {{
                const fn = (frame.getFunctionName && frame.getFunctionName()) || '';
                const file = (frame.getFileName && frame.getFileName()) || '';
                return fn.indexOf('__playwright') === -1 && fn.indexOf('__puppeteer') === -1 &&
                       file.indexOf('__playwright') === -1 && file.indexOf('pptr:') === -1;
            }});
            return 'Error: ' + (err.message || '') + '\\n' +
                   filtered.map(function(f) {{
                       return '    at ' + (f.getFunctionName() || '<anonymous>') +
                              ' (' + (f.getFileName() || '<anonymous>') + ':' +
                              (f.getLineNumber() || 0) + ':' + (f.getColumnNumber() || 0) + ')';
                   }}).join('\\n');
        }};
    }}

    // --- window.outerWidth/outerHeight consistency ----------------------------
    Object.defineProperty(window, 'outerWidth', {{ get: () => window.innerWidth + 15 }});
    Object.defineProperty(window, 'outerHeight', {{ get: () => window.innerHeight + 85 }});
}})();
"""
