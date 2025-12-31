import Millennium
import PluginUtils                

logger = PluginUtils.Logger()

import json
import os
import shutil
import tempfile


import httpx
import threading
import time
import re
import sys
import zipfile
import subprocess
import base64
import webbrowser
if sys.platform.startswith('win'):
	try:
		import winreg                
	except Exception:
		winreg = None                

WEBKIT_DIR_NAME = "MangoUnlock"
WEB_UI_JS_FILE = "MangoUnlock.js"
WEB_UI_ICON_FILE = "steam_icon.ico"
CSS_ID = None
JS_ID = None
HTTP_CLIENT = None
HTTP_TIMEOUT_SECONDS = 15
DOWNLOAD_STATE = {}
DOWNLOAD_LOCK = threading.Lock()
MANIFEST_CACHE = {}
MANIFEST_CACHE_LOCK = threading.Lock()
MANIFEST_CACHE_TTL = 15 * 60           
STEAM_INSTALL_PATH = None
MANIFEST_REPOSITORIES = [
    'KunalR31/manifest',
    'steamautoCracks/ManifestHub',
]
USER_AGENT = 'luatools-v61-stplugin-hoe'
API_MANIFEST_URL = 'https://raw.githubusercontent.com/madoiscool/lt_api_links/refs/heads/main/load_free_manifest_apis'
API_MANIFEST_PROXY_URL = 'https://luatools.vercel.app/load_free_manifest_apis'
API_DOWNLOAD_HEADERS = {
    'User-Agent': USER_AGENT,
}
REMOTE_API_LIST_CACHE = {
    'timestamp': 0.0,
    'entries': None,
}
REMOTE_API_LIST_LOCK = threading.Lock()
REMOTE_API_LIST_TTL = 10 * 60           
GITHUB_JSON_HEADERS = {
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'MangoUnlock-Plugin',
}
GITHUB_RAW_HEADERS = {
    'User-Agent': 'MangoUnlock-Plugin',
}
ADDAPP_PATTERN = re.compile(r'^\s*addappid\s*\(([^)]*)\)', re.IGNORECASE)
# Pattern to extract the first argument (appid) from addappid lines
ADDAPP_APPID_EXTRACT = re.compile(r'^\s*addappid\s*\(\s*(\d+)', re.IGNORECASE)

# DLC cache for pre-fetching DLCs on Steam startup
DLC_CACHE = {}
DLC_CACHE_LOCK = threading.Lock()
DLC_CACHE_TTL = 30 * 60  # 30 minutes
DLC_PREFETCH_THREAD = None
DLC_PREFETCH_APPIDS = set()

# Auto-update configuration
UPDATE_CONFIG_FILE = 'update.json'
UPDATE_PENDING_ZIP = 'update_pending.zip'
UPDATE_PENDING_INFO = 'update_pending.json'
UPDATE_CHECK_INTERVAL_SECONDS = 2 * 60 * 60  # 2 hours
UPDATE_CHECK_THREAD = None
LAST_UPDATE_MESSAGE = None
LAST_UPDATE_MESSAGE_LOCK = threading.Lock()

class Logger:
    @staticmethod
    def log(message: str) -> None:
        logger.log(message)

    @staticmethod
    def warn(message: str) -> None:
        logger.warn(message)

    @staticmethod
    def error(message: str) -> None:
        logger.error(message)

def GetPluginDir():
    return os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..'))

# ==================== AUTO-UPDATE FUNCTIONS ====================

def _read_json(path: str) -> dict:
    """Read and parse a JSON file, returns empty dict on failure."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def _write_json(path: str, data: dict) -> bool:
    """Write data to a JSON file."""
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception:
        return False

def _parse_version(version: str) -> tuple:
    """Parse a version string into a comparable tuple."""
    try:
        parts = [int(part) for part in re.findall(r'\d+', str(version))]
        return tuple(parts or [0])
    except Exception:
        return (0,)

def _get_plugin_version() -> str:
    """Get the current plugin version from plugin.json."""
    try:
        plugin_json_path = os.path.join(GetPluginDir(), 'plugin.json')
        data = _read_json(plugin_json_path)
        return str(data.get('version', '0'))
    except Exception:
        return '0'

def _store_last_message(message: str) -> None:
    """Store a message to be retrieved by the frontend."""
    global LAST_UPDATE_MESSAGE
    with LAST_UPDATE_MESSAGE_LOCK:
        LAST_UPDATE_MESSAGE = message

def _get_last_message() -> str:
    """Get and clear the last stored message."""
    global LAST_UPDATE_MESSAGE
    with LAST_UPDATE_MESSAGE_LOCK:
        msg = LAST_UPDATE_MESSAGE
        LAST_UPDATE_MESSAGE = None
        return msg or ''

def _fetch_github_latest(cfg: dict) -> dict:
    """Fetch the latest release info from GitHub."""
    owner = str(cfg.get('owner', '')).strip()
    repo = str(cfg.get('repo', '')).strip()
    asset_name = str(cfg.get('asset_name', 'MangoUnlock.zip')).strip()
    tag = str(cfg.get('tag', '')).strip()
    tag_prefix = str(cfg.get('tag_prefix', '')).strip()
    token = str(cfg.get('token', '')).strip()
    
    if not owner or not repo:
        logger.warn('AutoUpdate: GitHub config missing owner or repo')
        return {}
    
    _ensure_http_client()
    endpoint = f'https://api.github.com/repos/{owner}/{repo}/releases/latest'
    if tag:
        endpoint = f'https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}'
    
    headers = {
        'Accept': 'application/vnd.github+json',
        'User-Agent': 'MangoUnlock-Updater',
    }
    if token:
        headers['Authorization'] = f'Bearer {token}'
    
    data = None
    tag_name = ''
    
    try:
        logger.log(f'AutoUpdate: Fetching GitHub release from {endpoint}')
        resp = HTTP_CLIENT.get(endpoint, headers=headers, follow_redirects=True, timeout=15)
        logger.log(f'AutoUpdate: GitHub API response: status={resp.status_code}')
        resp.raise_for_status()
        data = resp.json()
        tag_name = str(data.get('tag_name', '')).strip()
        logger.log('AutoUpdate: GitHub API request successful')
    except Exception as api_err:
        logger.warn(f'AutoUpdate: GitHub API failed: {api_err}')
        return {}
    
    if not data:
        return {}
    
    version = tag_name or str(data.get('name', '')).strip()
    if tag_prefix and version.startswith(tag_prefix):
        version = version[len(tag_prefix):]
    
    zip_url = ''
    try:
        assets = data.get('assets', [])
        if isinstance(assets, list):
            for asset in assets:
                a_name = str(asset.get('name', '')).strip()
                if a_name == asset_name:
                    zip_url = str(asset.get('browser_download_url', '')).strip()
                    break
    except Exception:
        pass
    
    # Fallback to zipball_url if no asset found
    if not zip_url:
        zip_url = str(data.get('zipball_url', '')).strip()
    
    if not zip_url:
        logger.warn('AutoUpdate: No download URL found')
        return {}
    
    return {'version': version, 'zip_url': zip_url}

def _download_and_extract_update(zip_url: str, pending_zip: str) -> bool:
    """Download an update zip file."""
    _ensure_http_client()
    try:
        logger.log(f'AutoUpdate: Downloading {zip_url} -> {pending_zip}')
        with HTTP_CLIENT.stream('GET', zip_url, follow_redirects=True, timeout=60) as response:
            logger.log(f'AutoUpdate: Update download response: status={response.status_code}')
            response.raise_for_status()
            with open(pending_zip, 'wb') as output:
                for chunk in response.iter_bytes():
                    if chunk:
                        output.write(chunk)
        return True
    except Exception as exc:
        logger.warn(f'AutoUpdate: Failed to download update: {exc}')
        return False

def _apply_pending_update_if_any() -> str:
    """Extract a pending update zip if present. Returns a message or empty string."""
    backend_dir = os.path.join(GetPluginDir(), 'backend')
    pending_zip = os.path.join(backend_dir, UPDATE_PENDING_ZIP)
    pending_info = os.path.join(backend_dir, UPDATE_PENDING_INFO)
    
    if not os.path.exists(pending_zip):
        return ''
    
    try:
        logger.log(f'AutoUpdate: Applying pending update from {pending_zip}')
        with zipfile.ZipFile(pending_zip, 'r') as archive:
            archive.extractall(GetPluginDir())
        try:
            os.remove(pending_zip)
        except Exception:
            pass
        
        info = _read_json(pending_info)
        try:
            os.remove(pending_info)
        except Exception:
            pass
        
        new_version = str(info.get('version', '')) if isinstance(info, dict) else ''
        if new_version:
            return f'MangoUnlock updated to {new_version}. Please restart Steam.'
        return 'MangoUnlock update applied. Please restart Steam.'
    except Exception as exc:
        logger.warn(f'AutoUpdate: Failed to apply pending update: {exc}')
        return ''

def _check_for_update_once() -> str:
    """Check for updates and download if available. Returns a message for the user."""
    backend_dir = os.path.join(GetPluginDir(), 'backend')
    cfg_path = os.path.join(backend_dir, UPDATE_CONFIG_FILE)
    cfg = _read_json(cfg_path)
    
    latest_version = ''
    zip_url = ''
    
    gh_cfg = cfg.get('github')
    if isinstance(gh_cfg, dict):
        manifest = _fetch_github_latest(gh_cfg)
        latest_version = str(manifest.get('version', '')).strip()
        zip_url = str(manifest.get('zip_url', '')).strip()
    else:
        # No update config found
        return ''
    
    if not latest_version or not zip_url:
        logger.warn('AutoUpdate: Manifest missing version or zip_url')
        return ''
    
    current_version = _get_plugin_version()
    if _parse_version(latest_version) <= _parse_version(current_version):
        logger.log(f'AutoUpdate: Up-to-date (current {current_version}, latest {latest_version})')
        return ''
    
    pending_zip = os.path.join(backend_dir, UPDATE_PENDING_ZIP)
    pending_info = os.path.join(backend_dir, UPDATE_PENDING_INFO)
    
    if not _download_and_extract_update(zip_url, pending_zip):
        return ''
    
    # Attempt to extract immediately
    try:
        with zipfile.ZipFile(pending_zip, 'r') as archive:
            archive.extractall(GetPluginDir())
        try:
            os.remove(pending_zip)
        except Exception:
            pass
        logger.log('AutoUpdate: Update extracted; will take effect after restart')
        return f'MangoUnlock updated to {latest_version}. Please restart Steam to apply.'
    except Exception as extract_err:
        logger.warn(f'AutoUpdate: Extraction failed, will apply on next start: {extract_err}')
        _write_json(pending_info, {'version': latest_version, 'zip_url': zip_url})
        logger.log('AutoUpdate: Update downloaded and queued for apply on next start')
        return f'Update {latest_version} downloaded. Restart Steam to apply.'

def _periodic_update_check_worker():
    """Background worker that periodically checks for updates."""
    while True:
        try:
            time.sleep(UPDATE_CHECK_INTERVAL_SECONDS)
            logger.log('AutoUpdate: Running periodic background check...')
            message = _check_for_update_once()
            if message:
                _store_last_message(message)
                logger.log(f'AutoUpdate: Periodic check found update: {message}')
        except Exception as exc:
            logger.warn(f'AutoUpdate: Periodic check failed: {exc}')

def _start_periodic_update_checks():
    """Start the periodic update check thread."""
    global UPDATE_CHECK_THREAD
    if UPDATE_CHECK_THREAD is None or not UPDATE_CHECK_THREAD.is_alive():
        UPDATE_CHECK_THREAD = threading.Thread(
            target=_periodic_update_check_worker, daemon=True
        )
        UPDATE_CHECK_THREAD.start()
        logger.log(f'AutoUpdate: Started periodic update check thread (every {UPDATE_CHECK_INTERVAL_SECONDS / 3600} hours)')

def _start_initial_check_worker():
    """Run an initial update check in a background thread."""
    try:
        message = _check_for_update_once()
        if message:
            _store_last_message(message)
            logger.log(f'AutoUpdate: Initial check found update: {message}')
        else:
            _start_periodic_update_checks()
    except Exception as exc:
        logger.warn(f'AutoUpdate: background check failed: {exc}')
        try:
            _start_periodic_update_checks()
        except Exception:
            pass

def _start_auto_update_background_check() -> None:
    """Kick off the initial check in a background thread."""
    threading.Thread(target=_start_initial_check_worker, daemon=True).start()

def CheckForUpdatesNow(contentScriptQuery: str = '') -> str:
    """Expose a synchronous update check for the frontend."""
    try:
        message = _check_for_update_once()
        if message:
            _store_last_message(message)
        return json.dumps({'success': True, 'message': message})
    except Exception as exc:
        logger.warn(f'MangoUnlock: CheckForUpdatesNow failed: {exc}')
        return json.dumps({'success': False, 'error': str(exc)})

def GetUpdateMessage(contentScriptQuery: str = '') -> str:
    """Get any pending update message for the frontend."""
    try:
        message = _get_last_message()
        return json.dumps({'success': True, 'message': message})
    except Exception as exc:
        return json.dumps({'success': False, 'error': str(exc)})

# ==================== END AUTO-UPDATE FUNCTIONS ====================
                                                    

def detect_steam_install_path() -> str:
    
    global STEAM_INSTALL_PATH

    if STEAM_INSTALL_PATH and os.path.exists(STEAM_INSTALL_PATH):
        return STEAM_INSTALL_PATH

    candidates = []

                                                
    path = _find_steam_path()
    if path:
        candidates.append(path)

                                                            
    try:
        millennium_path = Millennium.steam_path()
        if millennium_path:
            candidates.append(millennium_path)
    except Exception:
        pass

                                     
    for env_var in ('STEAM_PATH', 'SteamPath', 'STEAM_INSTALL_PATH'):
        env_path = os.environ.get(env_var)
        if env_path:
            candidates.append(env_path)

                                        
    for root_var in ('ProgramFiles(x86)', 'ProgramFiles'):
        root = os.environ.get(root_var)
        if root:
            candidates.append(os.path.join(root, 'Steam'))

    seen = set()
    for candidate in candidates:
        if not candidate:
            continue
        normalized = os.path.normpath(
            os.path.abspath(
                os.path.expanduser(os.path.expandvars(candidate))
            )
        )
        key = os.path.normcase(normalized)
        if key in seen:
            continue
        seen.add(key)
        if os.path.exists(normalized):
            STEAM_INSTALL_PATH = normalized
            logger.log(f'MangoUnlock: Steam path resolved to {normalized}')
            return normalized

    return ''

class Plugin:
    def init_http_client(self):
        global HTTP_CLIENT
        if HTTP_CLIENT is None:
            try:
                HTTP_CLIENT = httpx.Client(timeout=10)
            except Exception as e:
                logger.error(f'MangoUnlock: Failed to initialize HTTPX client: {e}')

    def close_http_client(self):
        global HTTP_CLIENT
        if HTTP_CLIENT is not None:
            try:
                HTTP_CLIENT.close()
            except Exception:
                pass
            HTTP_CLIENT = None

    def copy_webkit_files(self):
        webkit_js = os.path.join(GetPluginDir(), "public", WEB_UI_JS_FILE)
        steam_ui_path = os.path.join(Millennium.steam_path(), "steamui", WEBKIT_DIR_NAME)
        os.makedirs(steam_ui_path, exist_ok=True)
        js_dest = os.path.join(steam_ui_path, WEB_UI_JS_FILE)
        logger.log(f'Copying MangoUnlock web UI from {webkit_js} to {js_dest}')
        try:
            shutil.copy(webkit_js, js_dest)
        except Exception as e:
            logger.error(f'Failed to copy MangoUnlock web UI, {e}')
        try:
            icon_src = os.path.join(GetPluginDir(), "public", WEB_UI_ICON_FILE)
            icon_dest = os.path.join(steam_ui_path, WEB_UI_ICON_FILE)
            if os.path.exists(icon_src):
                shutil.copy(icon_src, icon_dest)
                logger.log(f'Copied MangoUnlock icon to {icon_dest}')
            else:
                logger.warn(f'MangoUnlock icon not found at {icon_src}')
        except Exception as e:
            logger.error(f'Failed to copy MangoUnlock icon, {e}')

    def inject_webkit_files(self):
        js_path = os.path.join(WEBKIT_DIR_NAME, WEB_UI_JS_FILE)
        Millennium.add_browser_js(js_path)
        logger.log(f'MangoUnlock injected web UI: {js_path}')

    def _front_end_loaded(self):
        self.copy_webkit_files()

    def _load(self):
        logger.log(f'bootstrapping MangoUnlock plugin, millennium {Millennium.version()}')
        try:
            detect_steam_install_path()
        except Exception as e:
            logger.warn(f'MangoUnlock: steam path detection failed: {e}')
        self.init_http_client()
        
        # Apply any pending updates from previous session
        try:
            message = _apply_pending_update_if_any()
            if message:
                _store_last_message(message)
        except Exception as exc:
            logger.warn(f'AutoUpdate: apply pending failed: {exc}')
        
        self.copy_webkit_files()
        self.inject_webkit_files()
        
        # Start auto-update background check
        try:
            _start_auto_update_background_check()
        except Exception as exc:
            logger.warn(f'AutoUpdate: start background check failed: {exc}')
        
        Millennium.ready()

    def _unload(self):
        logger.log("unloading")
        self.close_http_client()

                                                                                      

def _backend_path(filename: str) -> str:
    return os.path.join(GetPluginDir(), 'backend', filename)

def _backend_dir() -> str:
    return os.path.join(GetPluginDir(), 'backend')

def _ensure_http_client() -> None:
    global HTTP_CLIENT
    if HTTP_CLIENT is None:
        try:
            HTTP_CLIENT = httpx.Client(timeout=HTTP_TIMEOUT_SECONDS, follow_redirects=True)
        except Exception as e:
            logger.error(f'MangoUnlock: Failed to initialize shared HTTPX client: {e}')


def _get_dlc_cache_entry(appid: int):
    """Get cached DLC list for an app if not expired."""
    now = time.time()
    with DLC_CACHE_LOCK:
        entry = DLC_CACHE.get(appid)
        if not entry:
            return None
        if now - entry.get('timestamp', 0) > DLC_CACHE_TTL:
            DLC_CACHE.pop(appid, None)
            return None
        return entry.get('dlcs', [])


def _set_dlc_cache_entry(appid: int, dlcs: list) -> None:
    """Cache DLC list for an app."""
    with DLC_CACHE_LOCK:
        DLC_CACHE[appid] = {
            'timestamp': time.time(),
            'dlcs': dlcs or [],
        }


def _fetch_dlcs_for_app(appid: int) -> list:
    """
    Fetch all DLC app IDs for a given app from Steam Store API.
    Returns a list of DLC app IDs (integers).
    """
    # Check cache first
    cached = _get_dlc_cache_entry(appid)
    if cached is not None:
        logger.log(f'MangoUnlock: DLC cache hit for appid {appid}, {len(cached)} DLCs')
        return cached
    
    _ensure_http_client()
    try:
        url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
        resp = HTTP_CLIENT.get(url, follow_redirects=True, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        entry = data.get(str(appid)) or data.get(int(appid)) or {}
        if not isinstance(entry, dict) or not entry.get('success'):
            logger.log(f'MangoUnlock: No DLC data available for appid {appid}')
            _set_dlc_cache_entry(appid, [])
            return []
        
        inner = entry.get('data') or {}
        dlc_list = inner.get('dlc', [])
        if not isinstance(dlc_list, list):
            dlc_list = []
        
        # Convert to integers
        dlc_appids = []
        for dlc_id in dlc_list:
            try:
                dlc_appids.append(int(dlc_id))
            except (TypeError, ValueError):
                continue
        
        logger.log(f'MangoUnlock: Fetched {len(dlc_appids)} DLCs for appid {appid}')
        _set_dlc_cache_entry(appid, dlc_appids)
        return dlc_appids
    except Exception as e:
        logger.warn(f'MangoUnlock: Failed to fetch DLCs for appid {appid}: {e}')
        return []


def _extract_existing_appids_from_lua(lua_text: str) -> set:
    """
    Extract all app IDs that are already present in the lua file.
    Searches for lines starting with addappid(APPID to handle both:
    - addappid(123)
    - addappid(123,0,"key")
    Returns a set of app ID integers.
    """
    existing = set()
    for line in lua_text.splitlines():
        match = ADDAPP_APPID_EXTRACT.search(line)
        if match:
            try:
                existing.add(int(match.group(1)))
            except (TypeError, ValueError):
                continue
    return existing


def _inject_missing_dlcs_into_lua(lua_text: str, dlc_appids: list) -> str:
    """
    Inject missing DLC app IDs into the lua content.
    Checks each DLC app ID - if not already present as addappid(DLCID...,
    appends addappid(DLCID) at the end.
    Returns the modified lua text.
    """
    if not dlc_appids:
        return lua_text
    
    existing = _extract_existing_appids_from_lua(lua_text)
    missing = [dlc for dlc in dlc_appids if dlc not in existing]
    
    if not missing:
        logger.log(f'MangoUnlock: All {len(dlc_appids)} DLCs already present in lua file')
        return lua_text
    
    logger.log(f'MangoUnlock: Injecting {len(missing)} missing DLCs out of {len(dlc_appids)} total')
    
    # Ensure the lua text ends with a newline before appending
    result = lua_text.rstrip('\n') + '\n'
    
    # Add missing DLCs
    for dlc_id in missing:
        result += f'addappid({dlc_id})\n'
    
    return result


def PrefetchDLCsForApp(appid: int, contentScriptQuery: str = '') -> str:
    """
    Called from frontend to pre-fetch DLCs for an app.
    This starts a background fetch so DLCs are ready when download is requested.
    """
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({ 'success': False, 'error': 'Invalid appid' })
    
    def _prefetch():
        try:
            _fetch_dlcs_for_app(appid)
        except Exception as e:
            logger.warn(f'MangoUnlock: DLC prefetch failed for appid {appid}: {e}')
    
    thread = threading.Thread(target=_prefetch, name=f'DLCPrefetch-{appid}', daemon=True)
    thread.start()
    
    return json.dumps({ 'success': True, 'message': f'Started DLC prefetch for appid {appid}' })


def HasMangoUnlockForApp(appid: int, contentScriptQuery: str = '') -> str:
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({ 'success': False, 'error': 'Invalid appid' })
    base = detect_steam_install_path() or Millennium.steam_path()
    candidate1 = os.path.join(base, 'config', 'stplug-in', f'{appid}.lua')
    candidate2 = os.path.join(base, 'config', 'stplug-in', f'{appid}.lua.disabled')
    exists = os.path.exists(candidate1) or os.path.exists(candidate2)
    logger.log(f'MangoUnlock: HasMangoUnlockForApp appid={appid} -> {exists}')
    return json.dumps({ 'success': True, 'exists': exists })

def _get_manifest_cache_entry(appid: str):
    now = time.time()
    with MANIFEST_CACHE_LOCK:
        entry = MANIFEST_CACHE.get(appid)
        if not entry:
            return None
        if now - entry.get('timestamp', 0) > MANIFEST_CACHE_TTL:
            MANIFEST_CACHE.pop(appid, None)
            return None
        return entry

def _set_manifest_cache_entry(appid: str, available: bool, repository: str = None) -> None:
    with MANIFEST_CACHE_LOCK:
        MANIFEST_CACHE[appid] = {
            'timestamp': time.time(),
            'available': bool(available),
            'repository': repository,
        }

def _normalize_api_entry(entry: dict):
    
    try:
        if not isinstance(entry, dict):
            return None
        url = str(entry.get('url', '')).strip()
        if not url:
            return None
        name = str(entry.get('name', 'Remote')).strip() or 'Remote'
        success_code = int(entry.get('success_code', 200))
        unavailable_code = int(entry.get('unavailable_code', 404))
        enabled = bool(entry.get('enabled', True))
        return {
            'name': name,
            'url': url,
            'success_code': success_code,
            'unavailable_code': unavailable_code,
            'enabled': enabled,
        }
    except Exception:
        return None


def _normalize_manifest_text(text: str) -> str:
    content = (text or '').strip()
    if not content:
        return content

    content = re.sub(r",\s*]", "]", content)
    content = re.sub(r",\s*}\s*$", "}", content)

    if content.startswith('"api_list"') or content.startswith("'api_list'") or content.startswith("api_list"):
        if not content.startswith("{"):
            content = "{" + content
        if not content.endswith("}"):
            content = content.rstrip(",") + "}"

    try:
        json.loads(content)
        return content
    except Exception:
        return text


def _parse_remote_payload(text: str):
    try:
        return json.loads(text)
    except Exception:
        pass

    candidate = (text or '').strip().rstrip(",")
    if candidate.lstrip().startswith('"api_list"'):
        wrapped = "{" + candidate + "}"
        try:
            return json.loads(wrapped)
        except Exception:
            candidate = wrapped

    match = re.search(r'"api_list"\s*:\s*(\[[\s\S]*\])', candidate)
    if match:
        array_part = match.group(1)
        try:
            return {"api_list": json.loads(array_part)}
        except Exception:
            try:
                return json.loads(f'{{"api_list": {array_part}}}')
            except Exception:
                return None
    return None

def _load_remote_api_list() -> list:
    
    now = time.time()
    with REMOTE_API_LIST_LOCK:
        cached = REMOTE_API_LIST_CACHE.get('entries')
        cached_ts = REMOTE_API_LIST_CACHE.get('timestamp', 0.0)
    if cached and (now - cached_ts) < REMOTE_API_LIST_TTL:
        return cached

    cached = cached or []
    manifest_text = ''

    api_json_path = _backend_path('api.json')
    if os.path.exists(api_json_path):
        try:
            with open(api_json_path, 'r', encoding='utf-8') as f:
                manifest_text = f.read()
            logger.log(f'MangoUnlock: Loaded API manifest from local file {api_json_path}')
        except Exception as e:
            logger.warn(f'MangoUnlock: Failed to read local API manifest {api_json_path}: {e}')

    if not manifest_text:
        _ensure_http_client()
        try:
            resp = HTTP_CLIENT.get(API_MANIFEST_URL, timeout=HTTP_TIMEOUT_SECONDS)
            resp.raise_for_status()
            manifest_text = resp.text
            logger.log(f'MangoUnlock: Remote API manifest fetched (status {resp.status_code}, bytes {len(resp.content)})')
        except Exception as primary_error:
            logger.warn(f'MangoUnlock: Remote API manifest primary fetch failed: {primary_error}; trying proxy')
            try:
                resp = HTTP_CLIENT.get(API_MANIFEST_PROXY_URL, timeout=HTTP_TIMEOUT_SECONDS)
                resp.raise_for_status()
                manifest_text = resp.text
                logger.log(f'MangoUnlock: Remote API manifest fetched from proxy (status {resp.status_code}, bytes {len(resp.content)})')
            except Exception as proxy_error:
                if cached:
                    logger.warn(f'MangoUnlock: Remote API manifest fetch failed; using cached entries (primary={primary_error}; proxy={proxy_error})')
                    return cached
                logger.warn(f'MangoUnlock: Remote API manifest fetch failed (primary={primary_error}; proxy={proxy_error})')
                return []

    normalized_text = _normalize_manifest_text(manifest_text)
    if not normalized_text:
        return cached

    try:
        payload = json.loads(normalized_text)
    except Exception:
        payload = _parse_remote_payload(normalized_text)
        if payload is None:
            logger.warn('MangoUnlock: Remote API manifest parse failed after normalization')
            return cached

    entries = payload.get('api_list')
    normalized_entries = []
    if isinstance(entries, list):
        for entry in entries:
            normalized_entry = _normalize_api_entry(entry)
            if normalized_entry and normalized_entry.get('enabled', True):
                normalized_entries.append(normalized_entry)

    if not normalized_entries:
        return cached

    with REMOTE_API_LIST_LOCK:
        REMOTE_API_LIST_CACHE['entries'] = normalized_entries
        REMOTE_API_LIST_CACHE['timestamp'] = time.time()
    return normalized_entries

def _get_api_sources() -> list:
    
    return _load_remote_api_list()

def _check_api_availability(appid: int):
    
    api_sources = _get_api_sources()
    if not api_sources:
        return False, None

    _ensure_http_client()
    base_headers = dict(API_DOWNLOAD_HEADERS)

    for api in api_sources:
        if not api.get('enabled', True):
            continue

        name = api.get('name', 'Unknown')
        label = f'API:{name}'
        template = api.get('url', '')
        url = template.replace('<appid>', str(appid))
        success_code = int(api.get('success_code', 200))
        unavailable_code = int(api.get('unavailable_code', 404))

        try:
            with HTTP_CLIENT.stream('GET', url, headers=base_headers, timeout=HTTP_TIMEOUT_SECONDS) as resp:
                status = resp.status_code
                if status == success_code:
                    logger.log(f'MangoUnlock: API source reports availability for appid {appid} ({label})')
                    return True, label
                if status == unavailable_code:
                    continue
                logger.log(f'MangoUnlock: API source {label} returned unexpected status {status} for appid {appid}')
        except httpx.HTTPError as e:
            logger.warn(f'MangoUnlock: API availability check HTTP error for appid {appid}: {e}')
        except Exception as e:
            logger.warn(f'MangoUnlock: API availability check failed for appid {appid}: {e}')

    return False, None

def CheckManifestAvailability(appid: int, contentScriptQuery: str = '') -> str:
    
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({ 'success': False, 'error': 'Invalid appid' })

    appid_str = str(appid)
    cached = _get_manifest_cache_entry(appid_str)
    if cached:
        return json.dumps({
            'success': True,
            'available': bool(cached.get('available')),
            'repository': cached.get('repository'),
            'cached': True,
        })

    found_repo = None
    errors = []

    for repo in MANIFEST_REPOSITORIES:
        exists, error = _check_repo_branch(repo, appid_str)
        if exists:
            found_repo = repo
            break
        if error:
            errors.append(f'repository error: {error}')

    if found_repo:
        _set_manifest_cache_entry(appid_str, True, found_repo)
        logger.log(f'MangoUnlock: Manifest found for appid {appid}')
        return json.dumps({ 'success': True, 'available': True, 'repository': found_repo })

    api_available, api_label = _check_api_availability(appid)
    if api_available:
        if errors:
            logger.warn(f'MangoUnlock: GitHub manifest check issues for {appid}: {"; ".join(errors)}; using API fallback')
        _set_manifest_cache_entry(appid_str, True, api_label)
        logger.log(f'MangoUnlock: Manifest available for appid {appid} via API source')
        return json.dumps({ 'success': True, 'available': True, 'repository': api_label })

    if errors:
        logger.warn(f'MangoUnlock: Manifest availability check failed for {appid}: {"; ".join(errors)}')
        return json.dumps({ 'success': False, 'error': 'Manifest lookup failed', 'details': errors })

    _set_manifest_cache_entry(appid_str, False, None)
    logger.log(f'MangoUnlock: Manifest not available for appid {appid}')
    return json.dumps({ 'success': True, 'available': False })

def _restart_steam_internal():
    
    backend_dir = os.path.join(GetPluginDir(), 'backend')
    script_path = os.path.join(backend_dir, 'restart_steam.cmd')
    if not os.path.exists(script_path):
        logger.error(f'MangoUnlock: restart script not found: {script_path}')
        return False
    try:
                                                                   
        CREATE_NO_WINDOW = 0x08000000
        subprocess.Popen(['cmd', '/C', script_path], creationflags=CREATE_NO_WINDOW)
        logger.log('MangoUnlock: Restart script launched (hidden)')
        return True
    except Exception as e:
        logger.error(f'MangoUnlock: Failed to launch restart script: {e}')
        return False

def RestartSteam(contentScriptQuery: str = '') -> str:
    
    success = _restart_steam_internal()
    if success:
        return json.dumps({ 'success': True })
    else:
        return json.dumps({ 'success': False, 'error': 'Failed to restart Steam' })

                                                        

def _check_repo_branch(repo: str, branch: str):
    
    _ensure_http_client()
    url = f'https://api.github.com/repos/{repo}/branches/{branch}'
    try:
        resp = HTTP_CLIENT.get(url, headers=GITHUB_JSON_HEADERS, timeout=15)
    except Exception as e:
        logger.warn(f'MangoUnlock: Branch check failed for manifest branch {branch}: {e}')
        return False, str(e)
    if resp.status_code == 200:
        return True, None
    if resp.status_code == 404:
        return False, None
    logger.warn(f'MangoUnlock: Branch check unexpected status {resp.status_code} for manifest branch {branch}')
    return False, f'HTTP {resp.status_code}'

def _github_branch_exists(repo: str, branch: str) -> bool:
    
    exists, _ = _check_repo_branch(repo, branch)
    return exists

def _github_fetch_tree(repo: str, branch: str) -> list:
    
    _ensure_http_client()
    url = f'https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1'
    try:
        resp = HTTP_CLIENT.get(url, headers=GITHUB_JSON_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warn(f'MangoUnlock: Failed to fetch tree for manifest branch {branch}: {e}')
        return []
    tree = data.get('tree', [])
    if not isinstance(tree, list):
        return []
    return tree

def _pick_lua_path(entries: list, appid: str) -> str:
    
    preferred = None
    fallback = None
    expected = f'{appid}.lua'
    for entry in entries:
        if entry.get('type') != 'blob':
            continue
        path = entry.get('path') or ''
        if not path.lower().endswith('.lua'):
            continue
        fallback = fallback or path
        if os.path.basename(path).lower() == expected:
            preferred = path
            break
    return preferred or fallback or ''

def _github_download_lua(repo: str, branch: str, path: str) -> str:
    
    _ensure_http_client()
    url = f'https://raw.githubusercontent.com/{repo}/{branch}/{path}'
    try:
        resp = HTTP_CLIENT.get(url, headers=GITHUB_RAW_HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        raise RuntimeError(f'Failed to download {path} for manifest branch {branch}: {e}') from e

def _strip_lua_to_addappid(lua_text: str) -> str:
    
    lines = []
    for raw_line in lua_text.splitlines():
        match = ADDAPP_PATTERN.search(raw_line)
        if not match:
            continue
        args = match.group(1).strip()
        if not args:
            continue
        if '--' in args:
            args = args.split('--', 1)[0].rstrip()
        lines.append(f'addappid({args})')
    result = '\n'.join(lines).strip()
    if not result:
        raise RuntimeError('Lua file did not contain any addappid entries')
    return result + '\n'

def _is_valid_zip_file(path: str) -> bool:
    
    try:
        with open(path, 'rb') as fh:
            magic = fh.read(4)
        return magic in (b'PK\x03\x04', b'PK\x05\x06', b'PK\x07\x08')
    except Exception:
        return False

def _process_zip_keep_lua(zip_path: str, appid: int) -> str:
    
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f'Zip not found: {zip_path}')

    backend_dir = _backend_dir()
    os.makedirs(backend_dir, exist_ok=True)
    tmpdir = tempfile.mkdtemp(prefix=f'manifest_{appid}_', dir=backend_dir)
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(tmpdir)

        candidates = []
        for root, _, files in os.walk(tmpdir):
            for fn in files:
                if fn.lower().endswith('.lua'):
                    candidates.append(os.path.join(root, fn))

        if not candidates:
            raise RuntimeError('No Lua file found in archive')

        preferred_name = f'{appid}.lua'
        chosen = None
        for path in candidates:
            if os.path.basename(path) == preferred_name:
                chosen = path
                break
        if chosen is None:
            for path in candidates:
                if re.fullmatch(r'\d+\.lua', os.path.basename(path)):
                    chosen = path
                    break
        chosen = chosen or candidates[0]

        with open(chosen, 'rb') as f:
            data = f.read()
        try:
            text = data.decode('utf-8')
        except UnicodeDecodeError:
            text = data.decode('utf-8', errors='replace')

        return _strip_lua_to_addappid(text)
    finally:
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass
        try:
            if os.path.exists(zip_path):
                os.remove(zip_path)
        except Exception:
            pass

def _install_lua_content(appid: int, lua_text: str) -> str:
    
    base_path = detect_steam_install_path() or Millennium.steam_path() or ''
    target_dir = os.path.join(base_path, 'config', 'stplug-in')
    os.makedirs(target_dir, exist_ok=True)
    dest_file = os.path.join(target_dir, f'{appid}.lua')
    with open(dest_file, 'w', encoding='utf-8', newline='\n') as out:
        out.write(lua_text)
    logger.log(f'MangoUnlock: Installed Lua script -> {dest_file}')
    return dest_file

def _download_lua_via_api_sources(appid: int, dlc_future: dict = None) -> bool:
    """
    Download lua via API sources. If dlc_future is provided, it's a dict
    with 'thread' and 'result' keys for async DLC fetching.
    """
    _ensure_http_client()
    sources = _get_api_sources()
    if not sources:
        logger.warn('MangoUnlock: No API sources available for download')
        return False

    headers = dict(API_DOWNLOAD_HEADERS)
    dest_zip = _backend_path(f'{appid}.zip')
    os.makedirs(_backend_dir(), exist_ok=True)

    for index, api in enumerate(sources):
        if not api.get('enabled', True):
            continue

        name = api.get('name', 'Unknown')
        label = f'API:{name}'
        template = api.get('url', '')
        url = template.replace('<appid>', str(appid))
        success_code = int(api.get('success_code', 200))
        unavailable_code = int(api.get('unavailable_code', 404))

        try:
            if os.path.exists(dest_zip):
                os.remove(dest_zip)
        except Exception:
            pass

        _set_download_state(appid, {
            'status': 'checking',
            'currentRepo': label,
            'bytesRead': 0,
            'totalBytes': 0,
        })
        logger.log(f'MangoUnlock: Attempting API download source #{index + 1} ({label}) for appid {appid}')

        try:
            with HTTP_CLIENT.stream('GET', url, headers=headers, timeout=HTTP_TIMEOUT_SECONDS) as resp:
                status_code = resp.status_code
                if status_code == unavailable_code:
                    logger.log(f'MangoUnlock: API source {label} reports appid {appid} unavailable')
                    continue
                if status_code != success_code:
                    logger.log(f'MangoUnlock: API source {label} unexpected status {status_code} for appid {appid}')
                    continue

                total = int(resp.headers.get('Content-Length') or '0')
                bytes_read = 0
                _set_download_state(appid, {
                    'status': 'downloading',
                    'currentRepo': label,
                    'totalBytes': total,
                    'bytesRead': 0,
                })

                with open(dest_zip, 'wb') as out_file:
                    for chunk in resp.iter_bytes():
                        if not chunk:
                            continue
                        out_file.write(chunk)
                        bytes_read += len(chunk)
                        _set_download_state(appid, { 'bytesRead': bytes_read })

            if not _is_valid_zip_file(dest_zip):
                logger.warn(f'MangoUnlock: API source returned non-zip content for appid {appid} ({label})')
                try:
                    with open(dest_zip, 'rb') as f:
                        preview = f.read(80)
                    logger.warn(f'MangoUnlock: API source preview for appid {appid}: {preview}')
                except Exception:
                    pass
                continue

            processed = _process_zip_keep_lua(dest_zip, appid)
            
            # Inject missing DLCs if DLC fetch completed
            if dlc_future:
                try:
                    dlc_thread = dlc_future.get('thread')
                    if dlc_thread:
                        dlc_thread.join(timeout=5)  # Wait up to 5 seconds
                    dlc_list = dlc_future.get('result', [])
                    if dlc_list:
                        processed = _inject_missing_dlcs_into_lua(processed, dlc_list)
                except Exception as e:
                    logger.warn(f'MangoUnlock: DLC injection failed for appid {appid}: {e}')
            
            _set_download_state(appid, { 'status': 'installing', 'currentRepo': label })
            dest_file = _install_lua_content(appid, processed)

            try:
                fetched_name = _fetch_app_name(appid) or f'UNKNOWN ({appid})'
                _append_loaded_app(appid, fetched_name)
                _log_appid_event('ADDED', appid, fetched_name)
            except Exception:
                pass

            _set_download_state(appid, {
                'status': 'done',
                'success': True,
                'repository': label,
                'installedPath': dest_file,
            })
            logger.log(f'MangoUnlock: Lua installed for appid {appid} via API source ({label})')
            return True
        except httpx.HTTPError as e:
            logger.warn(f'MangoUnlock: API download HTTP error for appid {appid} ({label}): {e}')
        except Exception as e:
            logger.warn(f'MangoUnlock: API download processing failed for appid {appid} ({label}): {e}')
        finally:
            try:
                if os.path.exists(dest_zip):
                    os.remove(dest_zip)
            except Exception:
                pass

    return False

def _set_download_state(appid: int, update: dict) -> None:
    with DOWNLOAD_LOCK:
        state = DOWNLOAD_STATE.get(appid) or {}
        state.update(update)
        DOWNLOAD_STATE[appid] = state

def _get_download_state(appid: int) -> dict:
    with DOWNLOAD_LOCK:
        return DOWNLOAD_STATE.get(appid, {}).copy()

def _download_lua_for_app(appid: int):
    """
    Download lua file for app and inject missing DLCs.
    Starts DLC fetching in parallel with the download for efficiency.
    """
    _ensure_http_client()
    appid_str = str(appid)
    _set_download_state(appid, { 'status': 'queued', 'bytesRead': 0, 'totalBytes': 0, 'currentRepo': None })
    
    # Start DLC fetching in parallel - this runs while we search for/download the lua
    dlc_future = {'thread': None, 'result': []}
    def _fetch_dlcs_async():
        try:
            dlc_future['result'] = _fetch_dlcs_for_app(appid)
        except Exception as e:
            logger.warn(f'MangoUnlock: Async DLC fetch failed for appid {appid}: {e}')
            dlc_future['result'] = []
    
    dlc_thread = threading.Thread(target=_fetch_dlcs_async, name=f'DLCFetch-{appid}', daemon=True)
    dlc_thread.start()
    dlc_future['thread'] = dlc_thread

    for repo in MANIFEST_REPOSITORIES:
        try:
            _set_download_state(appid, { 'status': 'checking', 'currentRepo': repo })
            if not _github_branch_exists(repo, appid_str):
                logger.log(f'MangoUnlock: Branch {appid_str} missing in configured repository')
                continue

            entries = _github_fetch_tree(repo, appid_str)
            lua_path = _pick_lua_path(entries, appid_str)
            if not lua_path:
                logger.log(f'MangoUnlock: No Lua file found for branch {appid_str}')
                continue

            _set_download_state(appid, { 'status': 'downloading', 'currentRepo': repo, 'file': lua_path })
            lua_text = _github_download_lua(repo, appid_str, lua_path)
            lua_bytes = lua_text.encode('utf-8', errors='ignore')
            _set_download_state(appid, { 'bytesRead': len(lua_bytes), 'totalBytes': len(lua_bytes) })

            processed = _strip_lua_to_addappid(lua_text)
            
            # Wait for DLC fetch to complete and inject missing DLCs
            try:
                dlc_thread.join(timeout=10)  # Wait up to 10 seconds for DLC fetch
                dlc_list = dlc_future.get('result', [])
                if dlc_list:
                    processed = _inject_missing_dlcs_into_lua(processed, dlc_list)
            except Exception as e:
                logger.warn(f'MangoUnlock: DLC injection failed for appid {appid}: {e}')
            
            _set_download_state(appid, { 'status': 'installing', 'currentRepo': repo })
            dest_file = _install_lua_content(appid, processed)

            try:
                fetched_name = _fetch_app_name(appid) or f'UNKNOWN ({appid})'
                _append_loaded_app(appid, fetched_name)
                _log_appid_event('ADDED', appid, fetched_name)
            except Exception:
                pass

            _set_download_state(appid, { 'status': 'done', 'success': True, 'repository': repo, 'installedPath': dest_file })
            logger.log(f'MangoUnlock: Lua installed for {appid} from manifest repository')
            return
        except Exception as e:
            logger.warn(f'MangoUnlock: Repository download failed for appid {appid}: {e}')
            continue

    # Pass DLC future to API sources download
    if _download_lua_via_api_sources(appid, dlc_future):
        return

    _set_download_state(appid, { 'status': 'failed', 'error': 'Lua not available in configured repositories' })

def StartAddViaMangoUnlock(appid: int, contentScriptQuery: str = '') -> str:
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({ 'success': False, 'error': 'Invalid appid' })
    logger.log(f'MangoUnlock: StartAddViaMangoUnlock appid={appid}')
                 
    _set_download_state(appid, { 'status': 'queued', 'bytesRead': 0, 'totalBytes': 0, 'currentRepo': None })
    t = threading.Thread(target=_download_lua_for_app, args=(appid,), daemon=True)
    t.start()
    return json.dumps({ 'success': True })

def GetAddViaMangoUnlockStatus(appid: int, contentScriptQuery: str = '') -> str:
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({ 'success': False, 'error': 'Invalid appid' })
    state = _get_download_state(appid)
    return json.dumps({ 'success': True, 'state': state })

def GetIconDataUrl(contentScriptQuery: str = '') -> str:
    try:
        steamUIPath = os.path.join(Millennium.steam_path(), "steamui", WEBKIT_DIR_NAME)
        icon_path = os.path.join(steamUIPath, WEB_UI_ICON_FILE)
        if not os.path.exists(icon_path):
            icon_path = os.path.join(GetPluginDir(), 'public', WEB_UI_ICON_FILE)
        with open(icon_path, 'rb') as f:
            data = f.read()
        b64 = base64.b64encode(data).decode('ascii')
        return json.dumps({ 'success': True, 'dataUrl': f'data:image/ico;base64,{b64}' })
    except Exception as e:
        logger.warn(f'MangoUnlock: GetIconDataUrl failed: {e}')
        return json.dumps({ 'success': False, 'error': str(e) })

def DeleteMangoUnlockForApp(appid: int, contentScriptQuery: str = '') -> str:
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({ 'success': False, 'error': 'Invalid appid' })
    base = detect_steam_install_path() or Millennium.steam_path()
    target_dir = os.path.join(base or '', 'config', 'stplug-in')
    paths = [
        os.path.join(target_dir, f"{appid}.lua"),
        os.path.join(target_dir, f"{appid}.lua.disabled"),
    ]
    deleted = []
    for p in paths:
        try:
            if os.path.exists(p):
                os.remove(p)
                deleted.append(p)
        except Exception as e:
            logger.warn(f'MangoUnlock: Failed to delete {p}: {e}')
                                                      
    try:
        name = _get_loaded_app_name(appid) or _fetch_app_name(appid) or f'UNKNOWN ({appid})'
        _remove_loaded_app(appid)
        if deleted:
            _log_appid_event('REMOVED', appid, name)
    except Exception:
        pass
    return json.dumps({ 'success': True, 'deleted': deleted, 'count': len(deleted) })

LOADED_APPS_FILE = 'loadedappids.txt'

def _loaded_apps_path() -> str:
    return _backend_path(LOADED_APPS_FILE)


def _fetch_app_name(appid: int) -> str:
    _ensure_http_client()
    try:
        url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
        resp = HTTP_CLIENT.get(url, follow_redirects=True, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        entry = data.get(str(appid)) or data.get(int(appid)) or {}
        if isinstance(entry, dict):
            inner = entry.get('data') or {}
            name = inner.get('name')
            if isinstance(name, str) and name.strip():
                return name.strip()
    except Exception as e:
        logger.warn(f'MangoUnlock: _fetch_app_name failed for {appid}: {e}')
    return ''


def _append_loaded_app(appid: int, name: str) -> None:
    try:
        path = _loaded_apps_path()
        lines = []
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                lines = f.read().splitlines()

        prefix = f"{appid}:"
        lines = [ln for ln in lines if not ln.startswith(prefix)]
        lines.append(f"{appid}:{name}")
        with open(path, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines) + "\n")
    except Exception as e:
        logger.warn(f'MangoUnlock: _append_loaded_app failed for {appid}: {e}')


def _remove_loaded_app(appid: int) -> None:
    try:
        path = _loaded_apps_path()
        if not os.path.exists(path):
            return
        with open(path, 'r', encoding='utf-8') as f:
            lines = f.read().splitlines()
        prefix = f"{appid}:"
        new_lines = [ln for ln in lines if not ln.startswith(prefix)]
        if len(new_lines) != len(lines):
            with open(path, 'w', encoding='utf-8') as wf:
                wf.write("\n".join(new_lines) + ("\n" if new_lines else ""))
    except Exception as e:
        logger.warn(f'MangoUnlock: _remove_loaded_app failed for {appid}: {e}')


def ReadLoadedApps(contentScriptQuery: str = '') -> str:
    try:
        path = _loaded_apps_path()
        entries = []
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                for line in f.read().splitlines():
                    if ':' in line:
                        parts = line.split(':', 1)
                        appid_str = parts[0].strip()
                        name = parts[1].strip()
                        if appid_str.isdigit() and name:
                            entries.append({ 'appid': int(appid_str), 'name': name })
        return json.dumps({ 'success': True, 'apps': entries })
    except Exception as e:
        return json.dumps({ 'success': False, 'error': str(e) })


def DismissLoadedApps(contentScriptQuery: str = '') -> str:
    try:
        path = _loaded_apps_path()
        if os.path.exists(path):
            os.remove(path)
        return json.dumps({ 'success': True })
    except Exception as e:
        return json.dumps({ 'success': False, 'error': str(e) })

APPID_LOG_FILE = 'appidlogs.txt'

def _appid_log_path() -> str:
    return _backend_path(APPID_LOG_FILE)


def _log_appid_event(action: str, appid: int, name: str) -> None:
    try:
        stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        line = f"[{action}] {appid} - {name} - {stamp}\n"
        with open(_appid_log_path(), 'a', encoding='utf-8') as f:
            f.write(line)
    except Exception as e:
        logger.warn(f'MangoUnlock: _log_appid_event failed: {e}')


def _get_loaded_app_name(appid: int) -> str:
    try:
        path = _loaded_apps_path()
        if not os.path.exists(path):
            return ''
        with open(path, 'r', encoding='utf-8') as f:
            for line in f.read().splitlines():
                if line.startswith(f"{appid}:"):
                    return line.split(':', 1)[1].strip()
    except Exception:
        return ''
    return ''

def OpenExternalUrl(url: str, contentScriptQuery: str = '') -> str:
    try:
        s = str(url or '').strip()
        if not (s.startswith('http://') or s.startswith('https://')):
            return json.dumps({ 'success': False, 'error': 'Invalid URL' })
        if sys.platform.startswith('win'):
            try:
                os.startfile(s)                              
            except Exception:
                webbrowser.open(s)
        else:
            webbrowser.open(s)
        return json.dumps({ 'success': True })
    except Exception as e:
        logger.warn(f'MangoUnlock: OpenExternalUrl failed: {e}')
        return json.dumps({ 'success': False, 'error': str(e) })



def _find_steam_path() -> str:
    global STEAM_INSTALL_PATH
    if STEAM_INSTALL_PATH:
        return STEAM_INSTALL_PATH

    if sys.platform.startswith('win') and winreg:
        try:
            try:
                key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r'Software\Valve\Steam')
                steam_path = winreg.QueryValueEx(key, 'SteamPath')[0]
                winreg.CloseKey(key)
                if steam_path and os.path.exists(steam_path):
                    STEAM_INSTALL_PATH = steam_path
                    return steam_path
            except Exception:
                pass

            try:
                key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r'Software\Valve\Steam')
                steam_path = winreg.QueryValueEx(key, 'InstallPath')[0]
                winreg.CloseKey(key)
                if steam_path and os.path.exists(steam_path):
                    STEAM_INSTALL_PATH = steam_path
                    return steam_path
            except Exception:
                pass
        except Exception as e:
            logger.warn(f'MangoUnlock: Failed to read Steam path from registry: {e}')

    return ''
