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
from urllib.parse import quote
from difflib import SequenceMatcher
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
PENDING_UPDATE_INFO = None  # Stores {'version': ..., 'zip_url': ...} when update is available
PENDING_UPDATE_INFO_LOCK = threading.Lock()
UPDATE_DISMISSED = False  # Track if user dismissed update this session
UPDATE_DISMISSED_LOCK = threading.Lock()

# ==================== MULTIPLAYER FIX CONFIGURATION ====================
MULTIPLAYER_CONFIG_FILE = 'multiplayer.json'
MULTIPLAYER_FIX_LOG_FILE = 'multiplayer_fixes.json'  # Log of applied multiplayer fixes
MULTIPLAYER_CACHE = {}  # Cache for multiplayer check results
MULTIPLAYER_CACHE_LOCK = threading.Lock()
MULTIPLAYER_CACHE_TTL = 30 * 60  # 30 minutes
MULTIPLAYER_FIX_STATE = {}  # Track ongoing multiplayer fix operations
MULTIPLAYER_FIX_STATE_LOCK = threading.Lock()

# Steam API multiplayer category IDs
MULTIPLAYER_CATEGORY_IDS = {
    1,   # Multi-player
    9,   # Co-op
    27,  # Cross-Platform Multiplayer
    36,  # Online PvP
    37,  # Shared/Split Screen PvP
    38,  # Online Co-op
    39,  # Shared/Split Screen Co-op
    49,  # PvP
}

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

def _store_pending_update_info(version: str, zip_url: str) -> None:
    """Store pending update info for when user confirms."""
    global PENDING_UPDATE_INFO
    with PENDING_UPDATE_INFO_LOCK:
        PENDING_UPDATE_INFO = {'version': version, 'zip_url': zip_url}

def _get_pending_update_info() -> dict:
    """Get and clear pending update info."""
    global PENDING_UPDATE_INFO
    with PENDING_UPDATE_INFO_LOCK:
        info = PENDING_UPDATE_INFO
        return info.copy() if info else {}

def _clear_pending_update_info() -> None:
    """Clear pending update info."""
    global PENDING_UPDATE_INFO
    with PENDING_UPDATE_INFO_LOCK:
        PENDING_UPDATE_INFO = None

def _check_for_update_once() -> str:
    """Check for updates (does NOT download). Returns a message for the user if update available."""
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
    
    # Store the update info for when user confirms
    _store_pending_update_info(latest_version, zip_url)
    logger.log(f'AutoUpdate: Update available (current {current_version}, latest {latest_version})')
    return f'MangoUnlock {latest_version} is available. Would you like to update now?'

def _download_and_apply_update() -> dict:
    """Download and apply the pending update. Called when user confirms."""
    info = _get_pending_update_info()
    if not info:
        return {'success': False, 'error': 'No pending update'}
    
    version = info.get('version', '')
    zip_url = info.get('zip_url', '')
    
    if not version or not zip_url:
        return {'success': False, 'error': 'Invalid update info'}
    
    backend_dir = os.path.join(GetPluginDir(), 'backend')
    pending_zip = os.path.join(backend_dir, UPDATE_PENDING_ZIP)
    
    logger.log(f'AutoUpdate: User confirmed, downloading update {version}...')
    
    if not _download_and_extract_update(zip_url, pending_zip):
        return {'success': False, 'error': 'Failed to download update'}
    
    # Extract the update
    try:
        with zipfile.ZipFile(pending_zip, 'r') as archive:
            archive.extractall(GetPluginDir())
        try:
            os.remove(pending_zip)
        except Exception:
            pass
        _clear_pending_update_info()
        logger.log(f'AutoUpdate: Update {version} extracted successfully')
        return {'success': True, 'version': version, 'message': f'Updated to {version}. Restarting Steam...'}
    except Exception as extract_err:
        logger.warn(f'AutoUpdate: Extraction failed: {extract_err}')
        # Save for next startup
        pending_info = os.path.join(backend_dir, UPDATE_PENDING_INFO)
        _write_json(pending_info, {'version': version, 'zip_url': zip_url})
        return {'success': False, 'error': f'Extraction failed: {extract_err}'}

def DownloadAndApplyUpdate(contentScriptQuery: str = '') -> str:
    """Frontend calls this when user clicks Update Now."""
    try:
        result = _download_and_apply_update()
        return json.dumps(result)
    except Exception as exc:
        logger.warn(f'AutoUpdate: DownloadAndApplyUpdate failed: {exc}')
        return json.dumps({'success': False, 'error': str(exc)})

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
    # If user already dismissed, don't return update message
    with UPDATE_DISMISSED_LOCK:
        if UPDATE_DISMISSED:
            return json.dumps({'success': True, 'message': '', 'dismissed': True})
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
    # If user already dismissed, don't return update message
    with UPDATE_DISMISSED_LOCK:
        if UPDATE_DISMISSED:
            return json.dumps({'success': True, 'message': '', 'dismissed': True})
    try:
        message = _get_last_message()
        return json.dumps({'success': True, 'message': message})
    except Exception as exc:
        return json.dumps({'success': False, 'error': str(exc)})

def DismissUpdate(contentScriptQuery: str = '') -> str:
    """Called when user clicks 'Later' - prevents further prompts this session."""
    global UPDATE_DISMISSED
    with UPDATE_DISMISSED_LOCK:
        UPDATE_DISMISSED = True
    logger.log('AutoUpdate: User dismissed update, will not prompt again this session')
    return json.dumps({'success': True})

def IsUpdateDismissed(contentScriptQuery: str = '') -> str:
    """Check if user has dismissed update this session."""
    with UPDATE_DISMISSED_LOCK:
        return json.dumps({'success': True, 'dismissed': UPDATE_DISMISSED})

# ==================== END AUTO-UPDATE FUNCTIONS ====================

# ==================== MULTIPLAYER FIX FUNCTIONS ====================

def _get_multiplayer_config() -> dict:
    """Read multiplayer config (online-fix.me credentials)."""
    cfg_path = os.path.join(GetPluginDir(), 'backend', MULTIPLAYER_CONFIG_FILE)
    return _read_json(cfg_path)

def _save_multiplayer_config(config: dict) -> bool:
    """Save multiplayer config."""
    cfg_path = os.path.join(GetPluginDir(), 'backend', MULTIPLAYER_CONFIG_FILE)
    return _write_json(cfg_path, config)

def _get_multiplayer_fix_log_path() -> str:
    """Get the path to the multiplayer fix log file."""
    return os.path.join(GetPluginDir(), 'backend', MULTIPLAYER_FIX_LOG_FILE)

def _read_multiplayer_fix_log() -> dict:
    """Read the multiplayer fix log. Returns dict of appid -> fix info."""
    return _read_json(_get_multiplayer_fix_log_path())

def _save_multiplayer_fix_log(log: dict) -> bool:
    """Save the multiplayer fix log."""
    return _write_json(_get_multiplayer_fix_log_path(), log)

def _log_multiplayer_fix(appid: int, game_name: str, game_folder: str, added_files: list, backed_up_files: list) -> bool:
    """
    Log a multiplayer fix application.
    
    Args:
        appid: Game app ID
        game_name: Name of the game
        game_folder: Path to the game folder
        added_files: List of files added from the archive (relative paths)
        backed_up_files: List of dicts with 'original' and 'backup' paths (absolute)
    """
    try:
        log = _read_multiplayer_fix_log()
        log[str(appid)] = {
            'appid': appid,
            'game_name': game_name,
            'game_folder': game_folder,
            'added_files': added_files,
            'backed_up_files': backed_up_files,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
        }
        if _save_multiplayer_fix_log(log):
            logger.log(f'Multiplayer: Logged fix for {game_name} ({appid}) - {len(added_files)} files added, {len(backed_up_files)} files backed up')
            return True
        return False
    except Exception as e:
        logger.warn(f'Multiplayer: Failed to log fix for {appid}: {e}')
        return False

def _get_multiplayer_fix_info(appid: int) -> dict:
    """Get the fix info for a specific appid, or empty dict if not found."""
    log = _read_multiplayer_fix_log()
    return log.get(str(appid), {})

def _remove_multiplayer_fix_log_entry(appid: int) -> bool:
    """Remove a multiplayer fix log entry."""
    try:
        log = _read_multiplayer_fix_log()
        if str(appid) in log:
            del log[str(appid)]
            return _save_multiplayer_fix_log(log)
        return True
    except Exception as e:
        logger.warn(f'Multiplayer: Failed to remove log entry for {appid}: {e}')
        return False

def _is_multiplayer_fix_applied(appid: int) -> bool:
    """Check if a multiplayer fix has been applied to a game."""
    info = _get_multiplayer_fix_info(appid)
    return bool(info)

def _get_multiplayer_cache_entry(appid: int):
    """Get cached multiplayer check result."""
    now = time.time()
    with MULTIPLAYER_CACHE_LOCK:
        entry = MULTIPLAYER_CACHE.get(appid)
        if not entry:
            return None
        if now - entry.get('timestamp', 0) > MULTIPLAYER_CACHE_TTL:
            MULTIPLAYER_CACHE.pop(appid, None)
            return None
        return entry.get('has_multiplayer')

def _set_multiplayer_cache_entry(appid: int, has_multiplayer: bool) -> None:
    """Cache multiplayer check result."""
    with MULTIPLAYER_CACHE_LOCK:
        MULTIPLAYER_CACHE[appid] = {
            'timestamp': time.time(),
            'has_multiplayer': has_multiplayer,
        }

def _check_game_has_multiplayer(appid: int) -> bool:
    """Check if a game has multiplayer via Steam API."""
    # Check cache first
    cached = _get_multiplayer_cache_entry(appid)
    if cached is not None:
        return cached
    
    _ensure_http_client()
    try:
        url = f"https://store.steampowered.com/api/appdetails?appids={appid}"
        resp = HTTP_CLIENT.get(url, follow_redirects=True, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        resp.raise_for_status()
        data = resp.json()
        
        entry = data.get(str(appid)) or data.get(int(appid)) or {}
        if not isinstance(entry, dict) or not entry.get('success'):
            # Assume true on failure/missing data
            _set_multiplayer_cache_entry(appid, True)
            return True
        
        inner = entry.get('data') or {}
        categories = inner.get('categories', [])
        
        for cat in categories:
            cat_id = cat.get('id')
            if cat_id in MULTIPLAYER_CATEGORY_IDS:
                logger.log(f'Multiplayer: Game {appid} has multiplayer (category {cat_id})')
                _set_multiplayer_cache_entry(appid, True)
                return True
        
        logger.log(f'Multiplayer: Game {appid} does NOT have multiplayer')
        _set_multiplayer_cache_entry(appid, False)
        return False
    except Exception as e:
        logger.warn(f'Multiplayer: Failed to check multiplayer for {appid}: {e}')
        # Assume true on error
        _set_multiplayer_cache_entry(appid, True)
        return True

def _set_multiplayer_fix_state(appid: int, update: dict) -> None:
    """Update multiplayer fix progress state."""
    with MULTIPLAYER_FIX_STATE_LOCK:
        state = MULTIPLAYER_FIX_STATE.get(appid) or {}
        state.update(update)
        MULTIPLAYER_FIX_STATE[appid] = state

def _get_multiplayer_fix_state(appid: int) -> dict:
    """Get multiplayer fix progress state."""
    with MULTIPLAYER_FIX_STATE_LOCK:
        return MULTIPLAYER_FIX_STATE.get(appid, {}).copy()

def _find_steam_game_folders() -> list:
    """Find all Steam library game folders."""
    steam_paths = []
    found = set()
    
    # Check common Steam paths
    for path in [r"C:\Program Files (x86)\Steam\steamapps\common", r"C:\Program Files\Steam\steamapps\common"]:
        if os.path.exists(path) and path not in found:
            steam_paths.append(path)
            found.add(path)
    
    # Check all drives for SteamLibrary/Steam folders
    for letter in range(ord('A'), ord('Z') + 1):
        drive = f"{chr(letter)}:\\"
        if os.path.exists(drive):
            for sub in ["SteamLibrary", "Steam"]:
                common_path = os.path.join(drive, sub, "steamapps", "common")
                if os.path.exists(common_path) and common_path not in found:
                    steam_paths.append(common_path)
                    found.add(common_path)
    
    return steam_paths

def _find_game_folder_by_name(game_name: str, steam_paths: list) -> str:
    """Find game folder by name."""
    norm = re.sub(r'[^a-zA-Z0-9\s]', '', game_name).lower().strip()
    
    # First pass - exact match
    for path in steam_paths:
        try:
            for folder in os.listdir(path):
                folder_norm = re.sub(r'[^a-zA-Z0-9\s]', '', folder).lower().strip()
                if folder_norm == norm:
                    return os.path.join(path, folder)
        except Exception:
            pass
    
    # Second pass - fuzzy match
    for path in steam_paths:
        try:
            for folder in os.listdir(path):
                folder_norm = re.sub(r'[^a-zA-Z0-9\s]', '', folder).lower().strip()
                if norm in folder_norm or folder_norm in norm:
                    return os.path.join(path, folder)
        except Exception:
            pass
    
    return ''

def _find_game_folder_by_appid(app_id: str, steam_paths: list) -> str:
    """Find game folder by app ID via manifest files."""
    if not app_id:
        return ''
    
    for common in steam_paths:
        lib = os.path.dirname(common)  # steamapps folder
        try:
            for f in os.listdir(lib):
                if f.startswith('appmanifest_') and f.endswith('.acf'):
                    manifest_path = os.path.join(lib, f)
                    try:
                        with open(manifest_path, 'r', encoding='utf-8', errors='ignore') as mf:
                            content = mf.read()
                            if f'"appid"\t\t"{app_id}"' in content or f'"appid"		"{app_id}"' in content:
                                # Extract installdir
                                match = re.search(r'"installdir"\s+"([^"]+)"', content)
                                if match:
                                    install_dir = match.group(1)
                                    full_path = os.path.join(common, install_dir)
                                    if os.path.exists(full_path):
                                        return full_path
                    except Exception:
                        pass
        except Exception:
            pass
    
    return ''

def _detect_archiver() -> tuple:
    """Detect available archiver (WinRAR or 7-Zip)."""
    import shutil as sh
    
    # Check WinRAR
    for p in [sh.which("winrar"), r"C:\Program Files\WinRAR\winrar.exe", r"C:\Program Files (x86)\WinRAR\winrar.exe"]:
        if p and os.path.exists(p):
            return ("winrar", p)
    
    # Check 7-Zip
    for p in [sh.which("7z"), r"C:\Program Files\7-Zip\7z.exe", r"C:\Program Files (x86)\7-Zip\7z.exe"]:
        if p and os.path.exists(p):
            return ("7z", p)
    
    return (None, None)

def _extract_archive(archive: str, target: str, atype: str, apath: str, pwd: str = "online-fix.me") -> bool:
    """Extract archive using WinRAR or 7-Zip."""
    if atype == "winrar":
        cmd = [apath, "x", f"-p{pwd}", "-y", archive, target + os.sep]
    else:
        cmd = [apath, "x", f"-p{pwd}", "-y", f"-o{target}", archive]
    
    try:
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        result = subprocess.run(cmd, check=True, capture_output=True, timeout=300, 
                                startupinfo=startupinfo, creationflags=subprocess.CREATE_NO_WINDOW)
        return True
    except Exception as e:
        logger.warn(f'Multiplayer: Archive extraction failed: {e}')
        return False

def _list_archive_contents(archive: str, atype: str, apath: str, pwd: str = "online-fix.me") -> list:
    """List the contents of an archive without extracting. Returns list of relative file paths."""
    files = []
    logger.log(f'Multiplayer: Listing archive contents: {archive}')
    try:
        if atype == "winrar":
            # WinRAR GUI (winrar.exe) doesn't support list commands well
            # Return empty to trigger directory scanning fallback
            logger.log(f'Multiplayer: WinRAR detected - using directory scan instead of archive listing')
            return []
        else:
            # 7-Zip: l = list, -slt = show technical info
            cmd = [apath, "l", f"-p{pwd}", "-slt", archive]
        
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        
        logger.log(f'Multiplayer: Running archive list command: {atype}')
        result = subprocess.run(cmd, capture_output=True, timeout=30,
                                startupinfo=startupinfo, creationflags=subprocess.CREATE_NO_WINDOW,
                                text=True, encoding='utf-8', errors='ignore')
        
        logger.log(f'Multiplayer: Archive list command completed (exit code: {result.returncode})')
        
        # 7-Zip -slt output has "Path = filename" lines
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith('Path = '):
                path = line[7:]  # Remove "Path = " prefix
                if path and not path.endswith('\\') and not path.endswith('/'):
                    files.append(path)
        
        logger.log(f'Multiplayer: Listed {len(files)} files in archive')
    except subprocess.TimeoutExpired:
        logger.warn(f'Multiplayer: Timeout listing archive contents (>30s) - will use directory scan instead')
    except Exception as e:
        logger.warn(f'Multiplayer: Failed to list archive contents: {e}')
    
    return files

def _scan_directory_files(directory: str) -> dict:
    """
    Scan a directory recursively and return dict of relative_path -> (mtime, size).
    Used for detecting changes before/after extraction.
    """
    files = {}
    try:
        for root, dirs, filenames in os.walk(directory):
            for fname in filenames:
                full_path = os.path.join(root, fname)
                rel_path = os.path.relpath(full_path, directory)
                try:
                    stat = os.stat(full_path)
                    files[rel_path] = (stat.st_mtime, stat.st_size)
                except Exception:
                    pass
    except Exception as e:
        logger.warn(f'Multiplayer: Error scanning directory {directory}: {e}')
    return files

def _compare_directory_scans(before: dict, after: dict) -> tuple:
    """
    Compare two directory scans to find added and modified files.
    Returns (added_files, modified_files) as lists of relative paths.
    Excludes .bak files from the results.
    """
    added = []
    modified = []
    
    for rel_path, (mtime, size) in after.items():
        # Skip .bak files - these are our backups, not from the archive
        if rel_path.endswith('.bak'):
            continue
        if rel_path not in before:
            added.append(rel_path)
        else:
            old_mtime, old_size = before[rel_path]
            if mtime != old_mtime or size != old_size:
                modified.append(rel_path)
    
    return (added, modified)

def _run_extraction_with_timeout(cmd: list, timeout: int = 300) -> tuple:
    """
    Run extraction command with timeout and progress monitoring.
    Returns (success, stdout, stderr, error_message)
    """
    try:
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        
        logger.log(f'Multiplayer: Starting extraction process...')
        start_time = time.time()
        
        # Use Popen for better control
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            startupinfo=startupinfo,
            creationflags=subprocess.CREATE_NO_WINDOW
        )
        
        # Monitor the process with periodic logging
        while True:
            try:
                stdout, stderr = process.communicate(timeout=10)
                elapsed = time.time() - start_time
                logger.log(f'Multiplayer: Extraction completed in {elapsed:.1f}s')
                
                if process.returncode == 0:
                    return (True, stdout, stderr, None)
                else:
                    error_msg = stderr.decode('utf-8', errors='ignore') if stderr else f'Exit code: {process.returncode}'
                    logger.warn(f'Multiplayer: Extraction failed with code {process.returncode}: {error_msg[:200]}')
                    return (False, stdout, stderr, error_msg)
            except subprocess.TimeoutExpired:
                elapsed = time.time() - start_time
                if elapsed > timeout:
                    process.kill()
                    logger.error(f'Multiplayer: Extraction timeout after {elapsed:.1f}s')
                    return (False, None, None, f'Extraction timeout after {timeout}s')
                else:
                    logger.log(f'Multiplayer: Extraction in progress... {elapsed:.0f}s elapsed')
                    continue
                    
    except Exception as e:
        logger.error(f'Multiplayer: Extraction process error: {e}')
        return (False, None, None, str(e))

def _extract_archive_with_backup(archive: str, target: str, atype: str, apath: str, 
                                  appid: int, game_name: str, pwd: str = "online-fix.me") -> tuple:
    """
    Extract archive to target directory with backup of existing files.
    
    Returns tuple of (success, added_files, backed_up_files)
    - added_files: list of relative paths of files added
    - backed_up_files: list of dicts with 'original' and 'backup' absolute paths
    """
    added_files = []
    backed_up_files = []
    use_directory_scan = False
    
    try:
        _set_multiplayer_fix_state(appid, {'status': 'extracting', 'message': 'Analyzing archive contents...'})
        
        # First, try to list all files in the archive
        archive_files = _list_archive_contents(archive, atype, apath, pwd)
        
        if not archive_files:
            logger.warn(f'Multiplayer: Could not list archive contents - will use directory scanning')
            _set_multiplayer_fix_state(appid, {'status': 'extracting', 'message': 'Scanning game directory...'})
            use_directory_scan = True
        else:
            logger.log(f'Multiplayer: Archive contains {len(archive_files)} files for {game_name} ({appid})')
            # Log each file that will be extracted
            for f in archive_files:
                logger.log(f'Multiplayer: Archive file: {f}')
        
        # If using directory scan, capture before state AND backup existing files
        dir_before = {}
        if use_directory_scan:
            logger.log(f'Multiplayer: Scanning directory before extraction: {target}')
            dir_before = _scan_directory_files(target)
            logger.log(f'Multiplayer: Found {len(dir_before)} existing files in game directory')
            
            # Back up ALL existing files before extraction (we don't know which will be replaced)
            # We'll clean up unnecessary backups after extraction
            _set_multiplayer_fix_state(appid, {'status': 'extracting', 'message': f'Backing up {len(dir_before)} existing files...'})
            
            for rel_path in dir_before.keys():
                full_path = os.path.join(target, rel_path)
                backup_path = full_path + '.bak'
                try:
                    # Skip if it's already a .bak file
                    if rel_path.endswith('.bak'):
                        continue
                    # If backup already exists, skip (might be from previous attempt)
                    if os.path.exists(backup_path):
                        continue
                    # Copy the file to .bak (don't rename - we need original for extraction to overwrite)
                    shutil.copy2(full_path, backup_path)
                    logger.log(f'Multiplayer: Pre-backed up {rel_path}')
                except Exception as e:
                    logger.warn(f'Multiplayer: Failed to pre-backup {full_path}: {e}')
            
            _set_multiplayer_fix_state(appid, {'status': 'extracting', 'message': f'Backups created, extracting...'})
        else:
            _set_multiplayer_fix_state(appid, {'status': 'extracting', 'message': f'Preparing {len(archive_files)} files...'})
            
            # Check which files already exist and need backup (only if we have archive list)
            files_to_backup = []
            for rel_path in archive_files:
                # Normalize path separators
                rel_path_norm = rel_path.replace('/', os.sep).replace('\\', os.sep)
                full_path = os.path.join(target, rel_path_norm)
                
                if os.path.exists(full_path) and os.path.isfile(full_path):
                    files_to_backup.append((rel_path_norm, full_path))
                    logger.log(f'Multiplayer: File exists, will backup: {full_path}')
            
            # Backup existing files (rename to .bak)
            if files_to_backup:
                _set_multiplayer_fix_state(appid, {'status': 'extracting', 'message': f'Backing up {len(files_to_backup)} existing files...'})
            
            for rel_path, full_path in files_to_backup:
                backup_path = full_path + '.bak'
                try:
                    # If backup already exists, remove it first
                    if os.path.exists(backup_path):
                        os.remove(backup_path)
                    os.rename(full_path, backup_path)
                    backed_up_files.append({
                        'original': full_path,
                        'backup': backup_path,
                        'relative': rel_path
                    })
                    logger.log(f'Multiplayer: Backed up {full_path} -> {backup_path}')
                except Exception as e:
                    logger.warn(f'Multiplayer: Failed to backup {full_path}: {e}')
        
        _set_multiplayer_fix_state(appid, {'status': 'extracting', 'message': 'Extracting files to game folder...'})
        
        # Now extract the archive using the new method with progress monitoring
        if atype == "winrar":
            cmd = [apath, "x", f"-p{pwd}", "-y", archive, target + os.sep]
        else:
            cmd = [apath, "x", f"-p{pwd}", "-y", f"-o{target}", archive]
        
        success, stdout, stderr, error_msg = _run_extraction_with_timeout(cmd, timeout=300)
        
        if not success:
            raise Exception(error_msg or 'Extraction failed')
        
        _set_multiplayer_fix_state(appid, {'status': 'extracting', 'message': 'Verifying extracted files...'})
        
        if use_directory_scan:
            # Compare directory before and after to find added/modified files
            logger.log(f'Multiplayer: Scanning directory after extraction...')
            dir_after = _scan_directory_files(target)
            new_files, modified_files = _compare_directory_scans(dir_before, dir_after)
            
            logger.log(f'Multiplayer: Directory scan found {len(new_files)} new files, {len(modified_files)} modified files')
            
            # New files are definitely from the archive - these will be deleted on removal
            for rel_path in new_files:
                added_files.append(rel_path)
                logger.log(f'Multiplayer: Added file (detected): {rel_path}')
            
            # Modified files - these were replaced, we have pre-created backups
            # These should NOT be in added_files - they will be restored from backup, not deleted
            for rel_path in modified_files:
                logger.log(f'Multiplayer: Modified file (detected): {rel_path}')
                full_path = os.path.join(target, rel_path)
                backup_path = full_path + '.bak'
                if os.path.exists(backup_path):
                    backed_up_files.append({
                        'original': full_path,
                        'backup': backup_path,
                        'relative': rel_path
                    })
                    logger.log(f'Multiplayer: Backup exists for modified file: {backup_path}')
                else:
                    # No backup exists - shouldn't happen but add to added_files so it gets removed
                    logger.warn(f'Multiplayer: No backup found for modified file: {rel_path}')
                    added_files.append(rel_path)
            
            # Clean up unnecessary backups (files that weren't modified)
            _set_multiplayer_fix_state(appid, {'status': 'extracting', 'message': 'Cleaning up unnecessary backups...'})
            cleaned_count = 0
            for rel_path in dir_before.keys():
                if rel_path.endswith('.bak'):
                    continue
                # If file wasn't modified, remove the backup we created
                if rel_path not in modified_files:
                    backup_path = os.path.join(target, rel_path) + '.bak'
                    try:
                        if os.path.exists(backup_path):
                            os.remove(backup_path)
                            cleaned_count += 1
                    except Exception:
                        pass
            if cleaned_count > 0:
                logger.log(f'Multiplayer: Cleaned up {cleaned_count} unnecessary backup files')
        else:
            # Record all added files from the archive list
            for rel_path in archive_files:
                rel_path_norm = rel_path.replace('/', os.sep).replace('\\', os.sep)
                full_path = os.path.join(target, rel_path_norm)
                if os.path.exists(full_path):
                    added_files.append(rel_path_norm)
                    logger.log(f'Multiplayer: Added file: {full_path}')
        
        # Log the fix
        _log_multiplayer_fix(appid, game_name, target, added_files, backed_up_files)
        
        logger.log(f'Multiplayer: Extraction complete for {game_name} ({appid}) - {len(added_files)} files added, {len(backed_up_files)} files backed up')
        return (True, added_files, backed_up_files)
        
    except Exception as e:
        logger.error(f'Multiplayer: Extraction with backup failed for {appid}: {e}')
        _set_multiplayer_fix_state(appid, {'status': 'extracting', 'message': f'Extraction error: {str(e)[:50]}'})
        
        # Try to restore backed up files on failure
        for backup_info in backed_up_files:
            try:
                if os.path.exists(backup_info['backup']):
                    if os.path.exists(backup_info['original']):
                        os.remove(backup_info['original'])
                    os.rename(backup_info['backup'], backup_info['original'])
                    logger.log(f'Multiplayer: Restored backup on failure: {backup_info["original"]}')
            except Exception as restore_err:
                logger.warn(f'Multiplayer: Failed to restore backup: {restore_err}')
        
        return (False, [], [])

def _remove_multiplayer_fix_files(appid: int) -> tuple:
    """
    Remove a multiplayer fix by deleting added files and restoring backups.
    
    Returns tuple of (success, message)
    """
    try:
        fix_info = _get_multiplayer_fix_info(appid)
        if not fix_info:
            return (True, 'No fix record found - already removed')
        
        game_folder = fix_info.get('game_folder', '')
        added_files = fix_info.get('added_files', [])
        backed_up_files = fix_info.get('backed_up_files', [])
        game_name = fix_info.get('game_name', f'Game {appid}')
        
        if not game_folder or not os.path.exists(game_folder):
            _remove_multiplayer_fix_log_entry(appid)
            logger.log(f'Multiplayer: Game folder no longer exists for {appid}, cleared fix record')
            return (True, 'Game folder no longer exists - fix record cleared')
        
        removed_count = 0
        restored_count = 0
        errors = []
        
        # Remove added files
        for rel_path in added_files:
            full_path = os.path.join(game_folder, rel_path)
            try:
                if os.path.exists(full_path):
                    os.remove(full_path)
                    removed_count += 1
                    logger.log(f'Multiplayer: Removed added file: {full_path}')
            except Exception as e:
                errors.append(f'Failed to remove {rel_path}: {e}')
                logger.warn(f'Multiplayer: Failed to remove {full_path}: {e}')
        
        # Restore backed up files
        for backup_info in backed_up_files:
            original = backup_info.get('original', '')
            backup = backup_info.get('backup', '')
            try:
                if backup and os.path.exists(backup):
                    # Remove the new file if it still exists
                    if os.path.exists(original):
                        os.remove(original)
                    # Restore the backup
                    os.rename(backup, original)
                    restored_count += 1
                    logger.log(f'Multiplayer: Restored backup: {backup} -> {original}')
            except Exception as e:
                errors.append(f'Failed to restore backup: {e}')
                logger.warn(f'Multiplayer: Failed to restore {backup}: {e}')
        
        # Remove the log entry
        _remove_multiplayer_fix_log_entry(appid)
        
        if errors:
            error_str = '; '.join(errors[:3])  # Limit error messages
            return (True, f'Fix removed with {len(errors)} errors: {error_str}')
        
        logger.log(f'Multiplayer: Fix removed for {game_name} ({appid}) - {removed_count} files removed, {restored_count} backups restored')
        return (True, f'Fix removed successfully. {removed_count} files removed, {restored_count} backups restored.')
        
    except Exception as e:
        logger.error(f'Multiplayer: Failed to remove fix for {appid}: {e}')
        return (False, str(e))

def _wait_for_download(folder: str, max_wait: int = 600, appid: int = None) -> str:
    """
    Wait for download to complete in folder.
    
    - If no file appears within 5 seconds, abort with AV blocking error
    - If file exists but size doesn't change, warn about slow connection
    - Returns empty string and sets 'failed' status on error
    """
    start = time.time()
    exts = (".rar", ".zip", ".7z")
    sizes = {}
    stable = {}
    last_size_change_time = time.time()
    last_total_size = 0
    file_found = False
    slow_warning_shown = False
    
    while (time.time() - start) < max_wait:
        try:
            found_any_file = False
            current_total_size = 0
            
            for f in os.listdir(folder):
                full_path = os.path.join(folder, f)
                if not os.path.isfile(full_path):
                    continue
                lower = f.lower()
                if any(lower.endswith(ext) for ext in exts):
                    found_any_file = True
                    file_found = True
                    try:
                        size = os.path.getsize(full_path)
                        current_total_size += size
                        
                        if f in sizes and sizes[f] == size:
                            stable[f] = stable.get(f, 0) + 1
                            if stable[f] >= 3:  # Stable for 3+ seconds (file complete)
                                if appid:
                                    _set_multiplayer_fix_state(appid, {
                                        'status': 'downloading',
                                        'message': f'Download complete ({size // (1024*1024)} MB)'
                                    })
                                return full_path
                        else:
                            stable[f] = 0
                        sizes[f] = size
                        
                        # Update progress message
                        if appid and size > 0:
                            size_mb = size / (1024 * 1024)
                            _set_multiplayer_fix_state(appid, {
                                'status': 'downloading',
                                'message': f'Downloading... {size_mb:.1f} MB'
                            })
                    except Exception:
                        pass
            
            # Track size changes
            if current_total_size > last_total_size:
                last_size_change_time = time.time()
                last_total_size = current_total_size
                slow_warning_shown = False
            
            elapsed = time.time() - start
            time_since_change = time.time() - last_size_change_time
            
            # No file after 5 seconds = AV blocking
            if not file_found and elapsed >= 5:
                logger.error(f'Multiplayer: No download file after {elapsed:.0f}s - antivirus likely blocking')
                if appid:
                    _set_multiplayer_fix_state(appid, {
                        'status': 'failed',
                        'error': 'Download blocked by antivirus. Please disable AV or add an exclusion and try again.'
                    })
                return ''
            
            # File exists but no size change for 10+ seconds = slow connection
            if found_any_file and time_since_change >= 10 and not slow_warning_shown:
                slow_warning_shown = True
                if appid:
                    _set_multiplayer_fix_state(appid, {
                        'status': 'downloading',
                        'message': 'Download slow - check your internet connection...'
                    })
            
            # File exists but no size change for 30+ seconds = stalled, abort
            if found_any_file and time_since_change >= 30:
                logger.error(f'Multiplayer: Download stalled for {time_since_change:.0f}s')
                if appid:
                    _set_multiplayer_fix_state(appid, {
                        'status': 'failed',
                        'error': 'Download stalled - slow connection or file was quarantined by antivirus.'
                    })
                return ''
                    
        except Exception as e:
            logger.warn(f'Multiplayer: Error checking download folder: {e}')
        
        time.sleep(1)  # Check every 1 second for faster response
    
    # Timeout reached
    if appid:
        _set_multiplayer_fix_state(appid, {
            'status': 'failed',
            'error': 'Download timeout - check your connection and try again.'
        })
    
    return ''

def _run_multiplayer_fix_process(appid: int, game_name: str, username: str, password: str) -> None:
    """Run the multiplayer fix download and extraction process."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException
    except ImportError as e:
        _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'Selenium not installed. Please install selenium package.'})
        logger.error(f'Multiplayer: Selenium import failed: {e}')
        return
    
    driver = None
    # Use %APPDATA%/mangoplugin for downloads instead of temp
    appdata = os.environ.get('APPDATA', os.path.expanduser('~'))
    temp_parent = os.path.join(appdata, "mangoplugin")
    temp = os.path.join(temp_parent, "dl")
    os.makedirs(temp, exist_ok=True)
    
    # Clean download folder before starting
    for f in os.listdir(temp):
        try:
            os.remove(os.path.join(temp, f))
        except Exception:
            pass
    
    try:
        _set_multiplayer_fix_state(appid, {'status': 'starting', 'message': 'Initializing...'})
        
        if not username or not password:
            _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'No credentials configured'})
            return
        
        if not game_name:
            _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'Game name not found'})
            return
        
        game_name = game_name.strip()
        clean = re.sub(r'[^\w\s]', '', game_name)
        url = f"https://online-fix.me/index.php?do=search&subaction=search&story={quote(clean)}"
        
        _set_multiplayer_fix_state(appid, {'status': 'starting', 'message': 'Setting up browser...'})
        
        opts = Options()
        opts.add_argument("--window-size=1280,800")
        opts.add_argument("--mute-audio")
        opts.add_argument("--headless")
        opts.add_argument("--log-level=3")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_experimental_option("prefs", {
            "download.default_directory": os.path.abspath(temp),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })
        
        try:
            driver = webdriver.Chrome(service=Service(log_path=os.devnull), options=opts)
        except Exception as e:
            _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': f'Chrome driver error: {str(e)[:100]}'})
            logger.error(f'Multiplayer: WebDriver failed: {e}')
            return
        
        wait = WebDriverWait(driver, 15)
        
        _set_multiplayer_fix_state(appid, {'status': 'searching', 'message': 'Searching for fix...'})
        driver.get(url)
        wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, "a")))
        
        _set_multiplayer_fix_state(appid, {'status': 'searching', 'message': 'Finding best match...'})
        anchors = driver.find_elements(By.TAG_NAME, "a")
        if not anchors:
            _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'No search results found'})
            return
        
        game_lower = game_name.lower()
        best = None
        best_r = 0.0
        
        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
                txt = (a.text or "").strip().lower()
                if not href or "online-fix.me" not in href or "/page/" in href:
                    continue
                if "/games/" not in href and "/engine/" not in href:
                    continue
                ratio = SequenceMatcher(None, game_lower, txt).ratio()
                if ratio > best_r:
                    best_r = ratio
                    best = a
            except Exception:
                pass
        
        if not best or best_r < 0.2:
            _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': f'No suitable match found for "{game_name}"'})
            return
        
        _set_multiplayer_fix_state(appid, {'status': 'logging_in', 'message': 'Logging in...'})
        driver.execute_script("arguments[0].scrollIntoView(true);", best)
        driver.execute_script("arguments[0].click();", best)
        
        # Wait for page to load and check if game is no longer supported
        time.sleep(2)
        try:
            page_source = driver.page_source or ""
            if "Руководство закрыто" in page_source:
                logger.warn(f'Multiplayer: Game {game_name} ({appid}) is no longer supported (Руководство закрыто)')
                _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'Online-fix.me no longer has fix files for this game (guide closed)'})
                return
        except Exception as e:
            logger.warn(f'Multiplayer: Error checking page source: {e}')
        
        try:
            wait.until(EC.presence_of_element_located((By.NAME, "login_name")))
            wait.until(EC.presence_of_element_located((By.NAME, "login_password")))
        except TimeoutException:
            _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'Login form not found'})
            return
        
        ln = driver.find_element(By.NAME, "login_name")
        lp = driver.find_element(By.NAME, "login_password")
        ln.clear()
        ln.send_keys(username)
        lp.clear()
        lp.send_keys(password)
        
        try:
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='Вход'] | //button[contains(text(),'Вход')]")))
            driver.execute_script("arguments[0].scrollIntoView(true);", btn)
            driver.execute_script("arguments[0].click();", btn)
        except TimeoutException:
            lp.send_keys(Keys.ENTER)
        
        _set_multiplayer_fix_state(appid, {'status': 'finding_download', 'message': 'Finding download link...'})
        
        download_xpath = "//a[contains(text(),'Скачать фикс с сервера')] | //button[contains(text(),'Скачать фикс с сервера')]"
        # Use a shorter 10-second timeout for finding download link after login
        # If login failed, this will timeout quickly and show login_required
        short_wait = WebDriverWait(driver, 10)
        try:
            short_wait.until(EC.presence_of_element_located((By.XPATH, download_xpath)))
        except TimeoutException:
            _set_multiplayer_fix_state(appid, {'status': 'login_required', 'error': 'Login required - please enter credentials'})
            return
        
        btns = driver.find_elements(By.XPATH, download_xpath)
        if not btns:
            btns = driver.find_elements(By.XPATH, "//a[contains(text(),'Download the fix')] | //button[contains(text(),'Download the fix')]")
        
        if not btns:
            _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'Download button not found'})
            return
        
        dl_btn = btns[0]
        _set_multiplayer_fix_state(appid, {'status': 'downloading', 'message': 'Starting download...'})
        driver.execute_script("arguments[0].scrollIntoView(true);", dl_btn)
        driver.execute_script("arguments[0].click();", dl_btn)
        
        # Wait for new tab
        try:
            wait.until(lambda d: len(d.window_handles) > 1)
        except TimeoutException:
            pass
        
        # Switch to uploads tab if opened
        for h in driver.window_handles:
            driver.switch_to.window(h)
            if "uploads.online-fix.me" in driver.current_url.lower():
                break
        
        # Check if download page shows 401 Authorization Required (game no longer supported)
        time.sleep(1)
        try:
            page_source = driver.page_source or ""
            page_title = driver.title or ""
            if "401 Authorization Required" in page_source or "401 Authorization Required" in page_title:
                logger.warn(f'Multiplayer: Game {game_name} ({appid}) download returned 401 - no longer supported')
                _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'Online-fix.me no longer has fix files for this game (401 unauthorized)'})
                return
        except Exception as e:
            logger.warn(f'Multiplayer: Error checking download page: {e}')
        
        # Look for Fix Repair link
        try:
            wait.until(EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, "Fix Repair")))
        except TimeoutException:
            pass
        
        fix_links = driver.find_elements(By.PARTIAL_LINK_TEXT, "Fix Repair")
        if fix_links:
            try:
                driver.execute_script("arguments[0].scrollIntoView(true);", fix_links[0])
                driver.execute_script("arguments[0].click();", fix_links[0])
                time.sleep(2)
                
                # Check for 401 after clicking Fix Repair link
                try:
                    page_source = driver.page_source or ""
                    page_title = driver.title or ""
                    if "401 Authorization Required" in page_source or "401 Authorization Required" in page_title:
                        logger.warn(f'Multiplayer: Game {game_name} ({appid}) Fix Repair returned 401 - no longer supported')
                        _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'Online-fix.me no longer has fix files for this game (401 unauthorized)'})
                        return
                except Exception:
                    pass
            except Exception:
                pass
            
            # Find and click actual download link
            all_links = driver.find_elements(By.TAG_NAME, "a")
            for lnk in all_links:
                href = lnk.get_attribute("href") or ""
                if "uploads.online-fix.me" in href and any(ext in href.lower() for ext in [".rar", ".zip", ".7z"]):
                    try:
                        driver.execute_script("arguments[0].click();", lnk)
                        break
                    except Exception:
                        pass
        else:
            # No Fix Repair link found - check if the page itself shows 401
            try:
                page_source = driver.page_source or ""
                page_title = driver.title or ""
                if "401 Authorization Required" in page_source or "401 Authorization Required" in page_title:
                    logger.warn(f'Multiplayer: Game {game_name} ({appid}) uploads page returned 401 - no longer supported')
                    _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'Online-fix.me no longer has fix files for this game (401 unauthorized)'})
                    return
            except Exception:
                pass
        
        _set_multiplayer_fix_state(appid, {'status': 'downloading', 'message': 'Waiting for download...'})
        dl = _wait_for_download(temp, max_wait=600, appid=appid)
        
        if not dl:
            # Error message already set by _wait_for_download if appid provided
            # Only set generic error if not already set
            state = _get_multiplayer_fix_state(appid)
            if state.get('status') != 'failed':
                _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'Download timeout - check antivirus or connection'})
            return
        
        _set_multiplayer_fix_state(appid, {'status': 'extracting', 'message': 'Extracting fix...'})
        
        # Find game folder
        steam_paths = _find_steam_game_folders()
        if not steam_paths:
            _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'No Steam library found'})
            return
        
        gf = _find_game_folder_by_appid(str(appid), steam_paths)
        if not gf:
            gf = _find_game_folder_by_name(game_name, steam_paths)
        
        if not gf:
            _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': f'Game folder not found for {game_name}'})
            return
        
        atype, apath = _detect_archiver()
        if not atype:
            _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'No archiver found (need WinRAR or 7-Zip)'})
            return
        
        # Use the new extraction with backup function
        success, added_files, backed_up_files = _extract_archive_with_backup(dl, gf, atype, apath, appid, game_name)
        if not success:
            _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': 'Extraction failed'})
            return
        
        # Cleanup
        try:
            os.remove(dl)
        except Exception:
            pass
        
        _set_multiplayer_fix_state(appid, {
            'status': 'done',
            'success': True,
            'message': f'Fix installed to {gf} ({len(added_files)} files added, {len(backed_up_files)} backed up)'
        })
        logger.log(f'Multiplayer: Fix installed for {game_name} ({appid}) to {gf}')
        
    except Exception as e:
        logger.error(f'Multiplayer: Fix process failed for {appid}: {e}')
        _set_multiplayer_fix_state(appid, {'status': 'failed', 'error': str(e)[:150]})
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        # Always cleanup the contents inside mangoplugin folder (keep folder for AV exclusion)
        try:
            appdata = os.environ.get('APPDATA', os.path.expanduser('~'))
            mango_folder = os.path.join(appdata, "mangoplugin")
            if os.path.exists(mango_folder):
                for item in os.listdir(mango_folder):
                    item_path = os.path.join(mango_folder, item)
                    try:
                        if os.path.isfile(item_path):
                            os.remove(item_path)
                        elif os.path.isdir(item_path):
                            shutil.rmtree(item_path, ignore_errors=True)
                    except Exception:
                        pass
                logger.log(f'Multiplayer: Cleaned up contents of {mango_folder}')
        except Exception as cleanup_err:
            logger.warn(f'Multiplayer: Failed to cleanup mangoplugin folder: {cleanup_err}')

# ===== Multiplayer Fix Frontend API Functions =====

def CheckGameHasMultiplayer(appid: int, contentScriptQuery: str = '') -> str:
    """Check if a game has multiplayer (called from frontend)."""
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({'success': False, 'error': 'Invalid appid'})
    
    has_mp = _check_game_has_multiplayer(appid)
    return json.dumps({'success': True, 'has_multiplayer': has_mp})

def GetMultiplayerCredentials(contentScriptQuery: str = '') -> str:
    """Get multiplayer credentials (without password)."""
    config = _get_multiplayer_config()
    has_creds = bool(config.get('username')) and bool(config.get('password'))
    return json.dumps({
        'success': True,
        'has_credentials': has_creds,
        'username': config.get('username', '')
    })

def SaveMultiplayerCredentials(username: str, password: str, contentScriptQuery: str = '') -> str:
    """Save multiplayer credentials."""
    try:
        username = str(username or '').strip()
        password = str(password or '').strip()
        
        if not username or not password:
            return json.dumps({'success': False, 'error': 'Username and password required'})
        
        config = _get_multiplayer_config()
        config['username'] = username
        config['password'] = password
        
        if _save_multiplayer_config(config):
            logger.log('Multiplayer: Credentials saved')
            return json.dumps({'success': True})
        else:
            return json.dumps({'success': False, 'error': 'Failed to save config'})
    except Exception as e:
        return json.dumps({'success': False, 'error': str(e)})

def StartMultiplayerFix(appid: int, contentScriptQuery: str = '') -> str:
    """Start the multiplayer fix process for a game."""
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({'success': False, 'error': 'Invalid appid'})
    
    # Get credentials
    config = _get_multiplayer_config()
    username = config.get('username', '')
    password = config.get('password', '')
    
    if not username or not password:
        return json.dumps({'success': False, 'error': 'No credentials configured', 'need_credentials': True})
    
    # Get game name
    game_name = _fetch_app_name(appid) or f'Game {appid}'
    
    # Start fix in background thread
    _set_multiplayer_fix_state(appid, {'status': 'queued', 'message': 'Starting...'})
    
    t = threading.Thread(
        target=_run_multiplayer_fix_process,
        args=(appid, game_name, username, password),
        daemon=True
    )
    t.start()
    
    logger.log(f'Multiplayer: Started fix process for {game_name} ({appid})')
    return json.dumps({'success': True, 'game_name': game_name})

def GetMultiplayerFixStatus(appid: int, contentScriptQuery: str = '') -> str:
    """Get the current status of a multiplayer fix operation."""
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({'success': False, 'error': 'Invalid appid'})
    
    state = _get_multiplayer_fix_state(appid)
    return json.dumps({'success': True, 'state': state})

def IsMultiplayerFixApplied(appid: int, contentScriptQuery: str = '') -> str:
    """Check if a multiplayer fix has been applied to a game."""
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({'success': False, 'error': 'Invalid appid'})
    
    is_applied = _is_multiplayer_fix_applied(appid)
    fix_info = _get_multiplayer_fix_info(appid) if is_applied else {}
    
    return json.dumps({
        'success': True,
        'is_applied': is_applied,
        'game_name': fix_info.get('game_name', ''),
        'timestamp': fix_info.get('timestamp', ''),
        'files_count': len(fix_info.get('added_files', [])),
        'backups_count': len(fix_info.get('backed_up_files', []))
    })

def RemoveMultiplayerFix(appid: int, contentScriptQuery: str = '') -> str:
    """Remove a previously applied multiplayer fix."""
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({'success': False, 'error': 'Invalid appid'})
    
    success, message = _remove_multiplayer_fix_files(appid)
    
    if success:
        logger.log(f'Multiplayer: Fix removed for appid {appid}')
    else:
        logger.warn(f'Multiplayer: Failed to remove fix for appid {appid}: {message}')
    
    return json.dumps({
        'success': success,
        'message': message
    })

def GetMultiplayerFixInfo(appid: int, contentScriptQuery: str = '') -> str:
    """Get detailed info about an applied multiplayer fix."""
    try:
        appid = int(appid)
    except Exception:
        return json.dumps({'success': False, 'error': 'Invalid appid'})
    
    fix_info = _get_multiplayer_fix_info(appid)
    if not fix_info:
        return json.dumps({'success': False, 'error': 'No fix found for this game'})
    
    return json.dumps({
        'success': True,
        'info': {
            'appid': fix_info.get('appid'),
            'game_name': fix_info.get('game_name', ''),
            'game_folder': fix_info.get('game_folder', ''),
            'timestamp': fix_info.get('timestamp', ''),
            'added_files': fix_info.get('added_files', []),
            'backed_up_files': fix_info.get('backed_up_files', [])
        }
    })

# ==================== END MULTIPLAYER FIX FUNCTIONS ====================
                                                    

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

