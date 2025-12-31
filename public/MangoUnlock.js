(function(){
    'use strict';

    const PLUGIN_ID = 'mangounlock';
    let buttonInserted = false;
    let pollingTimer = null;
    let lastAppId = null;

    function backendCall(method, payload) {
        return new Promise((resolve, reject) => {
            try {
                if (typeof Millennium === 'undefined' || typeof Millennium.callServerMethod !== 'function') {
                    reject(new Error('Millennium API unavailable'));
                    return;
                }
                Millennium.callServerMethod(PLUGIN_ID, method, payload || {})
                    .then(resolve)
                    .catch(reject);
            } catch (err) {
                reject(err);
            }
        });
    }

    function parseAppId() {
        const match = window.location.href.match(/app\/(\d+)/);
        if (!match) return NaN;
        return parseInt(match[1], 10);
    }

    function createButton(label, referenceBtn, extraClass) {
        const btn = document.createElement('a');
        const baseClass = referenceBtn && referenceBtn.className ? referenceBtn.className : 'btnv6_blue_hoverfade btn_medium';
        btn.className = baseClass;
        if (extraClass) {
            btn.classList.add(extraClass);
        }
        btn.href = '#';
        btn.innerHTML = `<span>${label}</span>`;
        return btn;
    }

    function ensureStyles() {
        if (document.getElementById('MangoUnlock-styles')) {
            return;
        }
        const style = document.createElement('style');
        style.id = 'MangoUnlock-styles';
        style.textContent = `
.MangoUnlock-restart-button,
.MangoUnlock-button,
.MangoUnlock-remove-button,
.MangoUnlock-unavailable{
    margin-left:6px !important;
}
.MangoUnlock-unavailable,
.MangoUnlock-unavailable:hover{
    cursor:default !important;
}
.MangoUnlock-disabled,
.MangoUnlock-disabled:hover{
    pointer-events:none !important;
    opacity:0.6 !important;
    cursor:default !important;
}`;
        document.head.appendChild(style);
    }

    function showOverlay(message) {
        const overlay = document.createElement('div');
        overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:99999;display:flex;align-items:center;justify-content:center;';
        overlay.className = 'MangoUnlock-overlay';

        const modal = document.createElement('div');
        modal.style.cssText = 'background:#1b2838;color:#fff;border:1px solid #2a475e;border-radius:4px;min-width:320px;max-width:520px;padding:18px 20px;box-shadow:0 8px 24px rgba(0,0,0,.6);';

        const title = document.createElement('div');
        title.style.cssText = 'font-size:16px;color:#66c0f4;margin-bottom:10px;font-weight:600;';
        title.textContent = 'MangoUnlock';

        const body = document.createElement('div');
        body.style.cssText = 'font-size:14px;line-height:1.6;margin-bottom:12px;';
        body.textContent = message;

        const progress = document.createElement('div');
        progress.style.cssText = 'height:10px;background:#2a475e;border-radius:4px;overflow:hidden;margin-bottom:12px;display:none;';
        const bar = document.createElement('div');
        bar.style.cssText = 'height:100%;width:0%;background:#66c0f4;transition:width 0.2s ease;';
        progress.appendChild(bar);

        const percent = document.createElement('div');
        percent.style.cssText = 'text-align:right;color:#8f98a0;font-size:12px;display:none;';
        percent.textContent = '0%';

        const btnRow = document.createElement('div');
        btnRow.style.cssText = 'display:flex;justify-content:flex-end;gap:8px;';
        const closeBtn = createButton('Close');
        closeBtn.onclick = function(e){
            e.preventDefault();
            overlay.remove();
        };
        btnRow.appendChild(closeBtn);

        modal.appendChild(title);
        modal.appendChild(body);
        modal.appendChild(progress);
        modal.appendChild(percent);
        modal.appendChild(btnRow);
        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        return { overlay, body, bar, percent, progress };
    }

    function showConfirmModal(titleText, bodyText, onConfirm) {
        const overlay = document.createElement('div');
        overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:100000;display:flex;align-items:center;justify-content:center;';
        overlay.className = 'MangoUnlock-confirm-overlay';

        const modal = document.createElement('div');
        modal.style.cssText = 'background:#1b2838;color:#fff;border:1px solid #2a475e;border-radius:4px;min-width:280px;max-width:420px;padding:18px 20px;box-shadow:0 8px 24px rgba(0,0,0,.6);';

        const title = document.createElement('div');
        title.style.cssText = 'font-size:16px;color:#66c0f4;margin-bottom:10px;font-weight:600;';
        title.textContent = titleText;

        const body = document.createElement('div');
        body.style.cssText = 'font-size:14px;line-height:1.6;margin-bottom:16px;';
        body.textContent = bodyText;

        const btnRow = document.createElement('div');
        btnRow.style.cssText = 'display:flex;justify-content:flex-end;gap:8px;';
        const okBtn = createButton('OK');
        const cancelBtn = createButton('Cancel');

        cancelBtn.onclick = function(ev){
            ev.preventDefault();
            overlay.remove();
        };
        okBtn.onclick = function(ev){
            ev.preventDefault();
            overlay.remove();
            if (typeof onConfirm === 'function') {
                try {
                    onConfirm();
                } catch (err) {
                    console.warn('[MangoUnlock] Confirm action failed', err);
                }
            }
        };

        btnRow.appendChild(okBtn);
        btnRow.appendChild(cancelBtn);

        modal.appendChild(title);
        modal.appendChild(body);
        modal.appendChild(btnRow);
        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        return overlay;
    }

    function updateOverlay(overlayState, status) {
        if (!overlayState) return;
        const { body, bar, percent, progress } = overlayState;
        if (status.status === 'downloading') {
            progress.style.display = 'block';
            percent.style.display = 'block';
            const pct = status.totalBytes > 0 ? Math.floor((status.bytesRead / status.totalBytes) * 100) : 0;
            bar.style.width = pct + '%';
            percent.textContent = pct + '%';
            body.textContent = 'Downloading manifest...';
        } else if (status.status === 'installing' || status.status === 'processing') {
            progress.style.display = 'none';
            percent.style.display = 'none';
            body.textContent = 'Installing manifest...';
        } else if (status.status === 'done') {
            progress.style.display = 'none';
            percent.style.display = 'none';
            body.textContent = 'Done! Restart Steam to load changes.';
        } else if (status.status === 'failed') {
            progress.style.display = 'none';
            percent.style.display = 'none';
            body.textContent = 'Failed: ' + (status.error || 'Unknown error');
        } else {
            body.textContent = 'Preparing download...';
        }
    }

    function stopPolling() {
        if (pollingTimer) {
            clearInterval(pollingTimer);
            pollingTimer = null;
        }
    }

    function cleanupContainer(container) {
        if (!container) return;
        const selectors = [
            '.MangoUnlock-button',
            '.MangoUnlock-remove-button',
            '.MangoUnlock-restart-button',
            '.MangoUnlock-unavailable',
        ];
        selectors.forEach((selector) => {
            container.querySelectorAll(selector).forEach((node) => node.remove());
        });
        if (container._mango) {
            delete container._mango;
        }
        delete container.dataset.mangoAppid;
        buttonInserted = false;
    }

    function refreshButtons(appid) {
        const container = findContainer();
        if (!container) return;
        if (container.dataset.mangoAppid !== String(appid)) return;
        if (container._mango && typeof container._mango.refresh === 'function') {
            container._mango.refresh();
        }
    }

    function pollStatus(appid, overlayState, container) {
        stopPolling();
        pollingTimer = setInterval(() => {
            backendCall('GetAddViaMangoUnlockStatus', { appid }).then((res) => {
                const payload = typeof res === 'string' ? JSON.parse(res) : res;
                if (!payload || !payload.success) return;
                const state = payload.state || {};
                updateOverlay(overlayState, state);
                if (state.status === 'done' || state.status === 'failed') {
                    stopPolling();
                    refreshButtons(appid);
                }
            }).catch(() => {});
        }, 600);
    }

    function startAddFlow(appid, container) {
        const overlayState = showOverlay('Requesting manifest...');
        backendCall('StartAddViaMangoUnlock', { appid, contentScriptQuery: '' }).then((res) => {
            const payload = typeof res === 'string' ? JSON.parse(res) : res;
            if (!payload || !payload.success) {
                overlayState.body.textContent = (payload && payload.error) ? payload.error : 'Failed to start download';
                refreshButtons(appid);
                return;
            }
            pollStatus(appid, overlayState, container);
        }).catch((err) => {
            overlayState.body.textContent = 'Error: ' + err.message;
            refreshButtons(appid);
        });
    }

    function attachButtons(container, appid) {
        ensureStyles();
        cleanupContainer(container);

        const referenceBtn = container.querySelector('a');

        const restartBtn = createButton('Restart Steam', referenceBtn, 'MangoUnlock-restart-button');
        restartBtn.dataset.appid = String(appid);
        restartBtn.onclick = function(e){
            e.preventDefault();
            const confirmMessage = 'Restart Steam now?';
            const triggerRestart = () => {
                backendCall('RestartSteam', { contentScriptQuery: '' }).catch(() => {});
            };
            showConfirmModal('MangoUnlock', confirmMessage, triggerRestart);
        };

        const addBtn = createButton('Add via MangoUnlock', referenceBtn, 'MangoUnlock-button');
        addBtn.dataset.appid = String(appid);
        addBtn.style.display = 'none';
        addBtn.onclick = function(e){
            e.preventDefault();
            startAddFlow(appid, container);
        };

        const removeBtn = createButton('Remove via MangoUnlock', referenceBtn, 'MangoUnlock-remove-button');
        removeBtn.dataset.appid = String(appid);
        removeBtn.style.display = 'none';
        removeBtn.onclick = function(e){
            e.preventDefault();
            backendCall('DeleteMangoUnlockForApp', { appid, contentScriptQuery: '' }).then(() => {
                stopPolling();
                refreshButtons(appid);
            }).catch((err) => {
                console.warn('[MangoUnlock] Remove failed', err);
            });
        };

        const unavailableBtn = createButton('Checking availability...', referenceBtn, 'MangoUnlock-unavailable');
        unavailableBtn.dataset.appid = String(appid);
        unavailableBtn.classList.add('MangoUnlock-disabled');
        unavailableBtn.tabIndex = -1;
        unavailableBtn.style.display = '';
        unavailableBtn.style.pointerEvents = 'none';

        container.appendChild(restartBtn);
        container.appendChild(addBtn);
        container.appendChild(removeBtn);
        container.appendChild(unavailableBtn);

        const addSpan = addBtn.querySelector('span');
        const unavailableSpan = unavailableBtn.querySelector('span');
        const state = {
            exists: null,
            available: null,
            availabilityError: null,
            repository: null,
            indeterminate: false,
            message: null,
        };

        function render() {
            addBtn.style.display = 'none';
            removeBtn.style.display = 'none';
            unavailableBtn.style.display = 'none';

            if (state.exists === true) {
                removeBtn.style.display = '';
                return;
            }

            if (state.exists === null) {
                unavailableSpan.textContent = 'Checking status...';
                unavailableBtn.style.display = '';
                return;
            }

            if (state.availabilityError) {
                unavailableSpan.textContent = state.availabilityError;
                unavailableBtn.style.display = '';
                return;
            }

            if (state.available === true || state.indeterminate) {
                addSpan.textContent = state.indeterminate ? 'Try via MangoUnlock' : 'Add via MangoUnlock';
                addBtn.title = state.indeterminate
                    ? (state.message || 'Availability could not be confirmed, you can still try to download.')
                    : 'Add via MangoUnlock';
                addBtn.style.display = '';
                return;
            }

            if (state.available === false) {
                unavailableSpan.textContent = 'Game not available';
                unavailableBtn.style.display = '';
                return;
            }

            unavailableSpan.textContent = 'Checking availability...';
            unavailableBtn.style.display = '';
        }

        function refresh() {
            state.exists = null;
            state.available = null;
            state.availabilityError = null;
            state.repository = null;
            state.indeterminate = false;
            state.message = null;
            render();

            // Pre-fetch DLCs for this app in the background so they're ready for download
            backendCall('PrefetchDLCsForApp', { appid, contentScriptQuery: '' }).catch(() => {});

            backendCall('HasMangoUnlockForApp', { appid, contentScriptQuery: '' }).then((res) => {
                const payload = typeof res === 'string' ? JSON.parse(res) : res;
                state.exists = !!(payload && payload.success && payload.exists);
                render();
            }).catch(() => {
                state.exists = false;
                render();
            });

            backendCall('CheckManifestAvailability', { appid, contentScriptQuery: '' }).then((res) => {
                const payload = typeof res === 'string' ? JSON.parse(res) : res;
                if (payload && payload.success) {
                    state.available = !!payload.available;
                    state.repository = payload.repository || null;
                    state.indeterminate = !!payload.indeterminate;
                    state.message = payload.message || null;
                    state.availabilityError = null;
                } else {
                    state.available = null;
                    state.availabilityError = (payload && payload.error) ? String(payload.error) : 'Availability check failed';
                }
                render();
            }).catch((err) => {
                state.available = null;
                state.availabilityError = err && err.message ? err.message : 'Availability check failed';
                render();
            });
        }

        container._mango = { state, render, refresh };
        container.dataset.mangoAppid = String(appid);
        buttonInserted = true;
        lastAppId = appid;
        refresh();
    }

    function findContainer() {
        return document.querySelector('.steamdb-buttons') ||
               document.querySelector('[data-steamdb-buttons]') ||
               document.querySelector('.apphub_OtherSiteInfo');
    }

    function ensureButtons() {
        const container = findContainer();
        if (!container) {
            buttonInserted = false;
            lastAppId = null;
            return;
        }

        const currentAppId = parseAppId();
        if (Number.isNaN(currentAppId)) {
            cleanupContainer(container);
            lastAppId = null;
            return;
        }

        if (lastAppId !== currentAppId) {
            stopPolling();
            lastAppId = currentAppId;
        }

        if (container.dataset.mangoAppid !== String(currentAppId)) {
            cleanupContainer(container);
            attachButtons(container, currentAppId);
            return;
        }

        if (!container._mango) {
            cleanupContainer(container);
            attachButtons(container, currentAppId);
            return;
        }

        if (!container.querySelector('.MangoUnlock-restart-button')) {
            cleanupContainer(container);
            attachButtons(container, currentAppId);
        }
    }

    // ==================== AUTO-UPDATE FUNCTIONS ====================
    
    let updateCheckDone = false;
    
    function checkForUpdates() {
        if (updateCheckDone) return;
        updateCheckDone = true;
        
        // First, check if there's a pending message from a previous update check
        backendCall('GetUpdateMessage', {}).then(function(res) {
            try {
                const payload = typeof res === 'string' ? JSON.parse(res) : res;
                if (payload && payload.success && payload.message) {
                    const msg = String(payload.message);
                    // Show update notification to user
                    showUpdateNotification(msg);
                    return;
                }
            } catch (err) {
                console.warn('[MangoUnlock] GetUpdateMessage parse error', err);
            }
            
            // If no pending message, trigger a check now
            backendCall('CheckForUpdatesNow', {}).then(function(res2) {
                try {
                    const payload2 = typeof res2 === 'string' ? JSON.parse(res2) : res2;
                    if (payload2 && payload2.success && payload2.message) {
                        const msg2 = String(payload2.message);
                        showUpdateNotification(msg2);
                    }
                } catch (err2) {
                    console.warn('[MangoUnlock] CheckForUpdatesNow parse error', err2);
                }
            }).catch(function(err) {
                console.warn('[MangoUnlock] CheckForUpdatesNow failed', err);
            });
        }).catch(function(err) {
            console.warn('[MangoUnlock] GetUpdateMessage failed', err);
        });
    }
    
    function showUpdateNotification(message) {
        // Check if this is an update message
        const isUpdateMsg = message.toLowerCase().includes('update') || message.toLowerCase().includes('restart');
        
        if (isUpdateMsg) {
            // Show confirm dialog with Update (restart) and Later options
            showConfirmModal('MangoUnlock Update', message, function() {
                // User clicked OK - restart Steam
                backendCall('RestartSteam', {}).catch(function(err) {
                    console.warn('[MangoUnlock] RestartSteam failed', err);
                });
            });
        } else {
            // Just show an info overlay
            showOverlay(message);
        }
    }
    
    // ==================== END AUTO-UPDATE FUNCTIONS ====================

    function init() {
        ensureButtons();
        if (typeof MutationObserver !== 'undefined') {
            const observer = new MutationObserver(() => ensureButtons());
            observer.observe(document.body, { childList: true, subtree: true });
        }
        setInterval(ensureButtons, 2000);
        
        // Check for updates after a short delay to let UI load first
        setTimeout(checkForUpdates, 3000);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
