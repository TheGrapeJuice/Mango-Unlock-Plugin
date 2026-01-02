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
.MangoUnlock-unavailable,
.MangoUnlock-request-button,
.MangoUnlock-multiplayer-button{
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
}
.MangoUnlock-multiplayer-button{
    background: linear-gradient(to bottom, #5ba32b 0%, #4a8f24 50%, #3d7a1d 100%) !important;
}
.MangoUnlock-multiplayer-button:hover{
    background: linear-gradient(to bottom, #6cb93c 0%, #5ba32b 50%, #4a8f24 100%) !important;
}
.MangoUnlock-request-button{
    background: linear-gradient(to bottom, #4a90d9 0%, #3d7ab8 50%, #2f6499 100%) !important;
}
.MangoUnlock-request-button:hover{
    background: linear-gradient(to bottom, #5ba0e9 0%, #4a90d9 50%, #3d7ab8 100%) !important;
}
.MangoUnlock-request-button.MangoUnlock-requested{
    background: linear-gradient(to bottom, #5ba32b 0%, #4a8f24 50%, #3d7a1d 100%) !important;
    pointer-events: none !important;
}
.MangoUnlock-multiplayer-button.MangoUnlock-remove-mode{
    background: linear-gradient(to bottom, #8f4444 0%, #7a3838 50%, #652d2d 100%) !important;
}
.MangoUnlock-multiplayer-button.MangoUnlock-remove-mode:hover{
    background: linear-gradient(to bottom, #a55050 0%, #8f4444 50%, #7a3838 100%) !important;
}
.MangoUnlock-input{
    background-color: #233748 !important;
    border: 1px solid #3d6889 !important;
    border-radius: 2px !important;
    padding: 8px 10px !important;
    color: #ffffff !important;
    font-size: 13px !important;
    width: 100% !important;
    box-sizing: border-box !important;
    margin-top: 4px !important;
}
.MangoUnlock-input:focus{
    border-color: #66c0f4 !important;
    outline: none !important;
}
.MangoUnlock-label{
    display: block !important;
    margin-bottom: 12px !important;
    color: #c7d5e0 !important;
    font-size: 13px !important;
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

    function showUpdateConfirmModal(titleText, bodyText, onConfirm, onCancel) {
        const overlay = document.createElement('div');
        overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:100000;display:flex;align-items:center;justify-content:center;';
        overlay.className = 'MangoUnlock-update-overlay';

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
        const updateBtn = createButton('Update Now');
        const laterBtn = createButton('Later');

        laterBtn.onclick = function(ev){
            ev.preventDefault();
            overlay.remove();
            if (typeof onCancel === 'function') {
                try {
                    onCancel();
                } catch (err) {
                    console.warn('[MangoUnlock] Cancel action failed', err);
                }
            }
        };
        updateBtn.onclick = function(ev){
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

        btnRow.appendChild(updateBtn);
        btnRow.appendChild(laterBtn);

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
            '.MangoUnlock-request-button',
            '.MangoUnlock-multiplayer-button',
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

        const mpFixBtn = createButton('🔧 Fix Multiplayer', referenceBtn, 'MangoUnlock-multiplayer-button');
        mpFixBtn.dataset.appid = String(appid);
        mpFixBtn.style.display = 'none';
        mpFixBtn.onclick = function(e){
            e.preventDefault();
            if (state.mpFixApplied) {
                showConfirmModal('Remove Multiplayer Fix', 
                    'Are you sure you want to remove the multiplayer fix? This will restore the original game files.',
                    function() {
                        removeMultiplayerFix(appid, container);
                    });
            } else {
                startMultiplayerFix(appid, container);
            }
        };

        const requestBtn = createButton('📩 Request Game', referenceBtn, 'MangoUnlock-request-button');
        requestBtn.dataset.appid = String(appid);
        requestBtn.style.display = 'none';
        requestBtn.onclick = function(e){
            e.preventDefault();
            if (requestBtn.classList.contains('MangoUnlock-requested')) {
                return;
            }
            const requestSpan = requestBtn.querySelector('span');
            requestSpan.textContent = '📩 Requesting...';
            requestBtn.classList.add('MangoUnlock-disabled');
            backendCall('RequestGame', { appid, contentScriptQuery: '' }).then((res) => {
                const payload = typeof res === 'string' ? JSON.parse(res) : res;
                if (payload && payload.success) {
                    const msg = payload.message || 'Game Requested';
                    requestSpan.textContent = '✅ ' + msg;
                    requestBtn.classList.remove('MangoUnlock-disabled');
                    requestBtn.classList.add('MangoUnlock-requested');
                    state.requested = true;
                    state.requestMessage = msg;
                } else {
                    const errorMsg = payload.error || 'Request Failed';
                    requestSpan.textContent = '❌ ' + errorMsg;
                    requestBtn.classList.remove('MangoUnlock-disabled');
                    requestBtn.classList.add('MangoUnlock-requested');
                    state.requested = true;
                    state.requestMessage = errorMsg;
                }
            }).catch((err) => {
                console.warn('[MangoUnlock] Request game failed', err);
                requestSpan.textContent = '❌ Request Failed';
                requestBtn.classList.remove('MangoUnlock-disabled');
                requestBtn.classList.add('MangoUnlock-requested');
                state.requested = true;
            });
        };

        container.appendChild(restartBtn);
        container.appendChild(addBtn);
        container.appendChild(removeBtn);
        container.appendChild(unavailableBtn);
        container.appendChild(requestBtn);
        container.appendChild(mpFixBtn);

        const addSpan = addBtn.querySelector('span');
        const unavailableSpan = unavailableBtn.querySelector('span');
        const mpFixSpan = mpFixBtn.querySelector('span');
        const requestSpan = requestBtn.querySelector('span');
        const state = {
            exists: null,
            available: null,
            availabilityError: null,
            repository: null,
            indeterminate: false,
            message: null,
            hasMultiplayer: null,
            mpFixApplied: false,
            requested: false,
            requestMessage: null,
        };

        function render() {
            addBtn.style.display = 'none';
            removeBtn.style.display = 'none';
            unavailableBtn.style.display = 'none';
            requestBtn.style.display = 'none';
            mpFixBtn.style.display = 'none';

            if (state.exists === true) {
                removeBtn.style.display = '';
                if (state.hasMultiplayer === true) {
                    mpFixBtn.style.display = '';
                    if (state.mpFixApplied) {
                        mpFixSpan.textContent = '🔧 Remove Fix';
                        mpFixBtn.title = 'Remove the multiplayer fix and restore original files';
                        mpFixBtn.classList.add('MangoUnlock-remove-mode');
                    } else {
                        mpFixSpan.textContent = '🔧 Fix Multiplayer';
                        mpFixBtn.title = 'Apply multiplayer fix for online play';
                        mpFixBtn.classList.remove('MangoUnlock-remove-mode');
                    }
                }
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
                if (state.requested) {
                    requestSpan.textContent = state.requestMessage ? ('✅ ' + state.requestMessage) : '✅ Game Requested';
                    requestBtn.classList.add('MangoUnlock-requested');
                }
                requestBtn.style.display = '';
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
            state.hasMultiplayer = null;
            state.mpFixApplied = false;
            render();

            backendCall('HasMangoUnlockForApp', { appid, contentScriptQuery: '' }).then((res) => {
                const payload = typeof res === 'string' ? JSON.parse(res) : res;
                state.exists = !!(payload && payload.success && payload.exists);
                render();
                
                if (state.exists) {
                    backendCall('CheckGameHasMultiplayer', { appid: appid }).then((mpRes) => {
                        try {
                            const mpPayload = typeof mpRes === 'string' ? JSON.parse(mpRes) : mpRes;
                            if (mpPayload && mpPayload.success) {
                                state.hasMultiplayer = !!mpPayload.has_multiplayer;
                                render();
                                
                                if (state.hasMultiplayer) {
                                    backendCall('IsMultiplayerFixApplied', { appid: appid }).then((fixRes) => {
                                        try {
                                            const fixPayload = typeof fixRes === 'string' ? JSON.parse(fixRes) : fixRes;
                                            if (fixPayload && fixPayload.success) {
                                                state.mpFixApplied = !!fixPayload.is_applied;
                                                render();
                                            }
                                        } catch (err) {
                                            console.warn('[MangoUnlock] Fix applied check parse error', err);
                                        }
                                    }).catch((err) => {
                                        console.warn('[MangoUnlock] Fix applied check failed', err);
                                    });
                                }
                            }
                        } catch (err) {
                            console.warn('[MangoUnlock] Multiplayer check parse error', err);
                        }
                    }).catch((err) => {
                        console.warn('[MangoUnlock] Multiplayer check failed', err);
                    });
                }
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
                    if (payload.isp_blocked) {
                        state.availabilityError = 'ISP blocking connection. Try VPN or change DNS.';
                    }
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

    let updateCheckDone = sessionStorage.getItem('MangoUnlock_updateCheckDone') === 'true';
    
    function checkForUpdates() {
        if (updateCheckDone) return;
        updateCheckDone = true;
        sessionStorage.setItem('MangoUnlock_updateCheckDone', 'true');
        
        backendCall('IsUpdateDismissed', {}).then(function(res) {
            try {
                const payload = typeof res === 'string' ? JSON.parse(res) : res;
                if (payload && payload.dismissed) {
                    console.log('[MangoUnlock] Update already dismissed this session');
                    return;
                }
            } catch (err) {
                console.warn('[MangoUnlock] IsUpdateDismissed parse error', err);
            }
            
            backendCall('GetUpdateMessage', {}).then(function(res) {
                try {
                    const payload = typeof res === 'string' ? JSON.parse(res) : res;
                    if (payload && payload.dismissed) {
                        return;
                    }
                    if (payload && payload.success && payload.message) {
                        const msg = String(payload.message);
                        showUpdateNotification(msg);
                        return;
                    }
                } catch (err) {
                    console.warn('[MangoUnlock] GetUpdateMessage parse error', err);
                }
                
                backendCall('CheckForUpdatesNow', {}).then(function(res2) {
                    try {
                        const payload2 = typeof res2 === 'string' ? JSON.parse(res2) : res2;
                        if (payload2 && payload2.dismissed) {
                            return;
                        }
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
        }).catch(function(err) {
            console.warn('[MangoUnlock] IsUpdateDismissed failed', err);
        });
    }
    
    function showUpdateNotification(message) {
        const isUpdateMsg = message.toLowerCase().includes('update') || message.toLowerCase().includes('available');
        
        if (isUpdateMsg) {
            showUpdateConfirmModal('MangoUnlock Update', message, function() {
                const loadingOverlay = showOverlay('Downloading update...');
                
                backendCall('DownloadAndApplyUpdate', {}).then(function(res) {
                    try {
                        const payload = typeof res === 'string' ? JSON.parse(res) : res;
                        if (payload && payload.success) {
                            loadingOverlay.body.textContent = 'Update installed! Restarting Steam...';
                            setTimeout(function() {
                                backendCall('RestartSteam', {}).catch(function(err) {
                                    console.warn('[MangoUnlock] RestartSteam failed', err);
                                    loadingOverlay.body.textContent = 'Update installed! Please restart Steam manually.';
                                });
                            }, 1000);
                        } else {
                            loadingOverlay.body.textContent = 'Update failed: ' + (payload.error || 'Unknown error');
                        }
                    } catch (err) {
                        loadingOverlay.body.textContent = 'Update failed: ' + err.message;
                    }
                }).catch(function(err) {
                    loadingOverlay.body.textContent = 'Update failed: ' + err.message;
                    console.warn('[MangoUnlock] DownloadAndApplyUpdate failed', err);
                });
            }, function() {
                backendCall('DismissUpdate', {}).then(function() {
                    console.log('[MangoUnlock] Update dismissed on backend, will not prompt again until Steam restart');
                }).catch(function(err) {
                    console.warn('[MangoUnlock] DismissUpdate failed', err);
                });
            });
        } else {
            showOverlay(message);
        }
    }

    function showCredentialsModal(onSave) {
        const overlay = document.createElement('div');
        overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:100000;display:flex;align-items:center;justify-content:center;';
        overlay.className = 'MangoUnlock-credentials-overlay';

        const modal = document.createElement('div');
        modal.style.cssText = 'background:#1b2838;color:#fff;border:1px solid #2a475e;border-radius:4px;min-width:340px;max-width:420px;padding:18px 20px;box-shadow:0 8px 24px rgba(0,0,0,.6);';

        const title = document.createElement('div');
        title.style.cssText = 'font-size:16px;color:#66c0f4;margin-bottom:10px;font-weight:600;';
        title.textContent = 'Online-Fix.me Credentials';

        const info = document.createElement('div');
        info.style.cssText = 'font-size:12px;color:#8f98a0;margin-bottom:16px;';
        info.innerHTML = 'Enter your <a href="https://online-fix.me/" target="_blank" style="color:#66c0f4;">online-fix.me</a> account credentials to download multiplayer fixes.';

        const usernameLabel = document.createElement('label');
        usernameLabel.className = 'MangoUnlock-label';
        usernameLabel.textContent = 'Username:';
        const usernameInput = document.createElement('input');
        usernameInput.type = 'text';
        usernameInput.className = 'MangoUnlock-input';
        usernameInput.placeholder = 'Enter username';
        usernameLabel.appendChild(usernameInput);

        const passwordLabel = document.createElement('label');
        passwordLabel.className = 'MangoUnlock-label';
        passwordLabel.textContent = 'Password:';
        const passwordInput = document.createElement('input');
        passwordInput.type = 'password';
        passwordInput.className = 'MangoUnlock-input';
        passwordInput.placeholder = 'Enter password';
        passwordLabel.appendChild(passwordInput);

        const btnRow = document.createElement('div');
        btnRow.style.cssText = 'display:flex;justify-content:flex-end;gap:8px;margin-top:16px;';
        const saveBtn = createButton('Save');
        const cancelBtn = createButton('Cancel');

        cancelBtn.onclick = function(ev){
            ev.preventDefault();
            overlay.remove();
        };
        saveBtn.onclick = function(ev){
            ev.preventDefault();
            const username = usernameInput.value.trim();
            const password = passwordInput.value.trim();
            if (!username || !password) {
                info.style.color = '#c94a4a';
                info.textContent = 'Please enter both username and password.';
                return;
            }
            backendCall('SaveMultiplayerCredentials', { username: username, password: password }).then(function(res) {
                try {
                    const payload = typeof res === 'string' ? JSON.parse(res) : res;
                    if (payload && payload.success) {
                        overlay.remove();
                        if (typeof onSave === 'function') {
                            onSave();
                        }
                    } else {
                        info.style.color = '#c94a4a';
                        info.textContent = 'Failed to save: ' + (payload.error || 'Unknown error');
                    }
                } catch (err) {
                    info.style.color = '#c94a4a';
                    info.textContent = 'Failed to save credentials.';
                }
            }).catch(function(err) {
                info.style.color = '#c94a4a';
                info.textContent = 'Failed to save credentials.';
            });
        };

        btnRow.appendChild(saveBtn);
        btnRow.appendChild(cancelBtn);

        modal.appendChild(title);
        modal.appendChild(info);
        modal.appendChild(usernameLabel);
        modal.appendChild(passwordLabel);
        modal.appendChild(btnRow);
        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        return overlay;
    }

    function showMultiplayerProgress(appid) {
        const overlay = document.createElement('div');
        overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:99999;display:flex;align-items:center;justify-content:center;';
        overlay.className = 'MangoUnlock-mp-progress-overlay';

        const modal = document.createElement('div');
        modal.style.cssText = 'background:#1b2838;color:#fff;border:1px solid #2a475e;border-radius:4px;min-width:320px;max-width:520px;padding:18px 20px;box-shadow:0 8px 24px rgba(0,0,0,.6);';

        const title = document.createElement('div');
        title.style.cssText = 'font-size:16px;color:#5ba32b;margin-bottom:10px;font-weight:600;';
        title.textContent = 'Multiplayer Fix';

        const status = document.createElement('div');
        status.style.cssText = 'font-size:14px;line-height:1.6;margin-bottom:12px;';
        status.textContent = 'Starting...';

        const logBox = document.createElement('div');
        logBox.style.cssText = 'background:#0e1a26;border:1px solid #2a475e;border-radius:2px;padding:8px;font-family:monospace;font-size:11px;color:#67c1f5;max-height:120px;overflow-y:auto;margin-bottom:12px;';
        logBox.textContent = 'Initializing...';

        const btnRow = document.createElement('div');
        btnRow.style.cssText = 'display:flex;justify-content:flex-end;gap:8px;';
        const closeBtn = createButton('Close');
        closeBtn.style.display = 'none';
        closeBtn.onclick = function(e){
            e.preventDefault();
            overlay.remove();
        };
        btnRow.appendChild(closeBtn);

        modal.appendChild(title);
        modal.appendChild(status);
        modal.appendChild(logBox);
        modal.appendChild(btnRow);
        overlay.appendChild(modal);
        document.body.appendChild(overlay);

        let pollInterval = null;
        let lastMessage = '';

        function addLog(msg) {
            if (msg && msg !== lastMessage) {
                lastMessage = msg;
                const line = document.createElement('div');
                line.textContent = msg;
                logBox.appendChild(line);
                logBox.scrollTop = logBox.scrollHeight;
            }
        }

        function pollStatus() {
            backendCall('GetMultiplayerFixStatus', { appid: appid }).then(function(res) {
                try {
                    const payload = typeof res === 'string' ? JSON.parse(res) : res;
                    if (payload && payload.success && payload.state) {
                        const state = payload.state;
                        const statusText = state.status || 'unknown';
                        const message = state.message || '';
                        
                        if (statusText === 'queued') {
                            status.textContent = 'Queued...';
                        } else if (statusText === 'starting') {
                            status.textContent = 'Starting...';
                            addLog(message || 'Initializing browser...');
                        } else if (statusText === 'searching') {
                            status.textContent = 'Searching...';
                            addLog(message || 'Searching for fix...');
                        } else if (statusText === 'logging_in') {
                            status.textContent = 'Logging in...';
                            addLog(message || 'Authenticating...');
                        } else if (statusText === 'finding_download') {
                            status.textContent = 'Finding download...';
                            addLog(message || 'Locating download link...');
                        } else if (statusText === 'downloading') {
                            status.textContent = 'Downloading...';
                            addLog(message || 'Downloading fix...');
                        } else if (statusText === 'extracting') {
                            status.textContent = 'Extracting...';
                            addLog(message || 'Extracting files...');
                        } else if (statusText === 'done') {
                            status.style.color = '#5ba32b';
                            status.textContent = '✅ Fix installed successfully!';
                            addLog(message || 'Fix installed!');
                            closeBtn.style.display = '';
                            if (pollInterval) {
                                clearInterval(pollInterval);
                                pollInterval = null;
                            }
                            setTimeout(function() {
                                refreshButtons(appid);
                            }, 500);
                        } else if (statusText === 'login_required') {
                            if (pollInterval) {
                                clearInterval(pollInterval);
                                pollInterval = null;
                            }
                            overlay.remove();
                            showCredentialsModal(function() {
                                startMultiplayerFix(appid, null);
                            });
                        } else if (statusText === 'failed') {
                            const errorText = state.error || 'Unknown error';
                            const isCredentialError = errorText.toLowerCase().includes('credential') ||
                                                       errorText.toLowerCase().includes('login') ||
                                                       errorText.toLowerCase().includes('auth') ||
                                                       errorText.toLowerCase().includes('password') ||
                                                       errorText.toLowerCase().includes('username') ||
                                                       errorText.toLowerCase().includes('sign in') ||
                                                       errorText.toLowerCase().includes('logged in');
                            
                            if (pollInterval) {
                                clearInterval(pollInterval);
                                pollInterval = null;
                            }
                            
                            if (isCredentialError) {
                                overlay.remove();
                                showCredentialsModal(function() {
                                    startMultiplayerFix(appid, null);
                                });
                            } else {
                                status.style.color = '#c94a4a';
                                status.textContent = '❌ Fix failed';
                                addLog('Error: ' + errorText);
                                closeBtn.style.display = '';
                            }
                        }
                    }
                } catch (err) {
                    console.warn('[MangoUnlock] MP status parse error', err);
                }
            }).catch(function(err) {
                console.warn('[MangoUnlock] MP status fetch error', err);
            });
        }

        pollStatus();
        pollInterval = setInterval(pollStatus, 1500);

        return { overlay, status, logBox, closeBtn };
    }

    function startMultiplayerFix(appid, container) {
        backendCall('StartMultiplayerFix', { appid: appid }).then(function(res) {
            try {
                const payload = typeof res === 'string' ? JSON.parse(res) : res;
                if (payload && payload.success) {
                    showMultiplayerProgress(appid);
                } else if (payload && payload.need_credentials) {
                    showCredentialsModal(function() {
                        startMultiplayerFix(appid, container);
                    });
                } else {
                    const errorMsg = (payload && payload.error) ? payload.error : 'Failed to start fix';
                    showOverlay('Multiplayer Fix Error: ' + errorMsg);
                }
            } catch (err) {
                showOverlay('Multiplayer Fix Error: ' + err.message);
            }
        }).catch(function(err) {
            showOverlay('Multiplayer Fix Error: ' + err.message);
        });
    }

    function removeMultiplayerFix(appid, container) {
        const overlay = showOverlay('Removing multiplayer fix...');
        
        backendCall('RemoveMultiplayerFix', { appid: appid }).then(function(res) {
            try {
                const payload = typeof res === 'string' ? JSON.parse(res) : res;
                if (payload && payload.success) {
                    overlay.body.textContent = payload.message || 'Fix removed successfully!';
                    overlay.body.style.color = '#5ba32b';
                } else {
                    const errorMsg = (payload && payload.message) ? payload.message : 'Failed to remove fix';
                    overlay.body.textContent = 'Error: ' + errorMsg;
                    overlay.body.style.color = '#c94a4a';
                }
                setTimeout(function() {
                    refreshButtons(appid);
                }, 1000);
            } catch (err) {
                overlay.body.textContent = 'Error: ' + err.message;
                overlay.body.style.color = '#c94a4a';
                setTimeout(function() {
                    refreshButtons(appid);
                }, 1000);
            }
        }).catch(function(err) {
            overlay.body.textContent = 'Error: ' + err.message;
            overlay.body.style.color = '#c94a4a';
            setTimeout(function() {
                refreshButtons(appid);
            }, 1000);
        });
    }

    function init() {
        ensureButtons();
        if (typeof MutationObserver !== 'undefined') {
            const observer = new MutationObserver(() => {
                ensureButtons();
            });
            observer.observe(document.body, { childList: true, subtree: true });
        }
        setInterval(ensureButtons, 2000);
        
        setTimeout(checkForUpdates, 3000);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
