import { getContainers, changePassword, getSystemStatus, getSettings, updateSettings, containerAction } from "../api.js";
import { confirmDialog, alertDialog } from "../components/modal.js";

const systemStatusPanel = document.getElementById("system-status");
const containerButtons = document.getElementById("container-buttons");
const passwordForm = document.getElementById("password-form");
const passwordError = document.getElementById("password-error");
const settingsForm = document.getElementById("settings-form");
const settingsError = document.getElementById("settings-error");
const sessionTimeoutInput = document.getElementById("session-timeout-minutes");
const reconnectOverlay = document.getElementById("reconnect-overlay");

let initialized = false;
let statusPoller = null;

export async function initializeOps() {

    // Run independently - one failing must never block the others again.
    await Promise.allSettled([
        loadSystemStatus(),
        loadContainers(),
        loadSettings(),
    ]);

    if (!initialized) {
        initializeForms();
        initialized = true;
    }

    if (statusPoller) clearInterval(statusPoller);
    statusPoller = setInterval(loadSystemStatus, 5000);
}

export function teardownOps() {
    if (statusPoller) {
        clearInterval(statusPoller);
        statusPoller = null;
    }
}

async function loadSystemStatus() {
    try {
        const status = await getSystemStatus();

        if (!status) {
            systemStatusPanel.innerHTML = `<div class="empty-state">could not load system status</div>`;
            return;
        }

        const worker = status.worker || {};
        const llm = status.llm || {};

        systemStatusPanel.innerHTML = `
            <div class="source-row">
                <span class="source-path">controller</span>
                <span class="source-type">${status.controller_state || "running"}</span>
            </div>
            <div class="source-row">
                <span class="source-path">worker</span>
                <span class="source-type">${worker.state || "unknown"}</span>
            </div>
            <div class="source-row">
                <span class="source-path">events dropped (live stream)</span>
                <span class="source-type">${worker.events_dropped ?? "-"}</span>
            </div>
            <div class="source-row">
                <span class="source-path">events spilled to disk</span>
                <span class="source-type">${worker.events_spilled ?? "-"}</span>
            </div>
            <div class="source-row">
                <span class="source-path">spool backlog</span>
                <span class="source-type">${worker.spool_backlog ?? "-"}</span>
            </div>
            <div class="source-row">
                <span class="source-path">llm</span>
                <span class="source-type">${llm.reachable ? `reachable (${llm.model})` : "unreachable"}</span>
            </div>
        `;
    } catch (err) {
        console.error("failed to load system status:", err);
        systemStatusPanel.innerHTML = `<div class="empty-state">could not load system status</div>`;
    }
}

async function loadContainers() {
    try {
        const containers = await getContainers();

        containerButtons.innerHTML =
            containers
                .map(c => `
                    <div class="source-row">
                        <span class="source-path">${c.name}${c.is_self ? " (this instance)" : ""}</span>
                        <div>
                            <button class="remove-btn" data-name="${c.name}" data-action="restart" data-self="${c.is_self}">RESTART</button>
                            ${c.is_self ? "" : `
                                <button class="remove-btn" data-name="${c.name}" data-action="stop" data-self="false">STOP</button>
                                <button class="remove-btn" data-name="${c.name}" data-action="start" data-self="false">START</button>
                            `}
                        </div>
                    </div>
                `)
                .join("");

        containerButtons.querySelectorAll("button").forEach(btn => {
            btn.addEventListener("click", () =>
                sendAction(btn.dataset.name, btn.dataset.action, btn.dataset.self === "true")
            );
        });
    } catch (err) {
        console.error("failed to load containers:", err);
        containerButtons.innerHTML = `<div class="empty-state">could not load containers</div>`;
    }
}

async function loadSettings() {
    try {
        const settings = await getSettings();
        if (settings) {
            sessionTimeoutInput.value = Math.round(settings.session_timeout_seconds / 60);
        }
    } catch (err) {
        console.error("failed to load settings:", err);
    }
}

function initializeForms() {

    passwordForm.addEventListener("submit", async e => {
        e.preventDefault();
        passwordError.textContent = "";

        const current = document.getElementById("current-password").value;
        const next = document.getElementById("new-password").value;

        const res = await changePassword(current, next);
        if (!res.ok) {
            passwordError.textContent = await res.text();
            return;
        }

        passwordForm.reset();
        await alertDialog("Password changed.");
    });

    settingsForm.addEventListener("submit", async e => {
        e.preventDefault();
        settingsError.textContent = "";

        const minutes = parseInt(sessionTimeoutInput.value, 10);
        const res = await updateSettings(minutes * 60);

        if (!res.ok) {
            settingsError.textContent = await res.text();
            return;
        }

        await alertDialog("Settings saved.");
    });
}

function waitForReconnect() {
    reconnectOverlay.classList.add("active");

    const poll = setInterval(async () => {
        const status = await getSystemStatus();
        if (status) {
            clearInterval(poll);
            location.reload();
        }
    }, 1000);
}

async function sendAction(name, action, isSelf) {

    const ok = await confirmDialog(`${action.toUpperCase()} "${name}"?`, { danger: action === "stop" });
    if (!ok) return;

    const res = await containerAction(name, action);

    if (!res.ok) {
        await alertDialog(await res.text());
        return;
    }

    if (isSelf && action === "restart") {
        waitForReconnect();
    } else {
        setTimeout(loadSystemStatus, 500);
    }
}