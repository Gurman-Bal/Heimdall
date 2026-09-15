
import { getEvents, getEventNoise, clearEvents } from "../api.js";
import { eventRow } from "../components/eventRow.js";
import { enableExpandableRows } from "../utils.js";

let events = [];
let noise = [];
let currentFilter = "all";
let paused = false;
let pendingCount = 0;
let initialized = false;
let eventStream = null;
let reconnectTimer = null;

const eventList = document.getElementById("event-list");
const pauseBtn = document.getElementById("watch-pause-btn");
const statusDot = document.getElementById("status-dot");
const statusText = document.getElementById("status-text");
const bifrost = document.getElementById("bifrost");
const clearEventsBtn = document.getElementById("clear-events-btn");

export async function initializeWatch() {
    if (initialized) return;
    initialized = true;

    events = await getEvents();

    initializeFilters();
    pauseBtn.addEventListener("click", togglePause);
    clearEventsBtn.addEventListener("click", clearAllEvents);

    enableExpandableRows(eventList);

    renderEvents();
    updateStatus();
    connectStream();
}

function initializeFilters() {
    document
        .querySelectorAll("#view-watch .filter-btn[data-severity]")
        .forEach(btn => {
            btn.addEventListener("click", async () => {
                document
                    .querySelectorAll(
                        "#view-watch .filter-btn[data-severity]"
                    )
                    .forEach(b => b.classList.remove("active"));

                btn.classList.add("active");
                currentFilter = btn.dataset.severity;

                if (currentFilter === "noise") {
                    await refreshNoise();
                } else {
                    renderEvents();
                }
            });
        });
}

function togglePause() {
    paused = !paused;

    pauseBtn.textContent = paused
        ? `RESUME (${pendingCount})`
        : "PAUSE";

    pauseBtn.classList.toggle("active", paused);

    if (!paused) {
        pendingCount = 0;
        renderEvents();
        updateStatus();
    }
}

function renderEvents() {
    if (currentFilter === "noise") return;

    const filtered = currentFilter === "all"
        ? events
        : events.filter(e => e.Severity === currentFilter);

    if (filtered.length === 0) {
        eventList.innerHTML = `
            <div class="empty-state">
                no events
                ${
            currentFilter !== "all"
                ? ` at ${currentFilter} severity`
                : " yet - heimdall is watching"
        }
            </div>
        `;
        return;
    }

    eventList.innerHTML = filtered.map(eventRow).join("");
}

function appendEvent(e) {
    if (paused) {
        pendingCount++;
        pauseBtn.textContent = `RESUME (${pendingCount})`;
        return;
    }

    if (currentFilter === "noise") return;

    const matchesFilter =
        currentFilter === "all" ||
        e.Severity === currentFilter;

    if (!matchesFilter) return;

    const empty = eventList.querySelector(".empty-state");
    if (empty) empty.remove();

    const wrapper = document.createElement("div");
    wrapper.innerHTML = eventRow(e);

    const row = wrapper.firstElementChild;
    eventList.prepend(row);
}

function prependEvent(e) {
    if (e.Severity === "ignore") return;

    events.unshift(e);

    if (events.length > 200) {
        events.pop();
    }

    appendEvent(e);
    updateStatus();
}

async function refreshNoise() {
    noise = await getEventNoise();

    if (currentFilter !== "noise") return;

    if (!noise || noise.length === 0) {
        eventList.innerHTML =
            `<div class="empty-state">no noise recorded</div>`;
        return;
    }

    eventList.innerHTML = noise.map(n => `
        <div class="event-row noise">
            <span class="noise-count">${n.count}×</span>
            <span class="event-source">${n.source}</span>
            <span class="event-type">${n.type}</span>
            <span class="event-message">${n.message}</span>
            <span class="noise-last">
                last seen: ${new Date(n.last_seen).toLocaleTimeString()}
            </span>
        </div>
    `).join("");
}

function updateStatus() {
    const hasCritical = events.some(
        e => e.Severity === "critical"
    );

    const hasWarning = events.some(
        e => e.Severity === "warning"
    );

    const level = hasCritical
        ? "critical"
        : hasWarning
            ? "warning"
            : "info";

    statusDot.className = `status-dot ${level}`;

    statusText.textContent = hasCritical
        ? "critical events active"
        : hasWarning
            ? "warnings present"
            : "nominal";

    bifrost.className = `bifrost ${level}`;
}

function connectStream() {
    if (eventStream) return;

    eventStream = new EventSource("/api/stream");

    eventStream.onmessage = msg => {
        const event = JSON.parse(msg.data);
        prependEvent(event);
    };

    eventStream.onerror = () => {
        eventStream.close();
        eventStream = null;

        reconnectTimer = setTimeout(() => {
            reconnectTimer = null;
            connectStream();
        }, 3000);
    };
}

async function clearAllEvents() {
    if (!confirm("Clear all stored events?")) {
        return;
    }

    const res = await clearEvents();

    if (!res.ok) {
        return;
    }

    events = [];
    pendingCount = 0;

    if (currentFilter === "noise") {
        await refreshNoise();
    } else {
        renderEvents();
    }

    updateStatus();
}