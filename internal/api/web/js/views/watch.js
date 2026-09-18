import { getEvents, getEventNoise, clearEvents } from "../api.js";
import { eventRow } from "../components/eventRow.js";
import { enableExpandableRows } from "../utils.js";

let events = [];
let noise = [];
let pendingEvents = [];
let currentFilter = "all";
let paused = false;
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

    // Load existing events, but never keep IGNORE events
    events = (await getEvents()).filter(
        e => e.Severity !== "ignore"
    );

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

    if (paused) {
        pauseBtn.textContent = "RESUME (0)";
        pauseBtn.classList.add("active");
        return;
    }

    // Resume
    const bufferedEvents = pendingEvents;
    pendingEvents = [];

    pauseBtn.textContent = "PAUSE";
    pauseBtn.classList.remove("active");

    // Add buffered events to the normal event list
    for (const event of bufferedEvents) {
        addEventToHistory(event);
    }

    renderEvents();
    updateStatus();
}

function renderEvents() {
    if (currentFilter === "noise") return;

    // Defensive filtering:
    // IGNORE events should never be rendered.
    const visibleEvents = events.filter(
        e => e.Severity !== "ignore"
    );

    const filtered =
        currentFilter === "all"
            ? visibleEvents
            : visibleEvents.filter(
                e => e.Severity === currentFilter
            );

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

    eventList.innerHTML = filtered
        .map(eventRow)
        .join("");
}

function appendEvent(e) {
    // IGNORE events should never reach the UI.
    if (e.Severity === "ignore") return;

    if (currentFilter === "noise") return;

    const matchesFilter =
        currentFilter === "all" ||
        e.Severity === currentFilter;

    if (!matchesFilter) return;

    const empty = eventList.querySelector(".empty-state");
    if (empty) {
        empty.remove();
    }

    const wrapper = document.createElement("div");
    wrapper.innerHTML = eventRow(e);

    const row = wrapper.firstElementChild;
    eventList.prepend(row);
}

function addEventToHistory(e) {
    // Never store IGNORE events.
    if (e.Severity === "ignore") return;

    events.unshift(e);

    // Keep only the newest 200 events.
    if (events.length > 200) {
        events.pop();
    }
}

function prependEvent(e) {
    // IGNORE events do absolutely nothing.
    if (e.Severity === "ignore") return;

    // If paused, buffer the event instead of displaying it.
    if (paused) {
        pendingEvents.push(e);

        pauseBtn.textContent =
            `RESUME (${pendingEvents.length})`;

        return;
    }

    // Store the event.
    addEventToHistory(e);

    // Display it if it matches the current filter.
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

    eventList.innerHTML = noise
        .map(n => `
            <div class="event-row noise">
                <span class="noise-count">${n.count}×</span>
                <span class="event-source">${n.source}</span>
                <span class="event-type">${n.type}</span>
                <span class="event-message">${n.message}</span>
                <span class="noise-last">
                    last seen: ${new Date(
            n.last_seen
        ).toLocaleTimeString()}
                </span>
            </div>
        `)
        .join("");
}

function updateStatus() {
    // events should already contain no IGNORE events,
    // but keep this defensive anyway.
    const visibleEvents = events.filter(
        e => e.Severity !== "ignore"
    );

    const hasCritical = visibleEvents.some(
        e => e.Severity === "critical"
    );

    const hasWarning = visibleEvents.some(
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
        try {
            const event = JSON.parse(msg.data);
            prependEvent(event);
        } catch (err) {
            console.error(
                "Failed to parse event stream message:",
                err
            );
        }
    };

    eventStream.onerror = () => {
        eventStream.close();
        eventStream = null;

        if (reconnectTimer) return;

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
    pendingEvents = [];

    if (currentFilter === "noise") {
        await refreshNoise();
    } else {
        renderEvents();
    }

    updateStatus();

    if (paused) {
        pauseBtn.textContent = "RESUME (0)";
    }
}