import { getEvents, getEventNoise } from "../api.js";
import { eventRow } from "../components/eventRow.js";
import { enableExpandableRows } from "../utils.js";

let events = [];
let noise = [];
let currentFilter = "all";
let expandableEnabled = false;
let paused = false;
let pendingCount = 0;

const eventList = document.getElementById("event-list");
const pauseBtn = document.getElementById("watch-pause-btn");

const statusDot = document.getElementById("status-dot");
const statusText = document.getElementById("status-text");
const bifrost = document.getElementById("bifrost");

export async function initializeWatch() {

    events = await getEvents();

    initializeFilters();

    if (!expandableEnabled) {
        enableExpandableRows(eventList);
        pauseBtn.addEventListener("click", togglePause);
        expandableEnabled = true;
    }

    renderCurrentFilter();

    updateStatus();

    connectStream();
}

function togglePause() {
    paused = !paused;
    pauseBtn.textContent = paused ? `RESUME (${pendingCount})` : "PAUSE";
    pauseBtn.classList.toggle("active", paused);

    if (!paused) {
        pendingCount = 0;
        renderCurrentFilter();
        updateStatus();
    }
}

function initializeFilters() {

    // Scoped to #view-watch so this never touches the Activity tab's
    // 1H/24H/48H buttons or the pause button - they share the same
    // .filter-btn class but aren't part of this view's severity filter.
    document
        .querySelectorAll("#view-watch .filter-btn[data-severity]")
        .forEach(btn => {

            btn.addEventListener("click", () => {

                document
                    .querySelectorAll("#view-watch .filter-btn[data-severity]")
                    .forEach(b => b.classList.remove("active"));

                btn.classList.add("active");

                currentFilter =
                    btn.dataset.severity;

                renderCurrentFilter();

            });

        });

}

function renderCurrentFilter() {
    if (currentFilter === "noise") {
        loadNoise();
        return;
    }
    renderEvents();
}

async function loadNoise() {
    noise = await getEventNoise();

    if (!noise || noise.length === 0) {
        eventList.innerHTML = `<div class="empty-state">no noise recorded</div>`;
        return;
    }

    eventList.innerHTML = noise
        .map(n => `
            <div class="event-row noise">
                <span class="noise-count">${n.count}×</span>
                <span class="event-source">${n.source}</span>
                <span class="event-type">${n.type}</span>
                <span class="event-message">${n.message}</span>
                <span class="noise-last">last seen: ${new Date(n.last_seen).toLocaleTimeString()}</span>
            </div>
        `)
        .join("");
}

function renderEvents() {

    const filtered =
        currentFilter === "all"
            ? events
            : events.filter(
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

    eventList.innerHTML =
        filtered
            .map(eventRow)
            .join("");
}

function updateStatus() {

    const hasCritical =
        events.some(
            e => e.Severity === "critical"
        );

    const hasWarning =
        events.some(
            e => e.Severity === "warning"
        );

    const level =
        hasCritical
            ? "critical"
            : hasWarning
                ? "warning"
                : "info";

    const label =
        hasCritical
            ? "critical events active"
            : hasWarning
                ? "warnings present"
                : "nominal";

    statusDot.className =
        `status-dot ${level}`;

    statusText.textContent =
        label;

    bifrost.className =
        `bifrost ${level}`;
}

function prependEvent(e) {

    // Ignore-severity events are aggregated server-side into event_noise
    // and never land in /api/events, but the live SSE bus may still carry
    // them the instant they're classified. Drop them here too, otherwise
    // a single noisy source re-renders the whole list every second and
    // wipes any row you've expanded to read, even with pause off.
    if (e.Severity === "ignore") {
        return;
    }

    events.unshift(e);

    if (events.length > 200) {
        events.pop();
    }

    if (paused) {
        pendingCount++;
        pauseBtn.textContent = `RESUME (${pendingCount})`;
        return;
    }

    if (currentFilter !== "noise") {
        renderEvents();
    }

    updateStatus();
}

function connectStream() {

    const es =
        new EventSource("/api/stream");

    es.onmessage = msg => {

        const event =
            JSON.parse(msg.data);

        prependEvent(event);
    };

    es.onerror = () => {

        es.close();

        setTimeout(
            connectStream,
            3000
        );

    };
}