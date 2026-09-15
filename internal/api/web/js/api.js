export async function getEvents() {
    return (await fetch("/api/events")).json();
}

export async function getEventNoise() {
    return (await fetch("/api/events/noise")).json();
}

export async function getSourceTypes() {
    return (await fetch("/api/source-types")).json();
}

export async function getSources() {
    return (await fetch("/api/sources")).json();
}

export async function addSource(body) {

    return fetch("/api/sources", {
        method: "POST",

        headers: {
            "Content-Type": "application/json"
        },

        body: JSON.stringify(body)
    });
}

export async function removeSource(id) {

    return fetch(`/api/sources/${id}`, {
        method: "DELETE"
    });
}

export async function getRules() {
    return (await fetch("/api/rules")).json();
}

export async function addRule(body) {

    return fetch("/api/rules", {
        method: "POST",

        headers: {
            "Content-Type": "application/json"
        },

        body: JSON.stringify(body)
    });
}

export async function removeRule(id) {

    return fetch(`/api/rules/${id}`, {
        method: "DELETE"
    });
}

export async function getReports() {
    return (await fetch("/api/reports")).json();
}

export async function generateReport() {

    return fetch("/api/reports/generate", {
        method: "POST"
    });
}

export async function login(username, password) {
    return fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password })
    });
}

export async function logout() {
    return fetch("/api/auth/logout", { method: "POST" });
}

export async function getActivity(since) {
    return (await fetch(`/api/activity?since=${since || "24h"}`)).json();
}

export async function getSystemStatus() {
    const res = await fetch("/api/system/status");

    if (!res.ok) {
        return null;
    }

    return res.json();
}

export async function getContainers() {
    return (await fetch("/api/system/containers")).json();
}

export async function changePassword(currentPassword, newPassword) {
    return fetch("/api/system/password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ current_password: currentPassword, new_password: newPassword })
    });
}

export async function containerAction(name, action) {
    return fetch(`/api/system/containers/${encodeURIComponent(name)}/${action}`, {
        method: "POST"
    });
}

export async function getSettings() {
    const res = await fetch("/api/system/settings");
    if (!res.ok) return null;
    return res.json();
}

export async function updateSettings(sessionTimeoutSeconds) {
    return fetch("/api/system/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ session_timeout_seconds: sessionTimeoutSeconds })
    });
}

export async function clearEvents() {
    return fetch("/api/events", {
        method: "DELETE",
    });
}