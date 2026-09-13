import { initializeRouter } from "./router.js";
import { initializeWatch } from "./views/watch.js";
import { initializeLogin } from "./login.js";
import { logout } from "./api.js";
import { initializeStatusPolling } from "./status.js";

const logoutButton = document.getElementById("logout-btn");

logoutButton.addEventListener("click", async () => {
    try {
        await logout();
    } finally {
        location.reload();
    }
});

function initializeApp() {
    // Install login UI + global 401 handling first - this is synchronous
    // and cheap. If the session is actually invalid, the first API call
    // any of the code below makes will 401 and the global handler shows
    // the login overlay then. No need to block startup on a pre-check.
    initializeLogin();

    initializeRouter();
    initializeWatch();
    initializeStatusPolling();
}

initializeApp();