let resolveActive = null;

const overlay = document.createElement("div");
overlay.className = "modal-overlay";
overlay.innerHTML = `
    <div class="modal-box">
        <div class="modal-message" id="modal-message"></div>
        <div class="modal-actions" id="modal-actions"></div>
    </div>
`;
document.body.appendChild(overlay);

const messageEl = overlay.querySelector("#modal-message");
const actionsEl = overlay.querySelector("#modal-actions");

function open(message, buttons) {
    messageEl.textContent = message;
    actionsEl.innerHTML = "";

    buttons.forEach(({ label, value, variant }) => {
        const btn = document.createElement("button");
        btn.textContent = label;
        btn.className = variant === "danger" ? "modal-btn modal-btn-danger" : "modal-btn";
        btn.addEventListener("click", () => close(value));
        actionsEl.appendChild(btn);
    });

    overlay.classList.add("active");
}

function close(value) {
    overlay.classList.remove("active");
    if (resolveActive) {
        resolveActive(value);
        resolveActive = null;
    }
}

// Drop-in replacement for window.confirm - returns a Promise<boolean>.
export function confirmDialog(message, { danger = false } = {}) {
    return new Promise(resolve => {
        resolveActive = resolve;
        open(message, [
            { label: "CANCEL", value: false },
            { label: danger ? "CONFIRM" : "OK", value: true, variant: danger ? "danger" : undefined },
        ]);
    });
}

// Drop-in replacement for window.alert - returns a Promise<void> that
// resolves once dismissed, in case a caller wants to wait for it.
export function alertDialog(message) {
    return new Promise(resolve => {
        resolveActive = () => resolve();
        open(message, [{ label: "OK", value: true }]);
    });
}