import {
    getReports,
    generateReport
}
    from "../api.js";

import {
    reportRow
}
    from "../components/reportRow.js";

const reportList =
    document.getElementById("report-list");

const generateBtn =
    document.getElementById("generate-report-btn");

const reportError =
    document.getElementById("report-error");

let initialized = false;

export async function initializeReports() {

    await loadReports();

    if (!initialized) {

        initializeButton();

        initialized = true;

    }
}

async function loadReports() {

    const reports =
        await getReports();

    if (reports.length === 0) {

        reportList.innerHTML = `
            <div class="empty-state">
                no reports yet
                - click generate now,
                or wait for the scheduled run
            </div>
        `;

        return;
    }

    reportList.innerHTML =
        reports
            .map(reportRow)
            .join("");
}

function initializeButton() {

    generateBtn.addEventListener(
        "click",
        async () => {

            reportError.textContent = "";

            generateBtn.textContent =
                "GENERATING...";

            generateBtn.disabled = true;

            try {

                const res =
                    await generateReport();

                if (res.status === 204) {

                    reportError.textContent =
                        "No new events since last report.";

                } else if (!res.ok) {

                    reportError.textContent =
                        await res.text();

                } else {

                    await loadReports();

                }

            } finally {

                generateBtn.textContent =
                    "GENERATE NOW";

                generateBtn.disabled = false;

            }

        }
    );

}