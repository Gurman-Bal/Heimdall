import {
    getSources,
    getSourceTypes,
    addSource,
    removeSource
}
    from "../api.js";

import {
    sourceRow
}
    from "../components/sourceRow.js";

const sourceList =
    document.getElementById("source-list");

const sourceForm =
    document.getElementById("source-form");

const sourceError =
    document.getElementById("source-error");

const sourceType =
    document.getElementById("source-type");

const sourcePath =
    document.getElementById("source-path");

let initialized = false;

export async function initializeSources() {

    await loadSourceTypes();

    await loadSources();

    if (!initialized) {

        initializeForm();

        initialized = true;

    }
}

async function loadSourceTypes() {

    const types =
        await getSourceTypes();

    sourceType.innerHTML =
        types
            .map(
                t =>
                    `<option value="${t}">
                        ${t}
                    </option>`
            )
            .join("");
}

async function loadSources() {

    const sources =
        await getSources();

    if (sources.length === 0) {

        sourceList.innerHTML = `
            <div class="empty-state">
                no sources configured
                - add one below
            </div>
        `;

        return;
    }

    sourceList.innerHTML =
        sources
            .map(sourceRow)
            .join("");

    sourceList
        .querySelectorAll(".remove-btn")
        .forEach(btn => {

            btn.addEventListener(
                "click",
                async () => {

                    await removeSource(
                        btn.dataset.id
                    );

                    loadSources();

                }
            );

        });
}

function initializeForm() {

    sourceForm.addEventListener(
        "submit",
        async e => {

            e.preventDefault();

            sourceError.textContent = "";

            const body = {

                type:
                sourceType.value,

                path:
                    sourcePath
                        .value
                        .trim()

            };

            if (!body.path) {
                return;
            }

            const res =
                await addSource(body);

            if (!res.ok) {

                sourceError.textContent =
                    await res.text();

                return;
            }

            sourcePath.value = "";

            loadSources();

        }
    );

}