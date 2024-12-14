const mainContainer = document.getElementById("mainContainer");
const progressBar = document.getElementById("progressBar")
const dropZone = document.getElementById("dropZone");
const fileInput = document.getElementById("fileInput");
const fileList = document.getElementById("fileList");
const cancelButton = document.getElementById("cancelButton");
const submitButton = document.getElementById("submitButton");
const queryString = window.location.search;
let formData = new FormData();
let files = [];

function displayFileList() {
    const ul = document.createElement("ul");
    files.forEach((file) => {
        const li = document.createElement("li");
        li.textContent = file.name;
        ul.appendChild(li);
    });
    fileList.appendChild(ul);
};

function clearFileInput() {
    try {
        fileInput.value = null;
        formData = new FormData()
    } catch(ex) { }
    if (fileInput.value) {
        fileInput.parentNode.replaceChild(fileInput.cloneNode(true), fileInput);
    }
}

dropZone.addEventListener("dragover", (event) => {
    event.preventDefault();
    dropZone.classList.add("dragover");
});

dropZone.addEventListener("dragleave", () => {
    dropZone.classList.remove("dragover");
});

dropZone.addEventListener("drop", (event) => {
    event.preventDefault();
    dropZone.classList.remove("dragover");
    files = Array.from(event.dataTransfer.files);
    displayFileList();
});

dropZone.addEventListener("click", () => {
    fileInput.click();
});

fileInput.addEventListener("change", (event) => {
    files = Array.from(event.target.files);
    displayFileList(fileList, files);
});

cancelButton.addEventListener("click", async () => {
    formData = new FormData();
    clearFileInput()
    fileList.innerHTML = ""
});

submitButton.addEventListener("click", async () => {
    if (files.length === 0) {
        alert("No files selected!");
        return;
    }

    progressBar.classList.remove("hidden");
    mainContainer.classList.add("hidden");

    files.forEach((file, index) => {
        formData.append(`file${index}`, file);
    });

    try {
        const response = await fetch("/upload_csv" + queryString, {
            method: "POST",
            body: formData
        });

        if (response.ok) {
            console.log("Files upload successfully!");
            /*ftrackWidget.closeWidget()*/
            clearFileInput()
            progressBar.classList.add("hidden");
            mainContainer.classList.remove("hidden");
            mainContainer.innerHTML = "<h4>You can now close this panel</h4>"
        } else {
            alert("Failed to upload files.");
            clearFileInput()
            fileList.innerHTML = ""
            progressBar.classList.add("hidden");
            mainContainer.classList.remove("hidden");
        }
    } catch (error) {
        console.error("Error uploading files:", error);
        alert("An error occurred while uploading files.");
        clearFileInput()
        fileList.innerHTML = ""
        progressBar.classList.add("hidden");
        mainContainer.classList.remove("hidden");
    }
});

window.addEventListener("message", (event) => {
    var content = event.data || {};
    console.log('Got "' + content.topic + '" event.', content);
});

/*
function widgetLoad(event) {
    console.debug("DOM Widget ready!");
};

function widgetUpdate(event) {
    console.debug("Shit was updated...");
}

window.addEventListener("DOMContentLoaded", function () {
    ftrackWidget.initialize({
        onWidgetLoad: widgetLoad,
        onWidgetUpdate: widgetUpdate,
    });
});*/