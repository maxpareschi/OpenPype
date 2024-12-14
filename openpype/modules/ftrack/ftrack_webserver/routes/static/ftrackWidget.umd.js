(function(global, factory) {
  typeof exports === "object" && typeof module !== "undefined" ? factory(exports) : typeof define === "function" && define.amd ? define(["exports"], factory) : (global = typeof globalThis !== "undefined" ? globalThis : global || self, factory(global.ftrackWidget = {}));
})(this, function(exports2) {
  "use strict";
  let targetOrigin;
  let credentials = {
    serverUrl: "",
    apiUser: "",
    apiKey: "",
    csrfToken: ""
  };
  let entity;
  let onWidgetLoadCallback, onWidgetUpdateCallback;
  function openSidebar(entityType, entityId) {
    console.debug("Opening sidebar", entityType, entityId);
    window.parent.postMessage(
      {
        topic: "ftrack.application.open-sidebar",
        data: {
          type: entityType,
          id: entityId
        }
      },
      targetOrigin || credentials.serverUrl
    );
  }
  function openActions(selection) {
    console.debug("Opening actions", selection);
    window.parent.postMessage(
      {
        topic: "ftrack.application.open-actions",
        data: {
          selection
        }
      },
      targetOrigin || credentials.serverUrl
    );
  }
  function closeWidget() {
    console.debug("Close widget");
    window.parent.postMessage(
      {
        topic: "ftrack.application.close-widget"
      },
      targetOrigin || credentials.serverUrl
    );
  }
  function openPreview(componentId) {
    console.debug("Open preview", componentId);
    window.parent.postMessage(
      {
        topic: "ftrack.application.open-preview",
        data: {
          componentId
        }
      },
      targetOrigin || credentials.serverUrl
    );
  }
  function navigate(entityType, entityId, module2) {
    module2 = module2 || "project";
    console.debug("Navigating", entityType, entityId);
    window.parent.postMessage(
      {
        topic: "ftrack.application.navigate",
        data: {
          type: entityType,
          id: entityId,
          module: module2
        }
      },
      targetOrigin || credentials.serverUrl
    );
  }
  function onWidgetLoad(content) {
    console.debug("Widget loaded", content);
    if (content.data.targetOrigin) {
      targetOrigin = content.data.targetOrigin;
    }
    credentials = content.data.credentials;
    entity = content.data.entity;
    if (onWidgetLoadCallback) {
      onWidgetLoadCallback(content);
    }
    window.dispatchEvent(
      new CustomEvent("ftrackWidgetLoad", {
        detail: {
          credentials,
          entity
        }
      })
    );
    if (entity) {
      window.dispatchEvent(
        new CustomEvent("ftrackWidgetUpdate", { detail: { entity } })
      );
    }
  }
  function onWidgetUpdate(content) {
    console.debug("Widget updated", content);
    entity = content.data.entity;
    if (onWidgetUpdateCallback) {
      onWidgetUpdateCallback(content);
    }
    window.dispatchEvent(
      new CustomEvent("ftrackWidgetUpdate", { detail: { entity } })
    );
  }
  function onPostMessageReceived(event) {
    const content = event.data || {};
    if (!content.topic) {
      return;
    }
    console.debug('Got "' + content.topic + '" event.', content);
    if (content.topic === "ftrack.widget.load") {
      onWidgetLoad(content);
    } else if (content.topic === "ftrack.widget.update") {
      onWidgetUpdate(content);
    }
  }
  function getEntity() {
    return entity;
  }
  function getCredentials() {
    return credentials;
  }
  function onDocumentClick() {
    window.parent.postMessage(
      {
        topic: "ftrack.application.document-clicked",
        data: {}
      },
      targetOrigin || credentials.serverUrl
    );
  }
  function onDocumentKeyDown(event) {
    const target = event.target;
    if (!target || !(target instanceof HTMLElement))
      return;
    const tagName = target.tagName.toLowerCase();
    if (["textarea", "input"].indexOf(tagName) !== -1 || target.isContentEditable) {
      return;
    }
    const fields = [
      "key",
      "code",
      "location",
      "ctrlKey",
      "shiftKey",
      "altKey",
      "metaKey",
      "repeat",
      "isComposing",
      "charCode",
      "keyCode",
      "which"
    ];
    const eventData = Object.fromEntries(
      fields.map((field) => [field, event[field]])
    );
    window.parent.postMessage(
      {
        topic: "ftrack.application.document-keydown",
        data: eventData
      },
      targetOrigin || credentials.serverUrl
    );
  }
  function onHashChange() {
    window.parent.postMessage(
      {
        topic: "ftrack.widget.hashchange",
        data: {
          hash: window.location.hash
        }
      },
      targetOrigin || credentials.serverUrl
    );
  }
  function getActiveTheme() {
    const parameters = new URLSearchParams(window.location.search);
    return parameters.get("theme");
  }
  function initialize(options = {}) {
    if (options.onWidgetLoad) {
      onWidgetLoadCallback = options.onWidgetLoad;
    }
    if (options.onWidgetUpdate) {
      onWidgetUpdateCallback = options.onWidgetUpdate;
    }
    window.addEventListener("message", onPostMessageReceived, false);
    window.parent.postMessage({ topic: "ftrack.widget.ready" }, "*");
    document.addEventListener("click", onDocumentClick);
    document.addEventListener("keydown", onDocumentKeyDown);
    window.addEventListener("hashchange", onHashChange);
  }
  exports2.closeWidget = closeWidget;
  exports2.getActiveTheme = getActiveTheme;
  exports2.getCredentials = getCredentials;
  exports2.getEntity = getEntity;
  exports2.initialize = initialize;
  exports2.navigate = navigate;
  exports2.openActions = openActions;
  exports2.openPreview = openPreview;
  exports2.openSidebar = openSidebar;
  Object.defineProperty(exports2, Symbol.toStringTag, { value: "Module" });
});
//# sourceMappingURL=ftrackWidget.umd.js.map
