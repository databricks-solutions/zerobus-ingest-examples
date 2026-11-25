// Node-RED settings for this example. Node-RED loads this file from its user directory (/data in the container).
// Function nodes can read environment variables (client secret included), so the editor must stay private.
// Inside the container the editor listens on all container interfaces, so Docker's published port can reach it.
// run-docker.sh publishes that port on the host's loopback only (-p 127.0.0.1:PORT:PORT), so the editor is
// not reachable from other machines.
// Do not run the container with --network host: -p is ignored there, and 0.0.0.0 would then expose the editor on
// every host interface. With host networking, set UI_HOST=127.0.0.1 so the editor binds to loopback.
module.exports = {
    uiHost: process.env.UI_HOST || "0.0.0.0",
    uiPort: process.env.PORT || 48137,
    // The MQTT client reconnects on its own after a disconnect, with the options (and token) of the original
    // connect. Set this very large so the client never reconnects by itself: the flow's watch trigger reconnects
    // instead, and fetches a new token first. (0 is not a way to disable it: Node-RED falls back to 5000 ms.)
    mqttReconnectTime: 2147483647,
    flowFile: "flows.json",
    editorTheme: { projects: { enabled: false } },
    logging: { console: { level: "info", metrics: false, audit: false } }
};
