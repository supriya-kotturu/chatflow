let socket = null;

function getRoomIdFromPath() {
    const pathParts = window.location.pathname.split('/');
    return pathParts[pathParts.length - 1];
}

function initializeWebSocket() {
    const roomId = getRoomIdFromPath();
    socket = new WebSocket(`ws://${window.location.host}/ws/chat/${roomId}`);

    socket.onopen = function(event) {
        console.log(`WebSocket connection established for room: ${roomId}`);
    };
}

document.addEventListener("DOMContentLoaded", function() {
    initializeWebSocket();
});