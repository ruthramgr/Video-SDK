// wwwroot/js/call.js
const connection = new signalR.HubConnectionBuilder()
    .withUrl("/callHub")
    .withAutomaticReconnect()
    .build();

let pc = null;
let localStream = null;
let pendingCandidates = [];
let isInitiator = false;
let sessionIdGlobal = null;

// UI elements 
const usernameInput = document.getElementById("usernameInput");
const startBtn = document.getElementById("startCall");
const endBtn = document.getElementById("endCall");
const sessionInputPst = document.getElementById("sessionIdInputPs");
const sessionInput = document.getElementById("sessionIdInput");
const localVideo = document.getElementById("localVideo");
const remoteVideo = document.getElementById("remoteVideo");


function setUiState(joined) {
    startBtn.disabled = joined;
    endBtn.disabled = !joined;
    sessionInput.disabled = joined;
}

async function createPeer() {
    pc = new RTCPeerConnection({
        iceServers: [
            { urls: "stun:stun.l.google.com:19302" }
            // add TURN servers for production here
        ]
    });

    if (localStream) localStream.getTracks().forEach(t => pc.addTrack(t, localStream));

    pc.ontrack = (ev) => {
        console.log("ontrack", ev);
        if (ev.streams && ev.streams[0]) {
            remoteVideo.srcObject = ev.streams[0];
        } else {
            const ms = new MediaStream();
            if (ev.track) ms.addTrack(ev.track);
            if (ev.streams) ev.streams.forEach(s => s.getTracks().forEach(t => ms.addTrack(t)));
            remoteVideo.srcObject = ms;
        }
    };

    pc.onicecandidate = (evt) => {
        if (evt.candidate) {
            connection.invoke("SendIceCandidate", sessionIdGlobal, JSON.stringify(evt.candidate));
        }
    };

    pc.oniceconnectionstatechange = () => console.log("ICE state:", pc.iceConnectionState);
    pc.onconnectionstatechange = () => console.log("connectionState:", pc.connectionState);
}

async function flushPendingCandidates() {
    while (pendingCandidates.length) {
        const c = pendingCandidates.shift();
        try {
            await pc.addIceCandidate(new RTCIceCandidate(c));
            console.log("Flushed candidate");
        } catch (e) {
            console.warn("flush addIceCandidate failed", e);
        }
    }
}

function closeAndCleanup() {
    try { if (pc) { pc.close(); pc = null; } } catch (e) { }
    try { if (localStream) { localStream.getTracks().forEach(t => t.stop()); localStream = null; } } catch (e) { }
    localVideo.srcObject = null;
    remoteVideo.srcObject = null;
    pendingCandidates = [];
    isInitiator = false;
    sessionIdGlobal = null;
    setUiState(false);
}

// register handlers BEFORE starting SignalR connection
//connection.on("ReceiveOffer", async (callerId, sdpJson) => {
//    console.log("ReceiveOffer from", callerId);
//    if (!pc) await createPeer();
//    const sdp = JSON.parse(sdpJson);
//    await pc.setRemoteDescription(new RTCSessionDescription(sdp));
//    await flushPendingCandidates();
//    const answer = await pc.createAnswer();
//    await pc.setLocalDescription(answer);
//    await connection.invoke("SendAnswer", sessionIdGlobal, JSON.stringify(answer));
//});

connection.on("ReceiveOffer", async (callerId, sdpJson) => {
    console.log("ReceiveOffer from", callerId);
 
    // Ensure PC exists
    if (!pc) await createPeer();

    // Ensure we have local media and that tracks are added to the pc BEFORE creating answer
    if (!localStream) {
        try {
            console.log("No localStream yet — requesting getUserMedia() before answering");
            localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
            localVideo.srcObject = localStream;
            // add tracks to pc
            localStream.getTracks().forEach(t => pc.addTrack(t, localStream));
        } catch (err) {
            console.error("getUserMedia failed in ReceiveOffer:", err);
            return; // cannot answer without media (or you may still answer with recvonly)
        }
    } else {
        // ensure tracks are added (defensive: sometimes pc created before localStream was added)
        const senders = pc.getSenders ? pc.getSenders().map(s => s.track).filter(Boolean) : [];
        if (senders.length === 0) {
            console.log("Adding local tracks to pc (no senders found)");
            localStream.getTracks().forEach(t => pc.addTrack(t, localStream));
        }
    }

    // (Optional) ensure transceivers exist - helps some browsers negotiate direction
    try {
        pc.addTransceiver('video', { direction: 'sendrecv' });
        pc.addTransceiver('audio', { direction: 'sendrecv' });
    } catch (e) { /* ignore if not supported */ }

    // Now set remote desc, flush ICE, create answer
    const sdp = JSON.parse(sdpJson);
    await pc.setRemoteDescription(new RTCSessionDescription(sdp));
    await flushPendingCandidates();

    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    console.log("Sending answer SDP", answer);
    await connection.invoke("SendAnswer", sessionIdGlobal, JSON.stringify(answer));
});


connection.on("ReceiveAnswer", async (calleeId, sdpJson) => {
    console.log("ReceiveAnswer from", calleeId);
   
    if (!pc) return console.warn("No PC available for ReceiveAnswer");
    const sdp = JSON.parse(sdpJson);
    await pc.setRemoteDescription(new RTCSessionDescription(sdp));
    await flushPendingCandidates();
});

connection.on("ReceiveIceCandidate", async (senderId, candidateJson) => {
    const c = JSON.parse(candidateJson);
    if (!pc || !pc.remoteDescription || pc.remoteDescription.type === null) {
        pendingCandidates.push(c);
        const peerNameEl = document.getElementById("peerName");
        if (peerNameEl) peerNameEl.innerText = callerName || "Anonymous";
        console.log("Queued ICE candidate (waiting for remoteDescription)");
    } else {
        try {
            await pc.addIceCandidate(new RTCIceCandidate(c));
            console.log("Added ICE candidate");
        } catch (e) {
            console.warn("addIceCandidate failed:", e);
        }
    }
});

connection.on("PeerJoined", async (sid, initiatorConnectionId,username) => {
    console.log("PeerJoined", sid, "initiator:", initiatorConnectionId, username);
    // show the initiator name (or fallback)
    const peerNameEl = document.getElementById("peerName");
    if (peerNameEl) {
        peerNameEl.innerText = username && username.trim().length > 0 ? username : "Anonymous";
    }
    if (isInitiator) {
        if (!pc) await createPeer();
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        await connection.invoke("SendOffer", sessionIdGlobal, JSON.stringify(offer));
    } else {
        console.log("Waiting for initiator to create offer...");
        if (username) {
            console.log("Initiator / user name:", username);
        // optionally display it in UI
    }
    }
});

connection.on("PeerLeft", (sid, departedConnectionId) => {
    console.log("PeerLeft", sid, departedConnectionId);    
    const peerNameEl = document.getElementById("peerName");
    if (peerNameEl) peerNameEl.innerText = "No peer";
    closeAndCleanup();
});

async function start(sessionId) {
    try {
        sessionIdGlobal = sessionId;
        await connection.start();
        console.log("SignalR started");

        const username = (usernameInput?.value || "").trim();
        if (!username) {
            if (!confirm("You didn't enter a name. Continue as anonymous?")) {
                await connection.stop();
                return;
            }
        }

        // JoinSession returns { Joined, IsInitiator } (be defensive about casing)
        const joinResult = await connection.invoke("JoinSession", sessionId, username);
        const joined = joinResult && (joinResult.joined === true || joinResult.Joined === true);
        const initiator = joinResult && (joinResult.isInitiator === true || joinResult.IsInitiator === true);
        if (!joined) {
            alert("Failed to join session (possibly full).");
            await connection.stop();
            return;
        }

        isInitiator = !!initiator;
        console.log("Joined session. isInitiator=", isInitiator);
        try {
            localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
            localVideo.srcObject = localStream;
        } catch (err) {
            alert("Camera/microphone access denied or unavailable: " + err.message);
            return;
        }

        await createPeer();
        setUiState(true);
    } catch (err) {
        console.error("Start error:", err);
        alert("Start failed: " + err.toString());
    }
} 
async function end(sessionId) {
    try { if (sessionIdGlobal) await connection.invoke("LeaveSession", sessionIdGlobal); } catch (e) { console.warn(e); }
    closeAndCleanup();
    try { await connection.stop(); } catch (e) { }
}

// UI wiring
startBtn.onclick = async () => {
    //const s = (sessionInput.value || "").trim();
    const s = sessionInputPst.value.trim();
    if (!s) return alert("Enter session id");
    await start(s);
};

endBtn.onclick = async () => {
    const s = sessionInput.value || sessionIdGlobal;
    if (!s) { closeAndCleanup(); return; }
    await end(s);
};

// Copy Room ID text
const copyRoomBtn = document.getElementById("copyRoomBtn");
copyRoomBtn.onclick = async () => {
    const roomId = sessionIdInput.value;

    if (!roomId) {
        alert("Room ID not found.");
        return;
    }

    try {
        await navigator.clipboard.writeText(roomId);
        //alert("Room ID copied!");
    } catch (e) {
        // Fallback for HTTPS/older browsers
        sessionInput.select();
        document.execCommand("copy");
        //alert("Room ID copied!");
    }
};
sessionIdInputPs.addEventListener("keydown", (e) => e.preventDefault());
sessionIdInputPs.addEventListener("paste", (e) => e.preventDefault());
sessionIdInputPs.addEventListener("drop", (e) => e.preventDefault());
sessionIdInputPs.readOnly = true;
// Paste Room ID text
const pasteRoomBtn = document.getElementById("PasteRoomBtnJoin");
pasteRoomBtn.onclick = async () => {   
   
    try {
        const text = await navigator.clipboard.readText();
        if (!text) {
            alert("Clipboard is empty.");
            return;
        }

        // allow programmatic paste
        sessionIdInputPs.value = text;
        //alert("Pasted successfully!");
    } catch (e) {
        alert("Unable to read clipboard. Browser blocked it.");
        console.error(e);
    }
};

// init UI
setUiState(false);
if (!sessionInput.value) sessionInput.value = "room-" + Math.floor(Math.random() * 1000);

// old now
//const connection = new signalR.HubConnectionBuilder()
//    .withUrl("/callHub")
//    .withAutomaticReconnect()
//    .build();

//let peerConnection = null;
//let localStream = null;
//let isCaller = false;

//async function start(sessionId) {
//    await connection.start();
//    const joined = await connection.invoke("JoinSession", sessionId);

//    if (!joined) {
//        alert("Session is full. Please try another session.");
//        return;
//    }

//    // get media etc
//    localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
//    document.getElementById("localVideo").srcObject = localStream;

//    peerConnection = new RTCPeerConnection({ iceServers: [{ urls: "stun:stun.l.google.com:19302" }] });
//    localStream.getTracks().forEach(track => peerConnection.addTrack(track, localStream));

//    peerConnection.ontrack = event => {
//        document.getElementById("remoteVideo").srcObject = event.streams[0];
//    };
//    peerConnection.onicecandidate = event => {
//        if (event.candidate) {
//            connection.invoke("SendIceCandidate", sessionId, JSON.stringify(event.candidate));
//        }
//    };

//    connection.on("ReceiveOffer", async (callerId, sdp) => {
//        isCaller = false;
//        await peerConnection.setRemoteDescription(new RTCSessionDescription(JSON.parse(sdp)));
//        const answer = await peerConnection.createAnswer();
//        await peerConnection.setLocalDescription(answer);
//        await connection.invoke("SendAnswer", sessionId, JSON.stringify(answer));
//    });

//    connection.on("ReceiveAnswer", async (calleeId, sdp) => {
//        await peerConnection.setRemoteDescription(new RTCSessionDescription(JSON.parse(sdp)));
//    });

//    connection.on("ReceiveIceCandidate", async (senderId, candidate) => {
//        await peerConnection.addIceCandidate(new RTCIceCandidate(JSON.parse(candidate)));
//    });

//    // When both participants joined
//    connection.on("PeerJoined", async (sid) => {
//        if (isCaller === false) {
//            // since you joined first you become caller
//            isCaller = true;
//        }
//        if (isCaller) {
//            const offer = await peerConnection.createOffer();
//            await peerConnection.setLocalDescription(offer);
//            await connection.invoke("SendOffer", sessionId, JSON.stringify(offer));
//        }
//    });
//}

//function end(sessionId) {
//    connection.invoke("LeaveSession", sessionId);
//    peerConnection.close();
//    localStream.getTracks().forEach(t => t.stop());
//    document.getElementById("localVideo").srcObject = null;
//    document.getElementById("remoteVideo").srcObject = null;
//}

//// Trigger UI: e.g. on button click:
//document.getElementById("startCall").onclick = () => {
//    const sessionId = document.getElementById("sessionIdInput").value;
//    start(sessionId);
//};
//document.getElementById("endCall").onclick = () => {
//    const sessionId = document.getElementById("sessionIdInput").value;
//    end(sessionId);
//}

