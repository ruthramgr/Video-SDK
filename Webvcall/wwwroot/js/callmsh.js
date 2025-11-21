// wwwroot/js/call.js
const connection = new signalR.HubConnectionBuilder()
    .withUrl("/callHub")
    .withAutomaticReconnect()
    .build();

let localStream = null;
let sessionIdGlobal = null;
let myConnectionId = null; // filled once available if hub exposes; if not, we may rely on server snapshot

// maps and helper data
const pcMap = {};                // remoteConnectionId -> RTCPeerConnection
const pendingCandidates = {};    // remoteConnectionId -> array of ICE candidates (queued until remoteDescription)
const remoteSlotMap = {};        // remoteConnectionId -> video element id
const remoteSlots = ["remoteVideo", "remoteVideo2"]; // extend if you want more slots

// UI elements
const startBtn = document.getElementById("startCall");
const endBtn = document.getElementById("endCall");
const sessionInput = document.getElementById("sessionIdInput");
const localVideo = document.getElementById("localVideo");

// simple UI state
function setUiState(joined) {
    startBtn.disabled = joined;
    endBtn.disabled = !joined;
    sessionInput.disabled = joined;
}

function getFreeRemoteSlotId() {
    // find first unused slot
    for (let s of remoteSlots) {
        if (!Object.values(remoteSlotMap).includes(s)) return s;
    }
    // if none free, return null (or dynamically create video)
    return null;
}

function attachRemoteTrackToSlot(remoteConnectionId, stream) {
    const slotId = remoteSlotMap[remoteConnectionId] || getFreeRemoteSlotId();
    if (!slotId) {
        console.warn("No free remote slot for", remoteConnectionId);
        return;
    }
    remoteSlotMap[remoteConnectionId] = slotId;
    const el = document.getElementById(slotId);
    if (el) el.srcObject = stream;
}

// create or return existing pc for a given remote connection id
async function createPeerFor(remoteId) {
    if (pcMap[remoteId]) return pcMap[remoteId];

    const pc = new RTCPeerConnection({
        iceServers: [{ urls: "stun:stun.l.google.com:19302" }]
    });

    // Add local tracks to this pc
    if (localStream) localStream.getTracks().forEach(t => pc.addTrack(t, localStream));

    // remote track handling
    pc.ontrack = (ev) => {
        console.log("ontrack from", remoteId, ev);
        if (ev.streams && ev.streams[0]) {
            attachRemoteTrackToSlot(remoteId, ev.streams[0]);
        } else {
            const ms = new MediaStream();
            if (ev.track) ms.addTrack(ev.track);
            if (ev.streams) ev.streams.forEach(s => s.getTracks().forEach(t => ms.addTrack(t)));
            attachRemoteTrackToSlot(remoteId, ms);
        }
    };

    pc.onicecandidate = (evt) => {
        if (evt.candidate) {
            // send candidate to the specific remote peer
            connection.invoke("SendIceCandidate", sessionIdGlobal, remoteId, JSON.stringify(evt.candidate))
                .catch(e => console.warn("SendIceCandidate error", e));
        }
    };

    pc.oniceconnectionstatechange = () => console.log("ICE state", remoteId, pc.iceConnectionState);
    pc.onconnectionstatechange = () => console.log("connectionState", remoteId, pc.connectionState);

    pcMap[remoteId] = pc;
    pendingCandidates[remoteId] = pendingCandidates[remoteId] || [];
    return pc;
}

async function flushPendingCandidatesFor(remoteId) {
    const pc = pcMap[remoteId];
    if (!pc) return;
    const q = pendingCandidates[remoteId] || [];
    while (q.length) {
        const c = q.shift();
        try {
            await pc.addIceCandidate(new RTCIceCandidate(c));
            console.log("Flushed candidate for", remoteId);
        } catch (e) {
            console.warn("flush candidate failed for", remoteId, e);
        }
    }
}

function closePeer(remoteId) {
    const pc = pcMap[remoteId];
    if (pc) {
        try { pc.close(); } catch (e) { }
        delete pcMap[remoteId];
    }
    delete pendingCandidates[remoteId];
    // free slot mapping
    if (remoteSlotMap[remoteId]) {
        const el = document.getElementById(remoteSlotMap[remoteId]);
        if (el) el.srcObject = null;
        delete remoteSlotMap[remoteId];
    }
}

// SIGNALR handlers

// Peer list update: snapshot array, and initiatorConnectionId (first in snapshot)
connection.on("PeerListUpdated", async (sid, snapshot, initiatorConnectionId) => {
    console.log("PeerListUpdated", sid, snapshot, "initiator:", initiatorConnectionId);
    // identify other peers
    const others = snapshot.filter(id => id !== connection.connectionId); // connection.connectionId is not set by signalr client - keep defensive
    // fallback: the server sent participants in JoinResult too; we will use snapshot to compute peers
    // If we're the initiator (server told us) & we are the initiatorConnectionId, create offers to all others.
    const iAmInitiator = (initiatorConnectionId === connection.connectionId);
    // But the above may not work in all clients because signalR client doesn't expose our connectionId easily; rely on JoinSession response is better.
    // We'll just log snapshot; the JoinSession call sets isInitiator earlier.
});

// ReceiveOffer from a remote peer
connection.on("ReceiveOffer", async (callerId, sdpJson) => {
    console.log("ReceiveOffer from", callerId);
    if (!localStream) {
        try {
            localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
            localVideo.srcObject = localStream;
        } catch (err) {
            console.error("getUserMedia failed", err);
            return;
        }
    }

    const pc = await createPeerFor(callerId);

    // Ensure transceivers exist for better interop
    try {
        pc.addTransceiver('video', { direction: 'sendrecv' });
        pc.addTransceiver('audio', { direction: 'sendrecv' });
    } catch (e) { }

    const sdp = JSON.parse(sdpJson);
    await pc.setRemoteDescription(new RTCSessionDescription(sdp));
    await flushPendingCandidatesFor(callerId);

    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);

    await connection.invoke("SendAnswer", sessionIdGlobal, callerId, JSON.stringify(answer));
});

// ReceiveAnswer from a remote peer (in response to an offer we initiated)
connection.on("ReceiveAnswer", async (calleeId, sdpJson) => {
    console.log("ReceiveAnswer from", calleeId);
    const pc = pcMap[calleeId];
    if (!pc) {
        console.warn("No pc for ReceiveAnswer from", calleeId);
        return;
    }
    const sdp = JSON.parse(sdpJson);
    await pc.setRemoteDescription(new RTCSessionDescription(sdp));
    await flushPendingCandidatesFor(calleeId);
});

// ICE candidate from a remote
connection.on("ReceiveIceCandidate", async (senderId, candidateJson) => {
    const c = JSON.parse(candidateJson);
    const pc = pcMap[senderId];
    if (!pc || !pc.remoteDescription || pc.remoteDescription.type === null) {
        pendingCandidates[senderId] = pendingCandidates[senderId] || [];
        pendingCandidates[senderId].push(c);
        console.log("Queued ICE candidate for", senderId);
    } else {
        try {
            await pc.addIceCandidate(new RTCIceCandidate(c));
            console.log("Added ICE candidate from", senderId);
        } catch (e) {
            console.warn("addIceCandidate failed for", senderId, e);
        }
    }
});

connection.on("PeerJoined", async (sid, initiatorConnectionId) => {
    console.log("PeerJoined event (legacy)", sid, initiatorConnectionId);
    // We rely on PeerListUpdated to decide offers. Keep legacy handler no-op.
});

connection.on("PeerLeft", (sid, departedConnectionId) => {
    console.log("PeerLeft", sid, departedConnectionId);
    closePeer(departedConnectionId);
});

// Start: join session, get JoinResult (includes participants and isInitiator)
async function start(sessionId) {
    sessionIdGlobal = sessionId;
    try {
        await connection.start();
        console.log("SignalR started");

        const joinResult = await connection.invoke("JoinSession", sessionId);
        const joined = joinResult && (joinResult.joined === true || joinResult.Joined === true);
        const isInitiator = joinResult && (joinResult.isInitiator === true || joinResult.IsInitiator === true);
        const participants = joinResult && (joinResult.participants || joinResult.Participants) || [];

        if (!joined) {
            alert("Failed to join session (possibly full).");
            await connection.stop();
            return;
        }

        // get local media
        try {
            localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
            localVideo.srcObject = localStream;
        } catch (err) {
            alert("Camera/microphone access denied or unavailable: " + err.message);
            return;
        }

        // If initiator, create offers to other participants in participants list
        if (isInitiator) {
            console.log("I am initiator — creating offers to existing participants (if any).");
            // other participants = participants except ourselves. Depending on server, our connection id may be present in list.
            for (const p of participants) {
                if (!p) continue;
                // skip ourselves (best-effort: server included our id; if not sure, harmless to attempt)
                if (p === connection.connectionId) continue;
                // create pc and offer
                const pc = await createPeerFor(p);
                // create transceivers to ensure sendrecv
                try { pc.addTransceiver('video', { direction: 'sendrecv' }); pc.addTransceiver('audio', { direction: 'sendrecv' }); } catch (e) { }
                const offer = await pc.createOffer();
                await pc.setLocalDescription(offer);
                await connection.invoke("SendOffer", sessionIdGlobal, p, JSON.stringify(offer));
            }
        }

        // also set UI state
        setUiState(true);
    } catch (err) {
        console.error("Start error:", err);
        alert("Start failed: " + err.toString());
    }
}

async function end(sessionId) {
    try { if (sessionIdGlobal) await connection.invoke("LeaveSession", sessionIdGlobal); } catch (e) { console.warn(e); }

    // cleanup
    Object.keys(pcMap).forEach(k => closePeer(k));
    try { if (localStream) { localStream.getTracks().forEach(t => t.stop()); localStream = null; } } catch (e) { }
    localVideo.srcObject = null;
    sessionIdGlobal = null;
    setUiState(false);
    try { await connection.stop(); } catch (e) { }
}

// UI wiring
startBtn.onclick = async () => {
    const s = (sessionInput.value || "").trim();
    if (!s) return alert("Enter session id");
    await start(s);
};

endBtn.onclick = async () => {
    const s = sessionInput.value || sessionIdGlobal;
    if (!s) { await end(null); return; }
    await end(s);
};

// init UI
setUiState(false);
if (!sessionInput.value) sessionInput.value = "room-" + Math.floor(Math.random() * 1000);
