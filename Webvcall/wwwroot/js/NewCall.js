// Mesh client: one PC per remote peer
const connection = new signalR.HubConnectionBuilder()
    .withUrl("/callHub")
    .withAutomaticReconnect()
    .build();

const pcs = {}; // { [remoteId]: RTCPeerConnection }
let localStream = null;
let sessionIdGlobal = null;
const localVideo = document.getElementById('localVideo');
const remoteVideoContainer = document.getElementById('remoteContainer') || null; // optional
const startBtn = document.getElementById('startCall');
const sessionInput = document.getElementById("sessionIdInput");

// UI wiring
startBtn.onclick = async () => {
    const s = (sessionInput.value || "").trim();
    if (!s) return alert("Enter session id");
    await startCall(s);
};
async function ensureLocalStream() {
    if (!localStream) {
        localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
        localVideo.srcObject = localStream;
    }
}

// create PC for a specific remote peer
async function createPeerFor(remoteId, isInitiator) {
    if (pcs[remoteId]) return pcs[remoteId];

    const pc = new RTCPeerConnection({
        iceServers: [{ urls: "stun:stun.l.google.com:19302" }]
    });

    // add local tracks
    if (localStream) localStream.getTracks().forEach(t => pc.addTrack(t, localStream));

    // remote video element per peer (optional)
    let remoteEl = document.getElementById('remote_' + remoteId);
    if (!remoteEl) {
        remoteEl = document.createElement('video');
        remoteEl.id = 'remote_' + remoteId;
        remoteEl.autoplay = true;
        remoteEl.playsInline = true;
        remoteEl.style = "width:300px;height:200px;background:black;margin:5px;";
        (remoteVideoContainer || document.body).appendChild(remoteEl);
    }

    pc.ontrack = (ev) => {
        console.log('ontrack from', remoteId, ev);
        if (ev.streams && ev.streams[0]) remoteEl.srcObject = ev.streams[0];
        else {
            const ms = new MediaStream();
            if (ev.track) ms.addTrack(ev.track);
            remoteEl.srcObject = ms;
        }
    };

    pc.onicecandidate = (e) => {
        if (e.candidate) {
            // send ICE to that specific remote peer
            connection.invoke("SendIceTo", remoteId, JSON.stringify(e.candidate));
        }
    };

    pc.onconnectionstatechange = () => console.log('pc[' + remoteId + '] state', pc.connectionState);
    pcs[remoteId] = pc;

    // If initiator, create offer now
    if (isInitiator) {
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        await connection.invoke("SendOfferTo", remoteId, JSON.stringify(offer));
    }

    return pc;
}

// SIGNALR handlers
connection.on("ReceiveOffer", async (callerId, sdpJson) => {
    console.log("ReceiveOffer from", callerId);
    await ensureLocalStream();
    const pc = await createPeerFor(callerId, false); // non-initiator
    const sdp = JSON.parse(sdpJson);
    await pc.setRemoteDescription(new RTCSessionDescription(sdp));
    // add any local tracks if not already
    if (localStream) localStream.getTracks().forEach(t => {
        try { pc.addTrack(t, localStream); } catch (e) { /* ignore duplicate */ }
    });
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    await connection.invoke("SendAnswerTo", callerId, JSON.stringify(answer));
});

connection.on("ReceiveAnswer", async (calleeId, sdpJson) => {
    console.log("ReceiveAnswer from", calleeId);
    const pc = pcs[calleeId];
    if (!pc) return console.warn("No pc for", calleeId);
    const sdp = JSON.parse(sdpJson);
    await pc.setRemoteDescription(new RTCSessionDescription(sdp));
});

connection.on("ReceiveIceCandidate", async (senderId, candidateJson) => {
    const pc = pcs[senderId];
    const cand = JSON.parse(candidateJson);
    if (!pc) {
        // queue or ignore until pc created; simple approach: create pc placeholder
        console.warn("Received ICE for unknown pc", senderId);
        return;
    }
    try {
        await pc.addIceCandidate(new RTCIceCandidate(cand));
    } catch (e) {
        console.warn("addIceCandidate failed:", e);
    }
});

// PeerLeft handler: close and remove pc for that peer
connection.on("PeerLeft", (sid, departedConnectionId) => {
    console.log("PeerLeft", departedConnectionId);
    if (pcs[departedConnectionId]) {
        try { pcs[departedConnectionId].close(); } catch (e) { }
        delete pcs[departedConnectionId];
    }
    const el = document.getElementById('remote_' + departedConnectionId);
    if (el) el.remove();
});

// Start flow: get local stream, join, create offers to existing peers
async function startCall(sessionId) {
    sessionIdGlobal = sessionId;
    // start SignalR
    await connection.start();
    await ensureLocalStream();
    // invoke join which returns peers list
    const result = await connection.invoke("JoinSessionForMesh", sessionId);
    if (!result || result.joined === false) {
        return alert("Could not join session");
    }
    const peers = result.peers || result.Peers || [];
    console.log("Existing peers:", peers);

    // For each existing peer, create a pc and act as initiator (offerer)
    for (const remoteId of peers) {
        await createPeerFor(remoteId, true); // this will create offer and send
    }
}
