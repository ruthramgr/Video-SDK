using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;

namespace Webvcall.Hubs
{
    

    public class CallHubReset : Hub
    {
        public record JoinResult(bool Joined, bool IsInitiator, List<string> Participants);
        // sessionId => ordered list of connectionIds
        private static readonly ConcurrentDictionary<string, List<string>> Sessions
            = new ConcurrentDictionary<string, List<string>>();

        public async Task<JoinResult> JoinSession(string sessionId)
        {
            var list = Sessions.GetOrAdd(sessionId, _ => new List<string>());
            lock (list)
            {
                if (!list.Contains(Context.ConnectionId))
                {
                    // limit to 3 participants
                    if (list.Count >= 3)
                    {
                        return new JoinResult(false, false, new List<string>(list));
                    }
                    list.Add(Context.ConnectionId);
                }
            }

            await Groups.AddToGroupAsync(Context.ConnectionId, sessionId);

            // snapshot for broadcasting
            if (Sessions.TryGetValue(sessionId, out var snapshotList))
            {
                List<string> snapshot;
                lock (snapshotList) { snapshot = new List<string>(snapshotList); }
                // notify group of updated participant list (so clients can map peers -> slots)
                await Clients.Group(sessionId).SendAsync("PeerListUpdated", sessionId, snapshot, snapshot.Count > 0 ? snapshot[0] : null);
            }

            bool isInitiator;
            lock (list) { isInitiator = list.Count == 1; }

            List<string> participants;
            lock (list) { participants = new List<string>(list); }

            return new JoinResult(true, isInitiator, participants);
        }

        public async Task LeaveSession(string sessionId)
        {
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, sessionId);

            if (Sessions.TryGetValue(sessionId, out var list))
            {
                lock (list)
                {
                    list.Remove(Context.ConnectionId);
                    if (list.Count == 0)
                    {
                        Sessions.TryRemove(sessionId, out _);
                    }
                }
            }

            if (Sessions.TryGetValue(sessionId, out var snapshotList))
            {
                List<string> snapshot;
                lock (snapshotList) { snapshot = new List<string>(snapshotList); }
                await Clients.Group(sessionId).SendAsync("PeerListUpdated", sessionId, snapshot, snapshot.Count > 0 ? snapshot[0] : null);
            }

            await Clients.Group(sessionId).SendAsync("PeerLeft", sessionId, Context.ConnectionId);
        }

        // Send offer to a specific target connection id
        public Task SendOffer(string sessionId, string targetConnectionId, string sdp)
            => Clients.Client(targetConnectionId).SendAsync("ReceiveOffer", Context.ConnectionId, sdp);

        // Send answer to specific target
        public Task SendAnswer(string sessionId, string targetConnectionId, string sdp)
            => Clients.Client(targetConnectionId).SendAsync("ReceiveAnswer", Context.ConnectionId, sdp);

        // Send ICE candidate to specific target
        public Task SendIceCandidate(string sessionId, string targetConnectionId, string candidate)
            => Clients.Client(targetConnectionId).SendAsync("ReceiveIceCandidate", Context.ConnectionId, candidate);

        public override async Task OnDisconnectedAsync(Exception? exception)
        {
            foreach (var kvp in Sessions)
            {
                var list = kvp.Value;
                lock (list)
                {
                    if (list.Contains(Context.ConnectionId))
                    {
                        list.Remove(Context.ConnectionId);
                        if (list.Count == 0)
                        {
                            Sessions.TryRemove(kvp.Key, out _);
                        }
                    }
                }

                // broadcast updated list for each affected session
                if (list.Count > 0)
                {
                    List<string> snapshot;
                    lock (list) { snapshot = new List<string>(list); }
                    await Clients.Group(kvp.Key).SendAsync("PeerListUpdated", kvp.Key, snapshot, snapshot.Count > 0 ? snapshot[0] : null);
                }
                else
                {
                    await Clients.Group(kvp.Key).SendAsync("PeerLeft", kvp.Key, Context.ConnectionId);
                }
            }

            await base.OnDisconnectedAsync(exception);
        }
    }
}

