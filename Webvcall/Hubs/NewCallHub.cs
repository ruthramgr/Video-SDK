using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;

namespace Webvcall.Hubs
{
    public record JoinResult(bool Joined, bool IsInitiator);
    public class NewCallHub : Hub
    {
        // sessionId => ordered list of connectionIds
        private static readonly ConcurrentDictionary<string, List<string>> Sessions
            = new ConcurrentDictionary<string, List<string>>();
        // connectionId => username
        private static readonly ConcurrentDictionary<string, string> ConnectionUsernames
            = new ConcurrentDictionary<string, string>();

        //public async Task<JoinResult> JoinSession(string sessionId)       
        public async Task<JoinResult> JoinSession(string sessionId,string username)
        {

            // store username for this connection (allow empty if client didn't provide)
            if (!string.IsNullOrEmpty(username))
            {
                ConnectionUsernames[Context.ConnectionId] = username;
            }
            else
            {
                ConnectionUsernames.TryRemove(Context.ConnectionId, out _);
            }
            var list = Sessions.GetOrAdd(sessionId, _ => new List<string>());
            lock (list)
            {
                if (!list.Contains(Context.ConnectionId))
                {
                    list.Add(Context.ConnectionId);
                }

                if (list.Count > 2)
                {
                    ConnectionUsernames.TryRemove(Context.ConnectionId, out _);
                    // session full
                    return new JoinResult(false, false);
                }
            }

            await Groups.AddToGroupAsync(Context.ConnectionId, sessionId);

            // If second participant just joined, notify group and include initiator id
            if (Sessions.TryGetValue(sessionId, out var snapshotList))
            {
                List<string> snapshot;
                lock (snapshotList) { snapshot = new List<string>(snapshotList); }
                if (snapshot.Count == 2)
                {
                    var initiatorConnectionId = snapshot[0];
                    ConnectionUsernames.TryGetValue(initiatorConnectionId, out var initiatorName);
                    await Clients.Group(sessionId).SendAsync("PeerJoined", sessionId, initiatorConnectionId,username);
                }
            }

            // is initiator = true if we are the first to join
            bool isInitiator;
            lock (list) { isInitiator = list.Count == 1; }

            return new JoinResult(true, isInitiator);
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
            // remove stored username for this connection
            ConnectionUsernames.TryRemove(Context.ConnectionId, out _);
            await Clients.Group(sessionId).SendAsync("PeerLeft", sessionId, Context.ConnectionId);
        }

        public Task SendOffer(string sessionId, string sdp)
            => Clients.OthersInGroup(sessionId).SendAsync("ReceiveOffer", Context.ConnectionId, sdp);

        public Task SendAnswer(string sessionId, string sdp)
            => Clients.OthersInGroup(sessionId).SendAsync("ReceiveAnswer", Context.ConnectionId, sdp);

        public Task SendIceCandidate(string sessionId, string candidate)
        {

           return  Clients.OthersInGroup(sessionId).SendAsync("ReceiveIceCandidate", Context.ConnectionId, candidate);
        }
            

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
            }
            ConnectionUsernames.TryRemove(Context.ConnectionId, out _);
            await base.OnDisconnectedAsync(exception);
        }
    }
}


