using Microsoft.AspNetCore.SignalR;
using System.Collections.Concurrent;
using System.Threading.Tasks;
namespace Webvcall.Hubs
{
    public class CallHub : Hub
    {

        // Map sessionId → list of connectionIds
        private static readonly ConcurrentDictionary<string, ConcurrentBag<string>> Sessions
            = new ConcurrentDictionary<string, ConcurrentBag<string>>();

        // try to join a session

        public async Task<bool> JoinSession(string sessionId)
        {
            //await Groups.AddToGroupAsync(Context.ConnectionId, sessionId);

            var bag = Sessions.GetOrAdd(sessionId, _ => new ConcurrentBag<string>());

            // if already two participants, reject join
            if (bag.Count >= 2)
            {
                // optionally notify client of “session full”
                await Clients.Caller.SendAsync("SessionFull", sessionId);
                return false;
            }

            bag.Add(Context.ConnectionId);
            await Groups.AddToGroupAsync(Context.ConnectionId, sessionId);

            // if now two participants, you can notify both users that peer is ready
            if (bag.Count == 2)
            {
                // send a “ready” message to both
                await Clients.Group(sessionId).SendAsync("PeerJoined", sessionId);
            }

            return true;
        }

        

            public async Task LeaveSession(string sessionId)
            {
            //await Groups.RemoveFromGroupAsync(Context.ConnectionId, sessionId);
            await Groups.RemoveFromGroupAsync(Context.ConnectionId, sessionId);

            if (Sessions.TryGetValue(sessionId, out var bag))
            {
                // remove this connectionId
                // ConcurrentBag doesn't support remove easily; for demo:
                // you could rebuild bag or track with other structure
            }

            await Clients.Group(sessionId).SendAsync("PeerLeft", sessionId);
        }

            public async Task SendOffer(string sessionId, string sdp)
            {
                await Clients.OthersInGroup(sessionId)
                            .SendAsync("ReceiveOffer", Context.ConnectionId, sdp);
            }

            public async Task SendAnswer(string sessionId, string sdp)
            {
                await Clients.OthersInGroup(sessionId)
                            .SendAsync("ReceiveAnswer", Context.ConnectionId, sdp);
            }

            public async Task SendIceCandidate(string sessionId, string candidate)
            {
                await Clients.OthersInGroup(sessionId)
                            .SendAsync("ReceiveIceCandidate", Context.ConnectionId, candidate);
            }
        public override async Task OnDisconnectedAsync(System.Exception exception)
        {
            // Clean up: remove from all sessions
            foreach (var kvp in Sessions)
            {
                if (kvp.Value.Contains(Context.ConnectionId))
                {
                    // remove and notify
                    await LeaveSession(kvp.Key);
                    break;
                }
            }
            await base.OnDisconnectedAsync(exception);
        }
    

        }
}
