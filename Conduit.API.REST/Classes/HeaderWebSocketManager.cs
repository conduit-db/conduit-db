using Conduit.API.REST.Models;
using Conduit.API.REST.Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Conduit.API.REST.Classes
{
    public static class HeaderWebSocketManager
    {
        public static List<WebSocket> _webSockets = new();
        public static uint _headerHeight = uint.MaxValue;
        public static byte[] _headerData = null;

        public static async Task SendHeaderMessageBytes(WebSocket webSocket, byte[] headerData, uint height)
        {
            await webSocket.SendAsync(headerData.AsMemory(0, headerData.Length), WebSocketMessageType.Binary, false, CancellationToken.None);

            var heightData = BitConverter.GetBytes(height);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(heightData);
              
            await webSocket.SendAsync(heightData.AsMemory(0, heightData.Length), WebSocketMessageType.Binary, true, CancellationToken.None);
        }

        public static async Task SetTip(uint headerHeight, byte[] headerData)
        {
            _headerHeight = headerHeight;
            _headerData = headerData;

            foreach (WebSocket webSocket in _webSockets)
                if (webSocket.State == WebSocketState.Open)
                    await SendHeaderMessageBytes(webSocket, headerData, headerHeight);
        }

 
        // https://docs.microsoft.com/en-us/aspnet/core/fundamentals/websockets?view=aspnetcore-5.0
        public static async Task ManageConnection(WebSocket webSocket)
        {
            _webSockets.Add(webSocket);

            // If we have a cached header already notify the client, otherwise it should be pending
            // from `HeaderHostedService` and it should be sent to all connected web sockets when
            // it arrives.
            if (_headerData != null)
                await SendHeaderMessageBytes(webSocket, _headerData, _headerHeight);

            // We do not have a use for any data the client sends us through the websocket. We discard
            // a initial token amount, and disconnect them if they send more than that. Mostly we are
            // waiting for the connection to close.
            var buffer = new byte[1024 * 4];
            int receivedBytes = 0;
            try
            {
                WebSocketReceiveResult result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                while (!result.CloseStatus.HasValue && receivedBytes < 512)
                {
                    receivedBytes += result.Count;
                    result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                }
                await webSocket.CloseAsync(result.CloseStatus.Value, result.CloseStatusDescription, CancellationToken.None);
            }
            catch (SocketException)
            {
                // Catches: The client disconnecting.
                // TODO Work out best way to log this event.
            }
            catch (WebSocketException)
            {
                // Catches: The client disconnecting.
                // TODO Work out best way to log this event.
            }

            _webSockets.Remove(webSocket);
        }
    }
}
