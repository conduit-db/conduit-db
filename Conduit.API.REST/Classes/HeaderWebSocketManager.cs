using Conduit.API.REST.Models;
using Conduit.API.REST.Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Conduit.API.REST.Classes
{
    public class HeaderWebSocketManager
    {
        public static List<WebSocket> webSockets = new List<WebSocket>();

        public static async Task SendHeaderMessageJSON(WebSocket webSocket, byte[] headerData, uint height)
        {
            var jsonText = JsonSerializer.Serialize(new HeaderHeightModel
            {
                Header = Convert.ToHexString(headerData),
                Height = height
            });
            // TODO This probably needs a separator so tips can be distinguished if there's a long time between reads.
            var jsonBytes = Encoding.ASCII.GetBytes(jsonText);
            await webSocket.SendAsync(jsonBytes.AsMemory(0, jsonBytes.Length), WebSocketMessageType.Text, true, CancellationToken.None);
        }

        public static async Task SendHeaderMessageBytes(WebSocket webSocket, byte[] headerData, uint height)
        {
            await webSocket.SendAsync(headerData.AsMemory(0, headerData.Length), WebSocketMessageType.Binary, false, CancellationToken.None);

            // TODO Maybe there's a better way to just encode a unsigned int and write it than this, which seems like a lot of work for little benefit.
            var memoryStream = new MemoryStream(4);
            var binaryWriter = new BinaryWriter(memoryStream);
            binaryWriter.Write(height);
            var packedBytes = memoryStream.ToArray();
            await webSocket.SendAsync(packedBytes.AsMemory(0, packedBytes.Length), WebSocketMessageType.Binary, true, CancellationToken.None);
        }

        public static async Task SendNewTip(WebSocket webSocket, byte[] headerData, uint height, WebSocketMessageType messageType)
        {
            if (messageType == WebSocketMessageType.Text)
            {
                await SendHeaderMessageJSON(webSocket, headerData, height);
            }
            else
            {
                await SendHeaderMessageBytes(webSocket, headerData, height);
            }
        }

        // TODO This should be called externally.
        public static async Task BroadcastNewTip(byte[] headerData, uint height)
        {
            foreach (WebSocket webSocket in webSockets)
                if (webSocket.State == WebSocketState.Open)
                    await SendNewTip(webSocket, headerData, height, WebSocketMessageType.Binary);
        }


        // https://docs.microsoft.com/en-us/aspnet/core/fundamentals/websockets?view=aspnetcore-5.0
        // TODO Get the current processed tip header and it's height and send it rather than the dummy data.
        // TODO All web socket connections are assumed to be binary, no idea the correct way or whether we need to worry about people that prefer text.
        public static async Task ManageConnection(WebSocket webSocket, IHeaderService headerService)
        {
            webSockets.Add(webSocket);

            uint height = 0;
            byte[] headerData = new byte[80];
            await SendNewTip(webSocket, headerData, height, WebSocketMessageType.Binary);

            // All we are doing here in theory is blocking waiting for the connection to close.
            // TODO Does this even block? The `ReceiveAsync` method says it does not block.
            var buffer = new byte[1024 * 4];
            WebSocketReceiveResult result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            while (!result.CloseStatus.HasValue)
            {
                result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            }
            await webSocket.CloseAsync(result.CloseStatus.Value, result.CloseStatusDescription, CancellationToken.None);

            webSockets.Remove(webSocket);
        }
    }
}
