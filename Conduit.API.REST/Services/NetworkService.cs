using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Conduit.API.REST.Services
{
    public class NetworkService: INetworkService
    {
        private readonly ILogger<NetworkService> logger;
        private static string rawHost;
        private static int rawPort;

        public NetworkService(ILogger<NetworkService> logger, IConfiguration configuration)
        {
            var rawURL = configuration["CONDUIT_RAW_API_HOST"];

            if (string.IsNullOrWhiteSpace(rawURL))
                throw new Exception("URL for raw service is null or empty");

            var index = rawURL.IndexOf(':');
            if (index == -1)
                throw new Exception("URL for raw service must be of the form '<host>:<port>'");

            rawHost = rawURL.Substring(0, index);
            rawPort = int.Parse(rawURL[(index + 1)..]);
        }

        public async Task<Socket> ConnectToConduitRaw(CancellationToken externalToken = default)
        {
            return await Connect(rawHost, rawPort, externalToken);
        }

        private static async Task<Socket> Connect(string host, int port, CancellationToken externalToken=default)
        {
            var hostEntry = Dns.GetHostEntry(host);

            foreach (IPAddress address in hostEntry.AddressList)
            {
                IPEndPoint endpoint = new(address, port);
                Socket tempSocket = new(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

                await tempSocket.ConnectAsync(endpoint, externalToken);
                if (tempSocket.Connected)
                    return tempSocket;
            }
            return null;
        }
    }
}
