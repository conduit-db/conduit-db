using Conduit.API.REST.Classes;
using Microsoft.Extensions.Logging;
using PeterO.Cbor;
using System;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Conduit.API.REST.Services
{
    public class HeaderTimeoutError : Exception {}
    public class HeaderNetworkError : Exception { }

    public class HeaderService: IHeaderService
    {
        private ILogger<HeaderService> logger;
        private INetworkService networkService;

        public HeaderService(ILogger<HeaderService> logger, INetworkService networkService)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.networkService = networkService ?? throw new ArgumentNullException(nameof(networkService));
        }

        public async Task<(int, Socket)> GetHeaderSocketAsync(uint startHeight, uint headerCount)
        {
            var cborMap = CBORObject.NewMap()
                .Add("command", "headers_batched2")
                .Add("start_height", startHeight)
                .Add("batch_size", headerCount);
            var cborBytes = cborMap.EncodeToBytes();

            var socket = await networkService.ConnectToConduitRaw();
            if (socket == null)
                throw new Exception("Unable to connect to raw server");

            var cborSizeBytes = BitConverter.GetBytes((ulong)cborBytes.Length);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(cborSizeBytes);

            var countData = new byte[8];
            try
            {
                var bytesSent = await socket.SendAsync(cborSizeBytes.AsMemory(), SocketFlags.None);
                if (bytesSent != cborSizeBytes.Length)
                    throw new Exception("Failed to send the CBOR 'headers_batched' command size to the raw server");

                bytesSent = await socket.SendAsync(cborBytes.AsMemory(), SocketFlags.None);
                if (bytesSent != cborBytes.Length)
                    throw new Exception("Failed to send the CBOR 'headers_batched' command to the raw server");

                var bytesRead = await socket.ReceiveAsync(countData.AsMemory(), SocketFlags.None, new CancellationTokenSource(10000).Token);
                if (bytesRead != countData.Length)
                    throw new Exception("Failed to read count of headers");
            }
            catch (OperationCanceledException)
            {
                socket.Close();
                throw new HeaderTimeoutError();
            }
            catch (Exception)
            {
                socket.Close();
                throw;
            }
 
            // This will be a big-endian number.
            if (BitConverter.IsLittleEndian)
                Array.Reverse(countData);
            var actualHeaderCount = BitConverter.ToUInt64(countData);
            return ((int)actualHeaderCount, socket);
        }
    }
}
