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
    public class HeaderException : Exception {
        public HeaderException() : base() { }
        public HeaderException(string message) : base(message) { }
        public HeaderException(string message, Exception innerException) : base(message, innerException) { }
    }
    public class HeaderTimeoutException : HeaderException {
    }
    public class HeaderNetworkException : HeaderException {
        public HeaderNetworkException() : base() { }
        public HeaderNetworkException(string message) : base(message) { }
        public HeaderNetworkException(string message, Exception innerException) : base(message, innerException) { }
    }
    public class HeaderProtocolException : HeaderException {
        public HeaderProtocolException() : base() { }
        public HeaderProtocolException(string message) : base(message) { }
        public HeaderProtocolException(string message, Exception innerException) : base(message, innerException) { }
    }
    public class HeaderResponsePacketException : HeaderException {
        public HeaderResponsePacketException() : base() { }
        public HeaderResponsePacketException(string message) : base(message) { }
        public HeaderResponsePacketException(string message, Exception innerException) : base(message, innerException) { }
    }

    public class HeaderService: IHeaderService
    {
        private ILogger<HeaderService> logger;
        private INetworkService networkService;

        public HeaderService(ILogger<HeaderService> logger, INetworkService networkService)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.networkService = networkService ?? throw new ArgumentNullException(nameof(networkService));
        }

        public async Task<(uint, byte[])> GetTipAsync(CancellationToken externalToken = default)
        {
            var cborMap = CBORObject.NewMap()
                .Add("command", "chain_tip");
            var cborBytes = cborMap.EncodeToBytes();

            var socket = await networkService.ConnectToConduitRaw(externalToken);
            if (socket == null)
                throw new HeaderNetworkException();

            var timeoutTokenSource = new CancellationTokenSource(10000);
            var timeoutToken = timeoutTokenSource.Token;
            using CancellationTokenSource linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(timeoutToken, externalToken);

            var cborSizeBytes = BitConverter.GetBytes((ulong)cborBytes.Length);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(cborSizeBytes);

            var cborLengthData = new byte[8];
            try
            {
                var bytesSent = await socket.SendAsync(cborSizeBytes.AsMemory(), SocketFlags.None, linkedTokenSource.Token);
                if (bytesSent != cborSizeBytes.Length)
                    throw new HeaderProtocolException("Failed to send the CBOR 'chain_tip' command size to the raw server");

                bytesSent = await socket.SendAsync(cborBytes.AsMemory(), SocketFlags.None, linkedTokenSource.Token);
                if (bytesSent != cborBytes.Length)
                    throw new HeaderProtocolException("Failed to send the CBOR 'chain_tip' command to the raw server");

                var bytesRead = await socket.ReceiveAsync(cborLengthData.AsMemory(), SocketFlags.None, linkedTokenSource.Token);
                if (bytesRead != cborLengthData.Length)
                    throw new HeaderProtocolException("Failed to read cbor response size");

                // This will be a big-endian number.
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(cborLengthData);
                var cborPacketLength = BitConverter.ToUInt64(cborLengthData);
                var cborPacketData = new byte[cborPacketLength];
                bytesRead = await socket.ReceiveAsync(cborPacketData.AsMemory(), SocketFlags.None, linkedTokenSource.Token);
                if (bytesRead != cborPacketData.Length)
                    throw new HeaderProtocolException("Failed to read cbor packet");

                var cborObject = CBORObject.DecodeFromBytes(cborPacketData);
                var headerHeight = cborObject["height"].ToObject<uint>();
                var headerData = cborObject["header"].ToObject<byte[]>();
                return (headerHeight, headerData);
            }
            catch (OperationCanceledException)
            {
                socket.Close();

                if (externalToken.IsCancellationRequested)
                    throw;

                throw new HeaderTimeoutException();
            }
            catch (NotSupportedException)
            {
                socket.Close();
                throw new HeaderResponsePacketException();
            }
            catch (CBORException)
            {
                socket.Close();
                throw new HeaderResponsePacketException();
            }
            catch (Exception)
            {
                socket.Close();
                throw;
            }
        }

        public async Task<(int, Socket)> GetHeaderSocketAsync(uint startHeight, uint headerCount, CancellationToken externalToken = default)
        {
            var cborMap = CBORObject.NewMap()
                .Add("command", "headers_batched2")
                .Add("start_height", startHeight)
                .Add("batch_size", headerCount);
            var cborBytes = cborMap.EncodeToBytes();

            var socket = await networkService.ConnectToConduitRaw(externalToken);
            if (socket == null)
                throw new HeaderNetworkException("Unable to connect to raw server");

            var cborSizeBytes = BitConverter.GetBytes((ulong)cborBytes.Length);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(cborSizeBytes);

            var countData = new byte[8];
            try
            {
                var bytesSent = await socket.SendAsync(cborSizeBytes.AsMemory(), SocketFlags.None, externalToken);
                if (bytesSent != cborSizeBytes.Length)
                    throw new HeaderProtocolException("Failed to send the CBOR 'headers_batched2' command size to the raw server");

                bytesSent = await socket.SendAsync(cborBytes.AsMemory(), SocketFlags.None, externalToken);
                if (bytesSent != cborBytes.Length)
                    throw new HeaderProtocolException("Failed to send the CBOR 'headers_batched2' command to the raw server");

                var timeoutTokenSource = new CancellationTokenSource(10000);
                var timeoutToken = timeoutTokenSource.Token;
                using CancellationTokenSource linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(timeoutToken, externalToken);
                var bytesRead = await socket.ReceiveAsync(countData.AsMemory(), SocketFlags.None, linkedTokenSource.Token);
                if (bytesRead != countData.Length)
                    throw new HeaderProtocolException("Failed to read count of headers");
            }
            catch (OperationCanceledException)
            {
                socket.Close();

                if (externalToken.IsCancellationRequested)
                    throw;

                throw new HeaderTimeoutException();
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

        /// <summary>
        /// Request a range of headers.
        /// If there are no headers found, it will block and retry until it finds the requested range.
        /// This is intended to help obtain a new tip, by requesting the header after the current tip.
        /// </summary>
        /// <param name="startHeight">The height of the first header returned</param>
        /// <param name="headerCount">How many consecutive headers to return starting with the first header.</param>
        /// <returns>The header requested</returns>
        public async Task<byte[][]> GetHeaderSocket2Async(uint startHeight, uint headerCount, int timeoutMs=0, CancellationToken externalToken = default)
        {
            var cborMap = CBORObject.NewMap()
                .Add("command", "headers_batched")
                .Add("start_height", startHeight)
                .Add("batch_size", headerCount);
            var cborBytes = cborMap.EncodeToBytes();

            var socket = await networkService.ConnectToConduitRaw(externalToken);
            if (socket == null)
                throw new HeaderNetworkException("Unable to connect to raw server");

            var cborSizeBytes = BitConverter.GetBytes((ulong)cborBytes.Length);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(cborSizeBytes);

            CancellationToken token = externalToken;
            if (timeoutMs > 0)
            {
                var timeoutTokenSource = new CancellationTokenSource(timeoutMs);
                var timeoutToken = timeoutTokenSource.Token;
                using CancellationTokenSource linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(timeoutToken, externalToken);
                token = linkedTokenSource.Token;
            }

            var cborLengthData = new byte[8];
            try
            {
                var bytesSent = await socket.SendAsync(cborSizeBytes.AsMemory(), SocketFlags.None, token);
                if (bytesSent != cborSizeBytes.Length)
                    throw new HeaderProtocolException("Failed to send the CBOR 'headers_batched' command size to the raw server");

                bytesSent = await socket.SendAsync(cborBytes.AsMemory(), SocketFlags.None, token);
                if (bytesSent != cborBytes.Length)
                    throw new HeaderProtocolException("Failed to send the CBOR 'headers_batched' command to the raw server");

                var bytesRead = await socket.ReceiveAsync(cborLengthData.AsMemory(), SocketFlags.None, token);
                if (bytesRead != cborLengthData.Length)
                    throw new HeaderProtocolException("Failed to read cbor response size");

                // This will be a big-endian number.
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(cborLengthData);
                var cborPacketLength = BitConverter.ToUInt64(cborLengthData);
                var cborPacketData = new byte[cborPacketLength];
                bytesRead = await socket.ReceiveAsync(cborPacketData.AsMemory(), SocketFlags.None, token);
                if (bytesRead != cborPacketData.Length)
                    throw new HeaderProtocolException("Failed to read cbor packet");

                var cborObject = CBORObject.DecodeFromBytes(cborPacketData);
                var headerDatas = cborObject["headers_batch"].ToObject<byte[][]>();
                return headerDatas;
            }
            catch (OperationCanceledException)
            {
                socket.Close();

                if (externalToken.IsCancellationRequested)
                    throw;
                throw new HeaderTimeoutException();
            }
            catch (Exception)
            {
                socket.Close();
                throw;
            }
        }
    }
}
