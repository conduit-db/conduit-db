using Conduit.API.REST.Enums;
using Conduit.MySQL.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Net.Http.Headers;
using System;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net.Mime;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Conduit.API.REST.Controllers
{
    [Route("api/v1/transactions")]
    [ApiController]
    public class TransactionController : ControllerBase
    {
        private readonly ITransactionService transactionService;
        private readonly ILogger<TransactionController> logger;
        private static int HashXLength = 14;

        public TransactionController(ILogger<TransactionController> logger, ITransactionService transactionService)
        {
            this.transactionService = transactionService ?? throw new ArgumentNullException(nameof(transactionService));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }
        
        private static byte[] HashToHashX(byte[] fullHash)
        {
            return fullHash.Take(14).ToArray();
        }

        [HttpGet("{transactionId}")]
        public async Task GetTransaction(string transactionId)
        {
            if (transactionId.Length != 32 * 2)
            {
                Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }
            byte[] transactionHash = Convert.FromHexString(transactionId).Reverse().ToArray();

            var requestType = RequestAcceptType.JSON;
            Request.Headers.TryGetValue(HeaderNames.Accept, out var acceptType);
            if (acceptType == MediaTypeNames.Application.Octet)
            {
                requestType = RequestAcceptType.Binary;
                Response.Headers.Add(HeaderNames.ContentType, MediaTypeNames.Application.Octet);
            }
            else
            {
                Response.Headers.Add(HeaderNames.ContentType, MediaTypeNames.Application.Json);
            }

            // TOOD(optimisation) We should see if we can stream the transaction data in, but we would want the length ahead of the full data, so we can set content length.
            var (transactionStream, transactionLength) = await transactionService.GetTransactionBytes(HashToHashX(transactionHash));
            if (transactionStream == null)
            {
                Response.StatusCode = StatusCodes.Status404NotFound;
                return;
            }

            Response.StatusCode = StatusCodes.Status200OK;
            var outputStream = Response.Body;
            if (requestType == RequestAcceptType.Binary)
                Response.ContentLength = transactionLength;
            else
                Response.ContentLength = transactionLength * 2;

            byte[] transactionData = new byte[4096];
            while (transactionStream.Position < transactionLength - 1)
            {
                // TODO Does this busy loop????? How the hell do people know these things.
                var readLength = await transactionStream.ReadAsync(transactionData.AsMemory(0, transactionData.Length), CancellationToken.None);
                if (readLength > 0)
                {
                    if (requestType == RequestAcceptType.Binary)
                        await outputStream.WriteAsync(transactionData.AsMemory(0, readLength));
                    else
                    {
                        var jsonText = Convert.ToHexString(transactionData.AsSpan(0, readLength));
                        var jsonBytes = Encoding.ASCII.GetBytes(jsonText);
                        await outputStream.WriteAsync(jsonBytes.AsMemory(0, readLength * 2));
                    }
                }
            }

            await outputStream.FlushAsync();
        }

        [HttpGet("proof/{transactionId}")]
        public async Task GetProof(string transactionId, [FromQuery] bool withTransaction=false)
        {
            if (transactionId.Length != 32 * 2)
            {
                Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }
            byte[] transactionHash = Convert.FromHexString(transactionId).Reverse().ToArray();

            Request.Headers.TryGetValue(HeaderNames.Accept, out var acceptType);
            var outputStream = Response.Body;
            Stream proofStream;
            int proofLength;
            if (acceptType == MediaTypeNames.Application.Octet)
            {
                Response.Headers.Add(HeaderNames.ContentType, MediaTypeNames.Application.Octet);
                (proofStream, proofLength) = await transactionService.GetProofBytes(transactionHash, withTransaction);
            }
            else
            {
                Response.Headers.Add(HeaderNames.ContentType, MediaTypeNames.Application.Json);
                (proofStream, proofLength) = await transactionService.GetProofJSON(transactionHash, withTransaction);
            }

            byte[] transactionData = new byte[4096];
            while (proofStream.Position < proofLength - 1)
            {
                // TODO Does this busy loop????? How the hell do people know these things.
                var readLength = await proofStream.ReadAsync(transactionData.AsMemory(0, transactionData.Length), CancellationToken.None);
                if (readLength > 0)
                    await outputStream.WriteAsync(transactionData.AsMemory(0, readLength));
            }

            await outputStream.FlushAsync();
        }

        // TODO: Once we have the data streaming in:
        //  1. Look at the link and work out how to instegrate it.
        //  2. Test that it is actually streaming the data in through the lower level connection.
        // https://docs.microsoft.com/en-us/dotnet/standard/io/pipelines#stream-example
        //public async Task FillPipeAsync(Stream stream, PipeWriter writer, RequestAcceptType requestType)
        //{
        //    const int minimumBufferSize = 512;

        //    while (true)
        //    {
        //        // Allocate at least 512 bytes from the PipeWriter.
        //        Memory<byte> memory = writer.GetMemory(minimumBufferSize);
        //        try
        //        {
        //            int bytesRead = await stream.ReadAsync(memory, CancellationToken.None);
        //            if (bytesRead == 0)
        //            {
        //                break;
        //            }
        //            // Tell the PipeWriter how much was read from the Socket.
        //            writer.Advance(bytesRead);
        //        }
        //        catch (Exception ex)
        //        {
        //            logger.LogError(ex.ToString());
        //            break;
        //        }

        //        // Make the data available to the PipeReader.
        //        FlushResult result = await writer.FlushAsync();

        //        if (result.IsCompleted)
        //        {
        //            break;
        //        }
        //    }

        //    // By completing PipeWriter, tell the PipeReader that there's no more data coming.
        //    await writer.CompleteAsync();
        //}
    }
}
