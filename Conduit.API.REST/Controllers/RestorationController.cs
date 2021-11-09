using Conduit.API.REST.Enums;
using Conduit.API.REST.Models;
using Conduit.MySQL;
using Conduit.MySQL.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Net.Http.Headers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Mime;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Conduit.API.REST.Controllers
{
    [Route("api/v1/restoration")]
    [ApiController]
    public class RestorationController : ControllerBase
    {
        private readonly IRestorationService restorationService;
        private readonly ILogger<RestorationController> logger;
        private readonly byte[] emptyHash;

        public RestorationController(ILogger<RestorationController> logger, IRestorationService restorationService)
        {
            this.restorationService = restorationService ?? throw new ArgumentNullException(nameof(restorationService));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

            emptyHash = new byte[32];
            Array.Clear(emptyHash, 0, emptyHash.Length);
        }

        /// <summary>
        /// Match user-provided push data hashes.
        /// </summary>
        /// <returns>An array of matches keyed to the push data hash it matched on.</returns>
        /// Swagger does not allow GET methods to have data in the body. This is some elitist "We know better" nonsense.
        /// For now this is changed from HttpGet to HttpPost.
        [HttpPost("search")]
        public async Task GetMatches([FromBody] PushDataFilterModel filterModel)
        {
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

            Response.StatusCode = StatusCodes.Status200OK;
            var outputStream = Response.Body;
            await foreach (var match in restorationService.GetPushDataFilterMatches(filterModel.PushDataFilter))
            {
                if (requestType == RequestAcceptType.Binary)
                {
                    var memoryStream = new MemoryStream(109);
                    var binaryWriter = new BinaryWriter(memoryStream);
                    binaryWriter.Write(match.PushDataHash);         // 32
                    binaryWriter.Write(match.TransactionHash);      // 32
                    if (match.SpendTransactionHash == null)
                        binaryWriter.Write(emptyHash);              // 32
                    else
                        binaryWriter.Write(match.SpendTransactionHash);

                    var indexBytes = BitConverter.GetBytes(match.Index);
                    if (BitConverter.IsLittleEndian)
                        Array.Reverse(indexBytes);
                    binaryWriter.Write(indexBytes);                 // 4

                    var spendingInputIndexBytes = BitConverter.GetBytes(match.SpendInputIndex);
                    if (BitConverter.IsLittleEndian)
                        Array.Reverse(spendingInputIndexBytes);
                    binaryWriter.Write(spendingInputIndexBytes);    // 4

                    var blockHeightBytes = BitConverter.GetBytes(match.BlockHeight);
                    if (BitConverter.IsLittleEndian)
                        Array.Reverse(blockHeightBytes);
                    binaryWriter.Write(blockHeightBytes);           // 4

                    // The spare byte is packed last so that the other data can be 32 bit aligned.
                    binaryWriter.Write((byte)match.ReferenceType);  // 1

                    var packedBytes = memoryStream.ToArray();
                    Debug.Assert(packedBytes.Length == 109);        // 109 = 32 + 32 + 32 + 4 + 4 + 4 + 1
                    await outputStream.WriteAsync(packedBytes.AsMemory(0, packedBytes.Length));
                }
                else
                {
                    // We add the new line as we are streaming separate JSON objects each on it's own line.
                    // The serialization does not add any extra whitespace, or new lines between fields.
                    var jsonText = JsonSerializer.Serialize(new PushDataMatchModel(match)) + "\n";
                    var jsonBytes = Encoding.ASCII.GetBytes(jsonText);
                    await outputStream.WriteAsync(jsonBytes.AsMemory(0, jsonBytes.Length));
                }
            }

            await outputStream.FlushAsync();
        }
    }
}
