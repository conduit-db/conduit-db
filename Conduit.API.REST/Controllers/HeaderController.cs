using Conduit.API.REST.Classes;
using Conduit.API.REST.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Net.Http.Headers;
using System;
using System.Net;
using System.Net.Mime;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace Conduit.API.REST.Controllers
{
    [Route("api/v1/headers")]
    [ApiController]
    public class HeaderController : ControllerBase
    {
        private readonly ILogger<HeaderController> logger;
        private readonly IHeaderService headerService;

        public HeaderController(ILogger<HeaderController> logger, IHeaderService headerService)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.headerService = headerService ?? throw new ArgumentNullException(nameof(headerService));
        }

        [HttpGet]
        [Consumes(MediaTypeNames.Text.Html)]
        [Produces(MediaTypeNames.Application.Octet)]
        public async Task GetHeadersBinary([FromQuery] uint height, [FromQuery] uint count=1)
        {
            if (count < 1 || count > 4000)
            {
                Response.StatusCode = (int)HttpStatusCode.BadRequest;
                return;
            }
            var cancellationTokenSource = new CancellationTokenSource(20000);

            // The header count here may be less than requested as it may be more headers than are available from the base height.
            var (headerCount, socket) = await headerService.GetHeaderSocketAsync(height, count);
            using (socket)
            {
                var headerDataLength = headerCount * 80;

                Response.StatusCode = StatusCodes.Status200OK;
                Response.ContentLength = headerDataLength;
                Response.Headers.Add(HeaderNames.ContentType, MediaTypeNames.Application.Octet);

                // Stream the header data from the socket, if there is any.
                if (headerCount > 0)
                    await Util.FillPipeAsync(socket, Response.BodyWriter, logger, cancellationTokenSource.Token, headerDataLength);
                await Response.BodyWriter.CompleteAsync();
            }
        }

        // TODO Header web socket is incomplete.
        [HttpGet("/websocket")]
        public async Task GetWebSocket()
        {
            if (HttpContext.WebSockets.IsWebSocketRequest)
            {
                using WebSocket webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync();
                await HeaderWebSocketManager.ManageConnection(webSocket, headerService);
            }
            else
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            }
        }
    }
}
