using Conduit.API.REST.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mime;
using System.Threading.Tasks;

namespace Conduit.API.REST.Controllers
{
    [Route("api/v1/scan")]
    [ApiController]
    public class ScanController : Controller
    {
        private readonly IScanService scanService;
        private readonly ILogger<ScanController> logger;

        public ScanController(ILogger<ScanController> logger, IScanService scanService)
        {
            this.scanService = scanService ?? throw new ArgumentNullException(nameof(scanService));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        [HttpPost("filter")]
        [Consumes(MediaTypeNames.Application.Octet)]
        public async Task<ActionResult> SubmitBinaryFilter([FromQuery] string postURI)
        {
            // TODO Do something with this to get the filter data.
            // Request.BodyReader;
            // TODO Persist the filter.
            // TODO Register the filter to be run on any new block.
            await scanService.CreateFilter(Array.Empty<byte>());
            return new OkResult();
        }
    }
}
