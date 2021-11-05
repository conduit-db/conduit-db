using Conduit.BlockScanner.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Conduit.BlockScanner.Controllers
{
    /// <summary>
    /// This controller is used by the external REST API to communicate with a block scanning process.
    /// </summary>
    [Route("api/v1/external")]
    [ApiController]
    public class ExternalController : Controller
    {
        private readonly IScanService scanService;
        private readonly ILogger<ExternalController> logger;

        public ExternalController(ILogger<ExternalController> logger, IScanService scanService)
        {
            this.scanService = scanService ?? throw new ArgumentNullException(nameof(scanService));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public IActionResult Index()
        {
            return View();
        }
    }
}
