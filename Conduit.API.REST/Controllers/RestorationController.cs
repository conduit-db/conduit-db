using Conduit.API.REST.Models;
using Conduit.MySQL;
using Conduit.MySQL.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Conduit.API.REST.Controllers
{
    [Route("api/v1/restoration")]
    [ApiController]
    public class RestorationController : ControllerBase
    {
        private readonly IRestorationService restorationService;
        private readonly ILogger<RestorationController> logger;

        public RestorationController(ILogger<RestorationController> logger, IRestorationService restorationService)
        {
            this.restorationService = restorationService ?? throw new ArgumentNullException(nameof(restorationService));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Match user-provided push data hashes.
        /// </summary>
        /// <returns>An array of matches keyed to the push data hash it matched on.</returns>
        /// Swagger does not allow GET methods to have data in the body. This is some elitist "We know better" nonsense.
        /// For now this is changed from HttpGet to HttpPost.
        [HttpPost("search")]
        public async Task<IEnumerable<PushDataMatchModel>> GetMatches([FromBody] PushDataFilterModel filterModel)
        {
            var matches = await restorationService.GetPushDataFilterMatches(filterModel.PushDataFilter);
            return matches.Select(s => new PushDataMatchModel(s));
        }
    }
}
