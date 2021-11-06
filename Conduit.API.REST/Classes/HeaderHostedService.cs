using Conduit.API.REST.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Conduit.API.REST.Classes
{
    /// <summary>
    /// Listen for new tips from the raw socket server.
    /// </summary>
    public class HeaderHostedService: BackgroundService
    {
        private readonly ILogger<HeaderHostedService> _logger;
        private readonly IHeaderService _headerService;

        public HeaderHostedService(ILogger<HeaderHostedService> logger, IHeaderService headerService)
        {
            _logger = logger;
            _headerService = headerService;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Header Hosted Service is running.");

            uint headerHeight;
            byte[] headerData;
            while (true)
            {
                try
                {
                    (headerHeight, headerData) = await _headerService.GetTipAsync(stoppingToken);
                }
                catch (HeaderException)
                {
                    // This should be anything related to inability to connect or disconnection if the raw server goes down.
                    await Task.Delay(5000, stoppingToken);
                    continue;
                }
                await HeaderWebSocketManager.SetTip(headerHeight, headerData);
                break;
            }

            byte[][] headerDatas;
            while (!stoppingToken.IsCancellationRequested)
            {
                var nextHeaderHeight = headerHeight + 1;
                try
                {
                    headerDatas = await _headerService.GetHeaderSocket2Async(nextHeaderHeight, 1, externalToken: stoppingToken);
                }
                catch (HeaderException)
                {
                    // This should be anything related to inability to connect or disconnection if the raw server goes down.
                    await Task.Delay(5000, stoppingToken);
                    continue;
                }
                _logger.LogInformation($"Updated tip for height {nextHeaderHeight} detected.");
                await HeaderWebSocketManager.SetTip(nextHeaderHeight, headerDatas[0]);
                headerHeight = nextHeaderHeight;
            }
        }

        public override async Task StopAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Header Hosted Service is stopping.");

            await base.StopAsync(stoppingToken);
        }
    }
}
