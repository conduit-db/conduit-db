using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;
using Conduit.API.REST.Classes;
using Microsoft.Extensions.Logging;

namespace Conduit.API.REST.Services
{
    public class HttpClientFactoryService : IHttpClientServiceImplementation
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly JsonSerializerOptions _options;
        private ILogger<HttpClientFactoryService> _logger;

        public HttpClientFactoryService(IHttpClientFactory httpClientFactory, ILogger<HttpClientFactoryService> logger)
        {
            _httpClientFactory = httpClientFactory;
            _options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }
        
        public async Task<byte[]> GetRawTransaction(string transactionHash)
        {
            var httpClient = _httpClientFactory.CreateClient();
            httpClient.DefaultRequestHeaders.Clear();
            httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));
            using (HttpResponseMessage response = await httpClient.GetAsync($"http://127.0.0.1:34525/api/v1/transaction/{transactionHash}", HttpCompletionOption.ResponseHeadersRead))
            {
                response.EnsureSuccessStatusCode();
                var stream = await response.Content.ReadAsStreamAsync();
                if (response.Content.Headers.ContentLength != null)
                {
                    var rawTransaction = Util.ReadFully(stream);
                    Debug.WriteLine($"rawTransaction: {Convert.ToHexString(rawTransaction)}");
                    return rawTransaction;
                }
                else
                    throw new ArgumentNullException(nameof(response.Content.Headers.ContentLength.Value));
            }
        }
    }
}
