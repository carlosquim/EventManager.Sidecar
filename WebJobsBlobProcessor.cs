using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace EventManager.Sidecar
{
    public class WebJobsBlobProcessor
    {
        private readonly IEventForwarder _forwarder;
        private readonly ILogger<WebJobsBlobProcessor> _logger;

        public WebJobsBlobProcessor(IEventForwarder forwarder, ILogger<WebJobsBlobProcessor> logger)
        {
            _forwarder = forwarder;
            _logger = logger;
        }

        public async Task ProcessBlobAsync(
            [BlobTrigger("%WebJobsBlobContainer%/{name}", Connection = "AzureWebJobsStorage")] Stream blobStream,
            string name)
        {
            _logger.LogInformation($"[WebJobs SDK] Discovered new blob: {name}. Forwarding...");
            
            string payload = $"{{\"blobName\": \"{name}\", \"source\": \"WebJobsSDK\"}}";
            
            // Forwarding to the same common target function 
            var success = await _forwarder.ForwardEventAsync("blobtriggercqm01", payload);
            
            if (!success)
            {
                _logger.LogError($"Failed to forward WebJobs blob event for {name}");
                throw new Exception($"Forwarding failed for {name}. Letting WebJobs SDK retry.");
            }
        }
    }
}
