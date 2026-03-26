using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventManager.Sidecar
{
    public class BlobConfig
    {
        public string ConnectionString { get; set; } = string.Empty;
        public string QueueName { get; set; } = string.Empty;
        public string TargetFunctionName { get; set; } = string.Empty;
    }

    public class BlobStorageProcessor : BackgroundService
    {
        private readonly IEventForwarder _forwarder;
        private readonly ILogger<BlobStorageProcessor> _logger;
        private readonly List<BlobConfig> _configs;

        public BlobStorageProcessor(IEventForwarder forwarder, IConfiguration configuration, ILogger<BlobStorageProcessor> logger)
        {
            _forwarder = forwarder;
            _logger = logger;
            _configs = configuration.GetSection("BlobTriggers").Get<List<BlobConfig>>() ?? new List<BlobConfig>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"Starting {_configs.Count} Blob Storage Polling Processors...");

            var tasks = new List<Task>();
            foreach (var config in _configs)
            {
                tasks.Add(PollQueueAsync(config, stoppingToken));
            }

            await Task.WhenAll(tasks);
        }

        private async Task PollQueueAsync(BlobConfig config, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(config.ConnectionString) || string.IsNullOrWhiteSpace(config.QueueName)) return;

            var queueClient = new QueueClient(config.ConnectionString, config.QueueName);
            await queueClient.CreateIfNotExistsAsync(cancellationToken: ct);

            _logger.LogInformation($"Polling queue [{config.QueueName}] for blob events...");

            while (!ct.IsCancellationRequested)
            {
                var response = await queueClient.ReceiveMessagesAsync(maxMessages: 10, cancellationToken: ct);
                
                foreach (var message in response.Value)
                {
                    _logger.LogInformation($"Received blob signal from queue [{config.QueueName}]. Forwarding to [{config.TargetFunctionName}]...");
                    
                    var success = await _forwarder.ForwardEventAsync(config.TargetFunctionName, message.MessageText);
                    
                    if (success)
                    {
                        await queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, ct);
                    }
                    else
                    {
                        _logger.LogError($"Failed to forward event to [{config.TargetFunctionName}]. Message will be retried.");
                    }
                }

                if (response.Value.Length == 0)
                {
                    await Task.Delay(2000, ct); // Empty poll delay
                }
            }
        }
    }
}
