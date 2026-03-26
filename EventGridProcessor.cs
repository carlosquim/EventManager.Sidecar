using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventManager.Sidecar
{
    public class EventGridConfig
    {
        public string ConnectionString { get; set; } = string.Empty;
        public string QueueName { get; set; } = string.Empty;
        public string TargetFunctionName { get; set; } = string.Empty;
    }

    public class EventGridProcessor : BackgroundService
    {
        private readonly IEventForwarder _forwarder;
        private readonly ILogger<EventGridProcessor> _logger;
        private readonly List<EventGridConfig> _configs;

        public EventGridProcessor(IEventForwarder forwarder, IConfiguration configuration, ILogger<EventGridProcessor> logger)
        {
            _forwarder = forwarder;
            _logger = logger;
            _configs = configuration.GetSection("EventGridTriggers").Get<List<EventGridConfig>>() ?? new List<EventGridConfig>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"Starting {_configs.Count} Event Grid Processors...");

            var tasks = new List<Task>();
            foreach (var config in _configs)
            {
                tasks.Add(PollEventGridQueueAsync(config, stoppingToken));
            }

            await Task.WhenAll(tasks);
        }

        private async Task PollEventGridQueueAsync(EventGridConfig config, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(config.ConnectionString) || string.IsNullOrWhiteSpace(config.QueueName)) return;

            var queueClient = new QueueClient(config.ConnectionString, config.QueueName);
            await queueClient.CreateIfNotExistsAsync(cancellationToken: ct);

            _logger.LogInformation($"Listening to Event Grid queue [{config.QueueName}]...");

            while (!ct.IsCancellationRequested)
            {
                var response = await queueClient.ReceiveMessagesAsync(maxMessages: 10, visibilityTimeout: TimeSpan.FromSeconds(30), cancellationToken: ct);
                
                foreach (var message in response.Value)
                {
                    _logger.LogInformation($"Received Event Grid message from [{config.QueueName}]. Processing...");
                    
                    // Event Grid messages in Queues are often Base64 encoded JSON arrays
                    var payload = message.MessageText;
                    
                    var success = await _forwarder.ForwardEventAsync(config.TargetFunctionName, payload);
                    
                    if (success)
                    {
                        await queueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt, ct);
                    }
                    else
                    {
                        _logger.LogError($"Failed to forward Event Grid event to [{config.TargetFunctionName}].");
                    }
                }

                if (response.Value.Length == 0)
                {
                    await Task.Delay(2000, ct);
                }
            }
        }
    }
}
