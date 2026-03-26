using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventManager.Sidecar
{
    public class CosmosDbConfig
    {
        public string ConnectionEndpoint { get; set; } = string.Empty;
        public string DatabaseName { get; set; } = string.Empty;
        public string ContainerName { get; set; } = string.Empty;
        public string LeaseContainerName { get; set; } = "leases";
        public string TargetFunctionName { get; set; } = string.Empty;
        public string AuthKey { get; set; } = string.Empty;
    }

    public class CosmosDbProcessor : IHostedService, IChangeFeedListener
    {
        private readonly IEventForwarder _forwarder;
        private readonly ILogger<CosmosDbProcessor> _logger;
        private readonly List<ChangeFeedProcessor> _processors = new();
        private readonly List<CosmosDbConfig> _configs;

        public CosmosDbProcessor(IEventForwarder forwarder, IConfiguration configuration, ILogger<CosmosDbProcessor> logger)
        {
            _forwarder = forwarder;
            _logger = logger;
            
            _configs = configuration.GetSection("CosmosTriggers").Get<List<CosmosDbConfig>>() ?? new List<CosmosDbConfig>();
        }

        public static long TotalPendingChanges => _pendingChanges.Values.Sum();
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _pendingChanges = new();

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Starting {_configs.Count} distinct Cosmos DB Processors...");

            foreach (var config in _configs)
            {
                if (string.IsNullOrWhiteSpace(config.DatabaseName) || string.IsNullOrWhiteSpace(config.ContainerName)) continue;

                CosmosClient cosmosClient;
                if (!string.IsNullOrWhiteSpace(config.AuthKey))
                {
                    _logger.LogInformation($"Using explicit AuthKey (Master Key) authentication for {config.DatabaseName}.");
                    cosmosClient = new CosmosClient(config.ConnectionEndpoint, config.AuthKey);
                }
                else if (config.ConnectionEndpoint.Contains("AccountEndpoint=") && config.ConnectionEndpoint.Contains("AccountKey="))
                {
                    _logger.LogInformation($"Using Connection String authentication for {config.DatabaseName}.");
                    cosmosClient = new CosmosClient(config.ConnectionEndpoint);
                }
                else
                {
                    _logger.LogInformation($"Using Azure.Identity (Managed Identity / AZ CLI) for {config.DatabaseName}.");
                    cosmosClient = new CosmosClient(config.ConnectionEndpoint, new Azure.Identity.DefaultAzureCredential());
                }

                var container = cosmosClient.GetContainer(config.DatabaseName, config.ContainerName);
                var leaseContainer = cosmosClient.GetContainer(config.DatabaseName, config.LeaseContainerName);

                string processorName = $"Sidecar_{config.DatabaseName}_{config.ContainerName}";

                var processor = container.GetChangeFeedProcessorBuilder<Newtonsoft.Json.Linq.JObject>(
                    processorName: processorName,
                    onChangesDelegate: async (changes, ct) => await HandleChangesAsync(changes, config.TargetFunctionName))
                    .WithInstanceName(Environment.MachineName)
                    .WithLeaseContainer(leaseContainer)
                    .Build();

                var estimator = container.GetChangeFeedEstimatorBuilder(
                    processorName: processorName,
                    estimationDelegate: (long estimatedPendingChanges, CancellationToken ct) =>
                    {
                        _pendingChanges[config.TargetFunctionName] = estimatedPendingChanges;
                        return Task.CompletedTask;
                    },
                    estimationPeriod: TimeSpan.FromSeconds(10))
                    .WithLeaseContainer(leaseContainer)
                    .Build();

                await processor.StartAsync();
                await estimator.StartAsync();
                _processors.Add(processor);
                _processors.Add(estimator);
                _logger.LogInformation($"Started Cosmos DB array listener and estimator for [{config.DatabaseName}.{config.ContainerName}] routing logically to [{config.TargetFunctionName}]");
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Stopping all Cosmos DB Processors...");
            foreach (var processor in _processors)
            {
                await processor.StopAsync();
            }
        }

        public Task StopAsync() => StopAsync(CancellationToken.None);

        private async Task HandleChangesAsync(IReadOnlyCollection<Newtonsoft.Json.Linq.JObject> changes, string targetFunctionName)
        {
            _logger.LogInformation($"[Pod: {Environment.MachineName}] Received {changes.Count} changes; forwarding to main app proxy endpoint: {targetFunctionName}.");
            var payload = Newtonsoft.Json.JsonConvert.SerializeObject(changes);
            
            var success = await _forwarder.ForwardEventAsync(targetFunctionName, payload);
            
            if (!success)
            {
                _logger.LogError($"Failed to forward Cosmos DB event to Main App Container function: {targetFunctionName}.");
                throw new Exception($"Failed to forward event to {targetFunctionName}. Aborting checkpoint.");
            }
        }
    }
}
