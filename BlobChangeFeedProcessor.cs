using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.ChangeFeed;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventManager.Sidecar
{
    public class ChangeFeedConfig
    {
        public string ConnectionString { get; set; } = string.Empty;
        public string TargetFunctionName { get; set; } = string.Empty;
    }

    public class BlobChangeFeedProcessor : BackgroundService
    {
        private readonly IEventForwarder _forwarder;
        private readonly IBlobLeaseManager _leaseManager;
        private readonly ILogger<BlobChangeFeedProcessor> _logger;
        private readonly List<ChangeFeedConfig> _configs;
        private readonly string _instanceId;
        private readonly string _leaseId;

        public BlobChangeFeedProcessor(IEventForwarder forwarder, IBlobLeaseManager leaseManager, IConfiguration configuration, ILogger<BlobChangeFeedProcessor> logger)
        {
            _forwarder = forwarder;
            _leaseManager = leaseManager;
            _logger = logger;
            _configs = configuration.GetSection("BlobChangeFeedTriggers").Get<List<ChangeFeedConfig>>() ?? new List<ChangeFeedConfig>();
            _instanceId = Environment.GetEnvironmentVariable("HOSTNAME") ?? Guid.NewGuid().ToString().Substring(0, 8);
            _leaseId = Guid.NewGuid().ToString();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"Starting {_configs.Count} Blob Change Feed Processors with InstanceId: [{_instanceId}]...");

            var tasks = new List<Task>();
            foreach (var config in _configs)
            {
                tasks.Add(MonitorChangeFeedWithLeaseAsync(config, stoppingToken));
            }

            await Task.WhenAll(tasks);
        }

        private async Task MonitorChangeFeedWithLeaseAsync(ChangeFeedConfig config, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(config.ConnectionString)) return;

            string leaseBlobName = $"changefeed-{config.TargetFunctionName}.checkpoint";

            while (!ct.IsCancellationRequested)
            {
                // 1. Try to acquire the lease
                if (await _leaseManager.TryAcquireLeaseAsync(_leaseId, leaseBlobName, ct))
                {
                    using var heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    var heartbeatTask = RunHeartbeatAsync(leaseBlobName, heartbeatCts.Token);

                    try
                    {
                        await RunProcessingLoopAsync(config, leaseBlobName, ct);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error in Change Feed processing loop. Releasing lease and retrying...");
                    }
                    finally
                    {
                        heartbeatCts.Cancel();
                        try { await heartbeatTask; } catch { }
                        try { await _leaseManager.ReleaseLeaseAsync(_leaseId, leaseBlobName, ct); } catch { }
                    }
                }

                // Wait before trying to acquire the lease again
                await Task.Delay(15000, ct);
            }
        }

        private async Task RunHeartbeatAsync(string leaseBlobName, CancellationToken ct)
        {
            _logger.LogInformation($"Starting heartbeat for lease on [{leaseBlobName}]. Interval: {_leaseManager.HeartbeatIntervalSeconds}s");
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(_leaseManager.HeartbeatIntervalSeconds), ct);
                    await _leaseManager.RenewLeaseAsync(_leaseId, leaseBlobName, ct);
                    _logger.LogDebug($"Lease [{leaseBlobName}] renewed.");
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to renew lease. Processor may lose exclusive access.");
                    break; 
                }
            }
        }

        private async Task RunProcessingLoopAsync(ChangeFeedConfig config, string leaseBlobName, CancellationToken ct)
        {
            var client = new BlobChangeFeedClient(config.ConnectionString);
            
            // 2. Load the last saved continuation token
            string? continuationToken = await _leaseManager.DownloadStateAsync(leaseBlobName, ct);
            
            // SECURITY/ROUSTNESS: Azure BlobChangeFeedClient requires continuationToken to be EXACTLY null for an initial read.
            // If we try to feed it string.Empty, the internal SDK throws a System.Text.Json.JsonException because string.Empty has no JSON tokens.
            if (string.IsNullOrWhiteSpace(continuationToken))
            {
                _logger.LogWarning($"Downloaded continuation token for lease [{leaseBlobName}] was empty or null. " +
                                  $"Resetting to null so the Azure SDK starts from the very beginning of the change feed safely.");
                continuationToken = null;
            }
            else
            {
                _logger.LogInformation($"Resuming Change Feed from saved token length: {continuationToken.Length}");
            }

            int eventsProcessedSinceLastCheckpoint = 0;

            while (!ct.IsCancellationRequested)
            {
                var pages = client.GetChangesAsync(continuationToken: continuationToken).AsPages();

                await foreach (var page in pages)
                {
                    foreach (var changeEvent in page.Values)
                    {
                        _logger.LogInformation($"Received Change Feed event: {changeEvent.EventType} on {changeEvent.Subject}");
                        
                        var payload = System.Text.Json.JsonSerializer.Serialize(changeEvent);
                        var success = await _forwarder.ForwardEventAsync(config.TargetFunctionName, payload);
                        
                        if (success)
                        {
                            eventsProcessedSinceLastCheckpoint++;
                            // Update the local token only on success
                            continuationToken = page.ContinuationToken; 
                        }
                        else
                        {
                            _logger.LogWarning($"Failed to forward event. Will NOT update checkpoint. Retrying event on next iteration.");
                            // If we fail one event, we stop this page to ensure we resume from the EXACT same spot.
                            goto PageEnd; 
                        }
                    }

                    // 3. Periodic Checkpoint (if any progress made)
                    if (eventsProcessedSinceLastCheckpoint > 0)
                    {
                        await _leaseManager.UploadStateAsync(_leaseId, leaseBlobName, continuationToken ?? string.Empty, ct);
                        eventsProcessedSinceLastCheckpoint = 0;
                        _logger.LogInformation("Checkpoint saved.");
                    }

                    if (page.Values.Count == 0 || continuationToken == null)
                    {
                        await Task.Delay(10000, ct);
                    }
                }
                PageEnd: ;
            }
        }
    }
}
