using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EventManager.Sidecar
{
    public interface IBlobLeaseManager
    {
        int HeartbeatIntervalSeconds { get; }
        Task<bool> TryAcquireLeaseAsync(string leaseId, string blobName, CancellationToken ct);
        Task RenewLeaseAsync(string leaseId, string blobName, CancellationToken ct);
        Task ReleaseLeaseAsync(string leaseId, string blobName, CancellationToken ct);
        Task UploadStateAsync(string leaseId, string blobName, string state, CancellationToken ct);
        Task<string?> DownloadStateAsync(string blobName, CancellationToken ct);
    }

    public class BlobLeaseManager : IBlobLeaseManager
    {
        private readonly string _connectionString;
        private readonly string _containerName;
        private readonly int _leaseDurationSeconds;
        public int HeartbeatIntervalSeconds { get; }
        private readonly ILogger<BlobLeaseManager> _logger;

        public BlobLeaseManager(IConfiguration configuration, ILogger<BlobLeaseManager> logger)
        {
            _connectionString = configuration["LeaseConfiguration:ConnectionString"] ?? throw new ArgumentNullException("LeaseConfiguration:ConnectionString");
            _containerName = configuration["LeaseConfiguration:ContainerName"] ?? "sidecar-leases";
            _leaseDurationSeconds = configuration.GetValue<int>("LeaseConfiguration:LeaseDurationSeconds", 60);
            HeartbeatIntervalSeconds = configuration.GetValue<int>("LeaseConfiguration:HeartbeatIntervalSeconds", 15);
            _logger = logger;
        }

        private BlobContainerClient GetContainerClient() => new BlobContainerClient(_connectionString, _containerName);

        public async Task<bool> TryAcquireLeaseAsync(string leaseId, string blobName, CancellationToken ct)
        {
            var containerClient = GetContainerClient();
            await containerClient.CreateIfNotExistsAsync(cancellationToken: ct);

            var blobClient = containerClient.GetBlobClient(blobName);
            if (!await blobClient.ExistsAsync(cancellationToken: ct))
            {
                using var ms = new MemoryStream(Encoding.UTF8.GetBytes(string.Empty));
                await blobClient.UploadAsync(ms, overwrite: true, cancellationToken: ct);
            }

            var leaseClient = blobClient.GetBlobLeaseClient(leaseId);
            try
            {
                await leaseClient.AcquireAsync(TimeSpan.FromSeconds(_leaseDurationSeconds), cancellationToken: ct);
                _logger.LogInformation($"Acquired lease [{leaseId}] on blob [{blobName}] for {_leaseDurationSeconds}s.");
                return true;
            }
            catch (RequestFailedException ex) when (ex.Status == 409)
            {
                _logger.LogWarning($"Lease [{leaseId}] on blob [{blobName}] is already held by another instance.");
                return false;
            }
        }

        public async Task RenewLeaseAsync(string leaseId, string blobName, CancellationToken ct)
        {
            var leaseClient = GetContainerClient().GetBlobClient(blobName).GetBlobLeaseClient(leaseId);
            await leaseClient.RenewAsync(cancellationToken: ct);
        }

        public async Task ReleaseLeaseAsync(string leaseId, string blobName, CancellationToken ct)
        {
            var leaseClient = GetContainerClient().GetBlobClient(blobName).GetBlobLeaseClient(leaseId);
            await leaseClient.ReleaseAsync(cancellationToken: ct);
            _logger.LogInformation($"Released lease [{leaseId}] on blob [{blobName}].");
        }

        public async Task UploadStateAsync(string leaseId, string blobName, string state, CancellationToken ct)
        {
            var blobClient = GetContainerClient().GetBlobClient(blobName);
            using var ms = new MemoryStream(Encoding.UTF8.GetBytes(state));
            await blobClient.UploadAsync(ms, new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { LeaseId = leaseId }
            }, cancellationToken: ct);
        }

        public async Task<string?> DownloadStateAsync(string blobName, CancellationToken ct)
        {
            var blobClient = GetContainerClient().GetBlobClient(blobName);
            if (!await blobClient.ExistsAsync(cancellationToken: ct)) return null;

            var response = await blobClient.DownloadContentAsync(cancellationToken: ct);
            var resultBytes = response.Value.Content.ToArray();
            return resultBytes.Length == 0 ? string.Empty : System.Text.Encoding.UTF8.GetString(resultBytes);
        }
    }
}
