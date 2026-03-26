using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventManager.Sidecar
{
    public class SqlProcessor : IHostedService, IChangeFeedListener
    {
        private readonly IEventForwarder _forwarder;
        private readonly ILogger<SqlProcessor> _logger;

        public SqlProcessor(IEventForwarder forwarder, ILogger<SqlProcessor> logger)
        {
            _forwarder = forwarder;
            _logger = logger;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Starting SQL Server Change Tracking Processor...");
            // Implement standard SQL polling loop using SqlClient here
            await Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Stopping SQL Processor...");
            await Task.CompletedTask;
        }

        public Task StopAsync() => StopAsync(CancellationToken.None);
    }
}
