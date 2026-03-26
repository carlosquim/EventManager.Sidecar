using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Azure;
using Azure.Messaging.ServiceBus;
using System;
using Polly;
using Polly.Extensions.Http;
using Azure.Monitor.OpenTelemetry.Exporter;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace EventManager.Sidecar
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Pre-read configuration to conditionally enable the WebJobs SDK before host building
            var preConfig = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true)
                .AddJsonFile("local.settings.json", optional: true)
                .AddEnvironmentVariables()
                .AddCommandLine(args)
                .Build();

            var builder = Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((context, config) =>
                {
                    config.AddJsonFile("local.settings.json", optional: true, reloadOnChange: true);
                    config.AddEnvironmentVariables(); // Ensure Env Vars win!
                });

            if (preConfig.GetValue<bool>("EnableWebJobsBlobTrigger", false))
            {
                builder.ConfigureWebJobs(b => b.AddAzureStorageBlobs());
            }

            builder.ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder.ConfigureKestrel(options => options.ListenAnyIP(8081));
                webBuilder.Configure(app =>
                {
                    app.Run(async context =>
                    {
                        if (context.Request.Path == "/metrics/cosmos-lag")
                        {
                            long totalLag = CosmosDbProcessor.TotalPendingChanges;
                            context.Response.ContentType = "application/json";
                            await context.Response.WriteAsync($"{{\"lag\": {totalLag}}}");
                        }
                        else
                        {
                            context.Response.StatusCode = 404;
                        }
                    });
                });
            });

            builder.ConfigureLogging((context, logging) =>
            {
                var aiConnectionString = context.Configuration["APPLICATIONINSIGHTS_CONNECTION_STRING"] ?? Environment.GetEnvironmentVariable("APPLICATIONINSIGHTS_CONNECTION_STRING");
                if (!string.IsNullOrEmpty(aiConnectionString))
                {
                    logging.AddOpenTelemetry(options =>
                    {
                        options.IncludeFormattedMessage = true;
                        options.IncludeScopes = true;
                        options.AddAzureMonitorLogExporter(amOptions => amOptions.ConnectionString = aiConnectionString);
                    });
                }
                else
                {
                    Console.WriteLine("Warning: APPLICATIONINSIGHTS_CONNECTION_STRING not found. Telemetry logging is disabled.");
                }
            });

            builder.ConfigureServices((context, services) =>
            {
                var config = context.Configuration;

                var aiConnectionString = config["APPLICATIONINSIGHTS_CONNECTION_STRING"] ?? Environment.GetEnvironmentVariable("APPLICATIONINSIGHTS_CONNECTION_STRING");
                if (!string.IsNullOrEmpty(aiConnectionString))
                {
                    services.AddOpenTelemetry()
                        .WithTracing(tracing => tracing.AddAzureMonitorTraceExporter(options => options.ConnectionString = aiConnectionString))
                        .WithMetrics(metrics => metrics.AddAzureMonitorMetricExporter(options => options.ConnectionString = aiConnectionString));
                }

                // Distributed Coordination: IBlobLeaseManager
                services.AddSingleton<IBlobLeaseManager, BlobLeaseManager>();

                // Service Bus: Optional Client Registration
                var sbConnectionString = config["ServiceBusOptions:ConnectionString"];
                if (!string.IsNullOrEmpty(sbConnectionString))
                {
                    services.AddAzureClients(clients =>
                    {
                        clients.AddServiceBusClient(sbConnectionString);
                    });
                    services.AddSingleton<ServiceBusEventForwarder>();
                }

                // Core Routing: Strategy-based EventDispatcher
                services.AddHttpClient<HttpEventForwarder>(client =>
                {
                    var targetUrl = config["MainAppTargetUrl"] ?? "http://localhost:80/";
                    client.BaseAddress = new Uri(targetUrl);
                })
                .AddPolicyHandler(HttpPolicyExtensions
                    .HandleTransientHttpError()
                    .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt))));

                services.AddSingleton<IEventForwarder, EventDispatcher>();

                // Event Processors: Register conditionally based on configuration to act as a Universal Addon
                if (config.GetValue<bool>("EnableCosmosDb", false)) 
                {
                    services.AddHostedService<CosmosDbProcessor>();
                }
                if (config.GetValue<bool>("EnableBlobStorage", false)) 
                {
                    services.AddHostedService<BlobStorageProcessor>();
                }
                if (config.GetValue<bool>("EnableEventGrid", false)) 
                {
                    services.AddHostedService<EventGridProcessor>();
                }
                if (config.GetValue<bool>("EnableBlobChangeFeed", false)) 
                {
                    services.AddHostedService<BlobChangeFeedProcessor>();
                }
                if (config.GetValue<bool>("EnableSql", false)) 
                {
                    services.AddHostedService<SqlProcessor>();
                }
            });

            var host = builder.Build();
            host.Run();
        }
    }
}
