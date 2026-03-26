using System.Text.Json;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EventManager.Sidecar
{
    public class ServiceBusEventForwarder : IEventForwarder
    {
        private readonly ServiceBusClient _client;
        private readonly string _queueOrTopicName;
        private readonly ILogger<ServiceBusEventForwarder> _logger;

        public ServiceBusEventForwarder(ServiceBusClient client, IConfiguration configuration, ILogger<ServiceBusEventForwarder> logger)
        {
            _client = client;
            _queueOrTopicName = configuration["ServiceBusOptions:QueueOrTopicName"] ?? throw new System.ArgumentNullException("ServiceBusOptions:QueueOrTopicName");
            _logger = logger;
        }

        public async Task<bool> ForwardEventAsync(string functionName, string payload)
        {
            try
            {
                var sender = _client.CreateSender(_queueOrTopicName);
                
                // Wrap the payload in a standard wrapper if needed, or send as is
                // We'll add the functionName as a property for filtering/routing
                var message = new ServiceBusMessage(payload);
                message.ApplicationProperties.Add("TargetFunction", functionName);
                message.ApplicationProperties.Add("Source", "EventManager.Sidecar");

                await sender.SendMessageAsync(message);
                
                _logger.LogInformation($"Successfully sent event for [{functionName}] to Service Bus [{_queueOrTopicName}].");
                return true;
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, $"Failed to send event for [{functionName}] to Service Bus.");
                return false;
            }
        }
    }
}
