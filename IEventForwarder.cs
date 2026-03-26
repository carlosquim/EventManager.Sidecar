using System.Net.Http;
using System.Threading.Tasks;

namespace EventManager.Sidecar
{
    public interface IEventForwarder
    {
        Task<bool> ForwardEventAsync(string functionName, string payload);
    }

    public class HttpEventForwarder : IEventForwarder
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<HttpEventForwarder> _logger;

        public HttpEventForwarder(HttpClient httpClient, ILogger<HttpEventForwarder> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        public async Task<bool> ForwardEventAsync(string functionName, string payload)
        {
            try
            {
                var content = new StringContent(payload, System.Text.Encoding.UTF8, "application/json");

                var response = await _httpClient.PostAsync($"api/sidecar/{functionName}", content);
                if (response.IsSuccessStatusCode)
                {
                    _logger.LogInformation($"Successfully forwarded event for [{functionName}] to HTTP endpoint.");
                    return true;
                }
                
                _logger.LogError($"HTTP forward for [{functionName}] failed with status: {response.StatusCode}");
                return false;
            }
            catch (HttpRequestException ex)
            {
                _logger.LogWarning($"Main App Container unreachable at [{_httpClient.BaseAddress}]. Event for [{functionName}] could not be forwarded yet. Ensure the main container is running. Quick Error: {ex.Message}");
                return false;
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, $"Critical failure forwarding event for [{functionName}] to HTTP.");
                return false;
            }
        }
    }

    public class EventDispatcher : IEventForwarder
    {
        private readonly HttpEventForwarder _httpForwarder;
        private readonly ServiceBusEventForwarder? _sbForwarder;
        private readonly string _mode;
        private readonly ILogger<EventDispatcher> _logger;

        public EventDispatcher(HttpEventForwarder httpForwarder, ILogger<EventDispatcher> logger, Microsoft.Extensions.Configuration.IConfiguration configuration, ServiceBusEventForwarder? sbForwarder = null)
        {
            _httpForwarder = httpForwarder;
            _sbForwarder = sbForwarder;
            _logger = logger;
            _mode = configuration["EventForwarderOptions:Mode"] ?? "Http";
        }

        public async Task<bool> ForwardEventAsync(string functionName, string payload)
        {
            if (_mode == "ServiceBus" && _sbForwarder != null)
            {
                return await _sbForwarder.ForwardEventAsync(functionName, payload);
            }

            return await _httpForwarder.ForwardEventAsync(functionName, payload);
        }
    }
}
