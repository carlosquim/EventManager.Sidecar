using System.Threading;
using System.Threading.Tasks;

namespace EventManager.Sidecar
{
    public interface IChangeFeedListener
    {
        Task StartAsync(CancellationToken cancellationToken);
        Task StopAsync();
    }
}
