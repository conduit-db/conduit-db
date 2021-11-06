using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Conduit.API.REST.Services
{
    public interface IHeaderService
    {
        Task<(int, Socket)> GetHeaderSocketAsync(uint startHeight, uint headerCount, CancellationToken externalToken = default);
        Task<byte[][]> GetHeaderSocket2Async(uint startHeight, uint headerCount, int timeoutMs = 0, CancellationToken externalToken = default);
        Task<(uint, byte[])> GetTipAsync(CancellationToken externalToken = default);
    }
}
