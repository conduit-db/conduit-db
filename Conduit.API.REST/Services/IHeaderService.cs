using System.Net.Sockets;
using System.Threading.Tasks;

namespace Conduit.API.REST.Services
{
    public interface IHeaderService
    {
        Task<(int, Socket)> GetHeaderSocketAsync(uint startHeight, uint headerCount);
    }
}
