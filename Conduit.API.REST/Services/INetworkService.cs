using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Conduit.API.REST.Services
{
    public interface INetworkService
    {
        Task<Socket> ConnectToConduitRaw();
    }
}
