using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;


namespace Conduit.API.REST.Services
{
    public interface IHttpClientServiceImplementation
    {
        Task<byte[]> GetRawTransaction(string transactionHash);
    }
}
