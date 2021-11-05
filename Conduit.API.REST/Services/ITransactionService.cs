using Conduit.MySQL.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Conduit.MySQL.Services
{
    public interface ITransactionService
    {
        Task<(Stream, int)> GetTransactionBytes(byte[] transactionHash);
        Task<(Stream, int)> GetProofBytes(byte[] transactionHash, bool withTransaction);
        Task<(Stream, int)> GetProofJSON(byte[] transactionHash, bool withTransaction);
    }
}
