using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Conduit.MySQL.Services
{
    public class TransactionService : ITransactionService
    {
        public TransactionService()
        {
        }

        public async Task<(Stream, int)> GetTransactionBytes(byte[] transactionHash)
        {
            // TODO Get the transaction data as a stream.
            var stream = new MemoryStream(transactionHash);
            return (stream, transactionHash.Length);
        }

        public async Task<(Stream, int)> GetProofBytes(byte[] transactionHash, bool withTransaction)
        {
            // TODO Get the proof data.
            // TODO Get the transaction data.
            // TODO Pack in TSC format. It should still be possible to stream the transaction mid-format.
            var stream = new MemoryStream(transactionHash);
            return (stream, transactionHash.Length);
        }

        public async Task<(Stream, int)> GetProofJSON(byte[] transactionHash, bool withTransaction)
        {
            // TODO Get the proof data.
            // TODO Get the transaction data.
            // TODO Pack in TSC format. It should still be possible to stream the transaction.
            var stream = new MemoryStream(transactionHash);
            return (stream, transactionHash.Length);
        }
    }
}
