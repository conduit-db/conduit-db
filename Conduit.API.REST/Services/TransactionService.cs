using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Conduit.API.REST.Services;
using Conduit.API.REST.Classes;
using Microsoft.VisualBasic;

namespace Conduit.MySQL.Services
{
    public class TransactionService : ITransactionService
    {
        private IHttpClientServiceImplementation _httpClient;
        public TransactionService(IHttpClientServiceImplementation httpClient)
        {
            _httpClient = httpClient;
        }
    
        public async Task<byte[]> GetTransactionBytes(byte[] transactionHash)
        {
            // TODO Get the transaction data as a stream.
            var transactionHashHex = Util.HashToHexStr(transactionHash);
            Debug.WriteLine($"transactionHashHex: {transactionHashHex}");
            var result = await _httpClient.GetRawTransaction(transactionHashHex);
            return result;
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
