using Conduit.MySQL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Conduit.API.REST.Models
{
    public class PushDataMatchModel
    {
        public PushDataMatchModel(PushDataFilterMatch match)
        {
            PushDataHashHex = Convert.ToHexString(match.PushDataHash);
            TransactionId = Convert.ToHexString(match.TransactionHash.Reverse().ToArray());
            Index = match.Index;
            ReferenceType = (int)match.ReferenceType;
            SpendTransactionId = match.SpendTransactionHash == null ? null : Convert.ToHexString(match.SpendTransactionHash.Reverse().ToArray());
            SpendInputIndex = match.SpendInputIndex;
        }

        public string PushDataHashHex { get; set; }

        public string TransactionId { get; set; }

        public int Index { get; set; }

        public int ReferenceType { get; set; }

        public string SpendTransactionId { get; set; }

        public int SpendInputIndex { get; set; }
    }
}
