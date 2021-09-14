using Conduit.MySQL.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Conduit.MySQL.Models
{
    public class PushDataFilterMatch
    {
        public byte[] PushDataHash { get; set; }
        public byte[] TransactionHash { get; set; }
        public int Index { get; set; }
        public TransactionReferenceType ReferenceType { get; set; }

        public PushDataFilterMatch(byte[] pushDataHash, byte[] transactionHash, int index, TransactionReferenceType referenceType)
        {
            PushDataHash = pushDataHash;
            TransactionHash = transactionHash;
            Index = index;
            ReferenceType = referenceType;
        }
    }
}
