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
        public uint Index { get; set; }
        public TransactionReferenceType ReferenceType { get; set; }
        public byte[] SpendTransactionHash { get; set; }
        public uint SpendInputIndex { get; set; }

        /// <summary>
        /// This will be the height of the block that the transaction is currently in if the transaction is in a block.
        /// If the transaction is not in a block then it will be the maximum value of an unsigned integer (0xFFFFFFFF).
        /// </summary>
        public uint BlockHeight { get; set; }

        public PushDataFilterMatch(byte[] pushDataHash, byte[] transactionHash, uint index, TransactionReferenceType referenceType, byte[] spendTransactionHash, uint spendInputIndex, uint blockHeight)
        {
            PushDataHash = pushDataHash;
            TransactionHash = transactionHash;
            Index = index;
            ReferenceType = referenceType;
            SpendTransactionHash = spendTransactionHash;
            SpendInputIndex = spendInputIndex;
            BlockHeight = blockHeight;
        }
    }
}
