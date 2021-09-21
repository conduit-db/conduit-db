﻿using Conduit.MySQL.Enums;
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
        public byte[] SpendTransactionHash { get; set; }
        public int SpendInputIndex { get; set; }

        public PushDataFilterMatch(byte[] pushDataHash, byte[] transactionHash, int index, TransactionReferenceType referenceType, byte[] spendTransactionHash, int spendInputIndex)
        {
            PushDataHash = pushDataHash;
            TransactionHash = transactionHash;
            Index = index;
            ReferenceType = referenceType;
            SpendTransactionHash = spendTransactionHash;
            SpendInputIndex = spendInputIndex;
        }
    }
}
