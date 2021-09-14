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
            TransactionId = Convert.ToHexString(match.PushDataHash.Reverse().ToArray());
            Index = match.Index;
            ReferenceType = (int)match.ReferenceType;
        }

        public string PushDataHashHex { get; set; }

        public string TransactionId { get; set; }

        public int Index { get; set; }

        public int ReferenceType { get; set; }
    }
}
