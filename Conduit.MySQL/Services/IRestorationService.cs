using Conduit.MySQL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Conduit.MySQL.Services
{
    public interface IRestorationService
    {
        Task<uint> GetTransactionHeight(byte[] transactionHash);
        uint GetTransactionHeightSync(byte[] transactionHash);
        Task<List<PushDataFilterMatch>> GetPushDataFilterMatches(PushDataFilter pushDataFilter);
    }
}
