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
        Task<List<PushDataFilterMatch>> GetPushDataFilterMatches(PushDataFilter pushDataFilter);
    }
}
