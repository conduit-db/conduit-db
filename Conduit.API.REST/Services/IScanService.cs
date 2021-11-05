using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Conduit.API.REST.Services
{
    public interface IScanService
    {
        Task CreateFilter(byte[] filterData);
    }
}
