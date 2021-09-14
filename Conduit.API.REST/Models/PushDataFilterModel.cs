using Conduit.MySQL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

namespace Conduit.API.REST.Models
{
    public class PushDataFilterModel
    {
        public List<string> FilterKeys { get; set; }

        [JsonIgnore]
        public PushDataFilter PushDataFilter => new()
        {
            FilterKeys = FilterKeys.Select(s => Convert.FromHexString(s)).ToList()
        };
    }
}
