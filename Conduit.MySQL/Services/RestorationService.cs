using Conduit.MySQL;
using Conduit.MySQL.Models;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Conduit.MySQL.Services
{
    public class RestorationService: IRestorationService
    {
        private readonly ApplicationDatabase database;

        public RestorationService(ApplicationDatabase database)
        {
            this.database = database;
        }

        public async Task<List<PushDataFilterMatch>> GetPushDataFilterMatches(PushDataFilter pushDataFilter)
        {
            List<PushDataFilterMatch> results = new();

            await database.Connection.OpenAsync();

            using (var command = new MySqlCommand())
            {
                // SqlCommand calls have no way of handling "IN" arrays natively, it needs to be faked. Everyone does it
                var parameterNames = new string[pushDataFilter.FilterKeys.Count];
                for (int i = 0; i < pushDataFilter.FilterKeys.Count; i++)
                {
                    parameterNames[i] = string.Format("@h{0}", i);
                    command.Parameters.AddWithValue(parameterNames[i], pushDataFilter.FilterKeys[i]);
                }
                command.CommandText = string.Format("SELECT pushdata_hash, tx_hash, idx, ref_type FROM pushdata WHERE pushdata_hash IN ({0})", string.Join(",", parameterNames));
                command.Connection = database.Connection;

                using (var reader = await command.ExecuteReaderAsync())
                    while (await reader.ReadAsync())
                    {
                        PushDataFilterMatch match = new(new byte[32], new byte[32], reader.GetInt32(2), (Enums.TransactionReferenceType)reader.GetInt16(3));

                        // Read the bytes from the pushdata hash column.
                        int index = 0;
                        while (index < 32)
                        {
                            int bytesRead = (int)reader.GetBytes(0, index, match.PushDataHash, index, 32 - index);
                            index += bytesRead;
                        }

                        // Read the bytes from the transaction hash column.
                        index = 0;
                        while (index < 32)
                        {
                            int bytesRead = (int)reader.GetBytes(1, index, match.TransactionHash, index, 32 - index);
                            index += bytesRead;
                        }
                        results.Add(match);
                    }
            }

            return results;
        }
    }
}
