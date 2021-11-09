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

        public uint GetTransactionHeightSync(byte[] transactionHash)
        {
            database.Connection.Open();

            using (var command = new MySqlCommand())
            {
                command.Parameters.AddWithValue("@tx_hash", transactionHash);
                command.CommandText = "SELECT tx_height FROM confirmed_transactions WHERE tx_hash=@tx_hash";
                command.Connection = database.Connection;

                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read())
                        return uint.MaxValue;
                    return reader.GetUInt32(0);
                }
            }
        }

        public async Task<uint> GetTransactionHeight(byte[] transactionHash)
        {
            await database.Connection.OpenAsync();

            using (var command = new MySqlCommand())
            {
                command.Parameters.AddWithValue("@tx_hash", transactionHash);
                command.CommandText = "SELECT tx_height FROM confirmed_transactions WHERE tx_hash=@tx_hash";
                command.Connection = database.Connection;

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (!await reader.ReadAsync())
                        return uint.MaxValue;
                    return reader.GetUInt32(0);
                }
            }
        }

        public async IAsyncEnumerable<PushDataFilterMatch> GetPushDataFilterMatches(PushDataFilter pushDataFilter)
        {
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
                command.CommandText = string.Format(
                    "SELECT PD.pushdata_hash, PD.tx_hash, PD.idx, PD.ref_type, IT.in_tx_hash, IT.in_idx, CT.tx_height FROM pushdata PD "+
                    "LEFT JOIN inputs_table IT ON PD.tx_hash=IT.out_tx_hash AND PD.idx=IT.out_idx AND PD.ref_type=0 "+
                    "LEFT JOIN confirmed_transactions CT ON CT.tx_hash=PD.tx_hash "+
                    "WHERE PD.pushdata_hash IN ({0})", string.Join(",", parameterNames));
                command.Connection = database.Connection;

                using (var reader = await command.ExecuteReaderAsync())
                    while (await reader.ReadAsync())
                    {
                        PushDataFilterMatch match = new(new byte[32], new byte[32], reader.GetUInt32(2), (Enums.TransactionReferenceType)reader.GetInt16(3), null,
                            await reader.IsDBNullAsync(5) ? uint.MaxValue : reader.GetUInt32(5), await reader.IsDBNullAsync(6) ? uint.MaxValue : reader.GetUInt32(6));

                        // Read the bytes from the pushdata hash column.
                        int index = 0;
                        while (index < 32)
                        {
                            int bytesRead = (int)reader.GetBytes(0, index, match.PushDataHash, index, 32 - index);
                            if (bytesRead == 0)
                                break;
                            index += bytesRead;
                        }

                        // Read the bytes from the transaction hash column.
                        index = 0;
                        while (index < 32)
                        {
                            int bytesRead = (int)reader.GetBytes(1, index, match.TransactionHash, index, 32 - index);
                            index += bytesRead;
                        }

                        if (!await reader.IsDBNullAsync(4))
                        {
                            match.SpendTransactionHash = new byte[32];
                            // Read the bytes from the spend transaction hash column.
                            index = 0;
                            while (index < 32)
                            {
                                int bytesRead = (int)reader.GetBytes(4, index, match.SpendTransactionHash, index, 32 - index);
                                index += bytesRead;
                            }
                        }

                        yield return match;
                    }
            }
        }
    }
}
