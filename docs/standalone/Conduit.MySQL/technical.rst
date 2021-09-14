Technical information
=====================

MySqlConnector
--------------

`MySqlConnector <https://mysqlconnector.net/>`__ was chosen over the standard MySQL offering from
Oracle, `MySQL.Data <https://dev.mysql.com/doc/connector-net/en/>`__, for a range of
reasons:

* MySqlConnector claims to have proper async support and that MySQL.Data just wraps synchronous
  IO.
* MySqlConnector claims to have fixed bugs that MySQL.Data does not.
* MySqlConnector is MIT licensed and MySQL.Data is GPLv2 licensed.
* MySqlConnector does not have extra dependencies that MySQL.Data has. MySqlConnector
  only seems to depend on .NET System libraries
  (`see the Nuget page <https://www.nuget.org/packages/MySqlConnector>`__).
  MySQL.Data depends on BouncyCastle, Google.Protobuf and a
  range of K4ox packages (`see the Nuget page <https://www.nuget.org/packages/MySql.Data/>`__).

It is a `ADO.NET <https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/>`__ data provider.

ADO.NET domain knowledge
~~~~~~~~~~~~~~~~~~~~~~~~

Like all modern technologies ADO.NET has quirks. This section is intended to document any we
find.

Handling array parameters for SQL ``IN`` usage
______________________________________________

No database providers have clean support for providing an array of values directly to an ``IN``
SQL clause. In ADO.NET the programmer has to generate the placeholder parameter names in the
statement, and set each and every one of the array entries to it's placeholder parameter name.

There is a way to do this `with SQL Server <https://stackoverflow.com/a/10779567>`__ using
it's `Table-Valued Parameters <https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql/table-valued-parameters>`__.
However the same workaround shown below is required for using SQLite from Python, so it has
precedence as a cross-platform standard approach.

.. code-block:: c#

    using (var command = new MySqlCommand())
    {
        var parameterNames = new string[pushDataFilter.FilterKeys.Count];
        for (int i = 0; i < pushDataFilter.FilterKeys.Count; i++)
        {
            parameterNames[i] = string.Format("@h{0}", i);
            command.Parameters.AddWithValue(parameterNames[i], pushDataFilter.FilterKeys[i]);
        }
        command.CommandText = string.Format("SELECT pushdata_hash, tx_hash, idx, ref_type "
            "FROM pushdata WHERE pushdata_hash IN ({0})", string.Join(",", parameterNames));
        command.Connection = database.Connection;

        using (var reader = await command.ExecuteReaderAsync())
            while (await reader.ReadAsync())
            {
                PushDataFilterMatch match = new(new byte[32], new byte[32], reader.GetInt32(2),
                    (Enums.TransactionReferenceType)reader.GetInt16(3));

                // Read the bytes from the pushdata hash column.
                int index = 0;
                while (index < 32)
                {
                    int bytesRead = (int)reader.GetBytes(0, index, match.PushDataHash, index,
                        32 - index);
                    index += bytesRead;
                }

                // Read the bytes from the transaction hash column.
                index = 0;
                while (index < 32)
                {
                    int bytesRead = (int)reader.GetBytes(1, index, match.TransactionHash, index,
                        32 - index);
                    index += bytesRead;
                }
                results.Add(match);
            }
    }
