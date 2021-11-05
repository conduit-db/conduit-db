using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Conduit.API.REST.Classes
{
    public static class Util
    {
        // Boilerplate Microsoft example (minor modifications).
        // https://docs.microsoft.com/en-us/dotnet/standard/io/pipelines#pipe-basic-usage
        public static async Task FillPipeAsync(Socket socket, PipeWriter writer, ILogger logger, CancellationToken token, int fixedLength=0)
        {
            const int minimumBufferSize = 40;
            int totalBytesRead = 0;

            while (fixedLength == 0 || totalBytesRead < fixedLength)
            {
                // Allocate at least the minimum buffer size from the PipeWriter.
                Memory<byte> memory = writer.GetMemory(minimumBufferSize);
                try
                {
                    int bytesRead = await socket.ReceiveAsync(memory, SocketFlags.None, token);
                    if (bytesRead == 0)
                    {
                        break;
                    }
                    if (fixedLength > 0)
                    {
                        if (totalBytesRead + bytesRead > fixedLength)
                            bytesRead = fixedLength - totalBytesRead;
                        totalBytesRead += bytesRead;
                    }
                    // Tell the PipeWriter how much was read from the Socket that we use.
                    writer.Advance(bytesRead);
                }
                catch (OperationCanceledException)
                {
                    break; 
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Exception piping socket");
                    break;
                }

                // Make the data available to the PipeReader.
                FlushResult result = await writer.FlushAsync();
                if (result.IsCompleted)
                {
                    break;
                }
            }
        }
    }
}
