// https://stackoverflow.com/questions/43424095/how-to-unit-test-with-ilogger-in-asp-net-core
// The third solution works the best in my opinion
using System;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace ConduitRaw.Test
{
    public class XunitLogger<T> : ILogger<T>, IDisposable
    {
        private ITestOutputHelper _output;

        public XunitLogger(ITestOutputHelper output)
        {
            _output = output;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception,
            Func<TState, Exception, string> formatter)
        {
            _output.WriteLine(state.ToString());
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            return this;
        }

        public void Dispose()
        {
        }
    }
}