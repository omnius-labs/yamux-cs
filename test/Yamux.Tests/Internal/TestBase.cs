using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace Omnius.Yamux.Internal;

public abstract class TestBase<T> where T : TestBase<T>
{
    private static ILoggerFactory _loggerFactory = LoggerFactory.Create(builder =>
    {
        builder
            .AddConsole()
            .AddDebug();
    });

    public TestBase(ITestOutputHelper output)
    {
        this.Logger = _loggerFactory.CreateLogger<T>();
        this.Output = new CustomOutput(output, this.Logger);
    }

    public ILogger<T> Logger { get; }
    public ITestOutputHelper Output { get; }

    private class CustomOutput : ITestOutputHelper
    {
        private readonly ITestOutputHelper _output;
        private readonly ILogger<T> _logger;

        public CustomOutput(ITestOutputHelper output, ILogger<T> logger)
        {
            _output = output;
            _logger = logger;
        }

        public void WriteLine(string message)
        {
            _output.WriteLine(message);
            _logger.LogInformation(message);
        }

        public void WriteLine(string format, params object[] args)
        {
            _output.WriteLine(format, args);
            _logger.LogInformation(format, args);
        }
    }
}