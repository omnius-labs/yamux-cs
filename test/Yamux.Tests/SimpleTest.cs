using System.Net;
using System.Net.Sockets;
using Xunit;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Omnius.Yamux;

public class SampleTest
{
    private readonly ILogger<SampleTest> _logger;

    public SampleTest()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddConsole()
                .AddDebug();
        });

        _logger = loggerFactory.CreateLogger<SampleTest>();
    }

    [Fact]
    public async Task YamuxTest()
    {
        var listener = new TcpListener(IPAddress.Loopback, 50000);
        listener.Start();

        var client = new TcpClient();
        client.Connect(IPAddress.Loopback, 50000);

        var server = listener.AcceptTcpClient();

        var serverYamuxConfig = new YamuxConfig();
        var serverYamuxMuxer = new YamuxMuxer(serverYamuxConfig, YamuxSessionType.Server, server.GetStream(), _logger);

        var clientYamuxConfig = new YamuxConfig();
        var clientYamuxMuxer = new YamuxMuxer(clientYamuxConfig, YamuxSessionType.Client, client.GetStream(), _logger);

        var caseList = new List<int>();
        caseList.AddRange(Enumerable.Range(1, 4));
        caseList.AddRange([100, 1000, 10000]);
        caseList.AddRange([1024 * 1024]);
        caseList.AddRange([1024 * 1024 * 8]);

        var random = new Random();

        foreach (var bufferSize in caseList)
        {
            var acceptTask = serverYamuxMuxer.AcceptAsync().AsTask();
            await using var clientStream = await clientYamuxMuxer.ConnectAsync();
            await using var serverStream = await acceptTask;

            var buffer1 = new byte[bufferSize];
            var buffer2 = new byte[bufferSize];

            random.NextBytes(buffer1);

            using var cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(3000));

            var sb = Stopwatch.StartNew();

            var writeTask = clientStream.WriteAsync(buffer1, cancellationTokenSource.Token).AsTask();
            var readTask = Task.Run(async () =>
            {
                int remain = buffer2.Length;
                while (remain > 0)
                {
                    int readLength = await serverStream.ReadAsync(buffer2.AsMemory(buffer2.Length - remain, remain), cancellationTokenSource.Token);
                    remain -= readLength;
                }
            });
            await Task.WhenAll(writeTask, readTask);

            Assert.Equal(buffer1, buffer2);

            Debug.WriteLine($"RandomSendAndReceiveTest ({bufferSize}), time: {sb.ElapsedMilliseconds}/ms");

            await clientStream.DisposeAsync();
            await serverStream.DisposeAsync();
        }

        var serverDisposeTask = serverYamuxMuxer.DisposeAsync().AsTask();
        var clientDisposingTask = clientYamuxMuxer.DisposeAsync().AsTask();
        await Task.WhenAll(serverDisposeTask, clientDisposingTask);
    }
}
