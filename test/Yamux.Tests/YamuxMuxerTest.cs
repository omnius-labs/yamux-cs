using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Omnius.Yamux.Internal;
using Xunit;
using Xunit.Abstractions;

namespace Omnius.Yamux;

public class YamuxMuxerTest : TestBase<YamuxMuxerTest>
{
    private readonly TestHelper _testHelper;

    public YamuxMuxerTest(ITestOutputHelper output) : base(output)
    {
        _testHelper = new TestHelper(output);
    }

    [Fact]
    public async Task RandomTest()
    {
        var (client, server) = await _testHelper.CreateYamuxMuxerPair(this.Logger);

        await using (client)
        await using (server)
        {
            var caseList = new List<int>();
            caseList.AddRange(Enumerable.Range(1, 4));
            caseList.AddRange([100, 1000, 10000]);
            caseList.AddRange([1024 * 1024]);
            caseList.AddRange([1024 * 1024 * 8]);

            var random = new Random();

            foreach (var bufferSize in caseList)
            {
                var acceptTask = server.AcceptStreamAsync().AsTask();

                await using (var clientStream = await client.ConnectStreamAsync())
                await using (var serverStream = await acceptTask)
                {
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

                    this.Output.WriteLine($"RandomSendAndReceiveTest ({bufferSize}), time: {sb.ElapsedMilliseconds}/ms");
                }
            }
        }
    }

    [Fact]
    public async Task PingTest()
    {
        var (client, server) = await _testHelper.CreateYamuxMuxerPair(this.Logger);

        await using (client)
        await using (server)
        {
            var clientRTT = await client.PingAsync();
            Assert.True(clientRTT > TimeSpan.Zero);

            var serverRTT = await server.PingAsync();
            Assert.True(serverRTT > TimeSpan.Zero);
        }
    }

    [Fact]
    public async Task PingTimeoutTest()
    {
        var yamuxConfig = new YamuxConfig() { PingTimeout = TimeSpan.FromSeconds(0) };
        var timeProvider = new FakeTimeProvider(DateTimeOffset.Parse("2020-01-01T00:00:00Z", CultureInfo.InvariantCulture)) { AutoAdvanceAmount = TimeSpan.FromSeconds(10) };

        var (client, server) = await _testHelper.CreateYamuxMuxerPair(yamuxConfig, timeProvider, this.Logger);

        await using (client)
        await using (server)
        {
            await Assert.ThrowsAsync<TimeoutException>(async () =>
            {
                _ = await client.PingAsync();
            });

            await Assert.ThrowsAsync<TimeoutException>(async () =>
            {
                _ = await server.PingAsync();
            });
        }
    }

    [Fact]
    public async Task AcceptTest()
    {
        var (client, server) = await _testHelper.CreateYamuxMuxerPair(this.Logger);

        await using (client)
        await using (server)
        {
            Assert.Equal(0, client.StreamCount);
            Assert.Equal(0, server.StreamCount);

            var tasks = new List<Task>
            {
                Task.Run(async () =>
                {
                    var stream = await server.AcceptStreamAsync();
                    Assert.Equal((uint)1, stream.StreamId);
                    await stream.DisposeAsync();
                }),
                Task.Run(async () =>
                {
                    var stream = await client.AcceptStreamAsync();
                    Assert.Equal((uint)2, stream.StreamId);
                    await stream.DisposeAsync();
                }),
                Task.Run(async () =>
                {
                    var stream = await server.ConnectStreamAsync();
                    Assert.Equal((uint)2, stream.StreamId);
                    await stream.DisposeAsync();
                }),
                Task.Run(async () =>
                {
                    var stream = await client.ConnectStreamAsync();
                    Assert.Equal((uint)1, stream.StreamId);
                    await stream.DisposeAsync();
                })
            };

            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
            var completedTask = await Task.WhenAny(Task.WhenAll(tasks), timeoutTask);
            if (completedTask == timeoutTask)
            {
                throw new TimeoutException("timeout");
            }

            await Task.WhenAll(tasks);
        }
    }

    [Fact]
    public async Task OpenStreamTimeoutTest()
    {
        var (client, server) = await _testHelper.CreateYamuxMuxerPair(this.Logger);

        await using (client)
        await using (server)
        {
            await Assert.ThrowsAsync<TaskCanceledException>(async () =>
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                var stream = await server.ConnectStreamAsync(cts.Token);
            });
        }
    }

    [Fact]
    public async Task CloseTimeoutTest()
    {
        var yamuxConfig = new YamuxConfig() { StreamCloseTimeout = TimeSpan.FromSeconds(0) };
        var (client, server) = await _testHelper.CreateYamuxMuxerPair(yamuxConfig, this.Logger);

        await using (client)
        await using (server)
        {
            Assert.Equal(0, client.StreamCount);
            Assert.Equal(0, server.StreamCount);

            Stream? clientStream = null;
            var tasks = new List<Task>
            {
                Task.Run(async () =>
                {
                    clientStream = await client.ConnectStreamAsync();
                }),
                Task.Run(async () =>
                {
                    var stream = await server.AcceptStreamAsync();
                    await stream.CloseAsync();
                })
            };

            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
            var completedTask = await Task.WhenAny(Task.WhenAll(tasks), timeoutTask);
            if (completedTask == timeoutTask)
            {
                throw new TimeoutException("timeout");
            }

            await Task.WhenAll(tasks);

            await Task.Delay(TimeSpan.FromSeconds(5));

            Assert.Equal(0, server.StreamCount);
            Assert.Equal(0, client.StreamCount);

            var exception = await Assert.ThrowsAsync<YamuxException>(async () =>
            {
                await clientStream!.WriteAsync("Hello"u8.ToArray());
            });
        }
    }
}
