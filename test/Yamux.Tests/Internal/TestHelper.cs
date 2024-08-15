using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace Omnius.Yamux.Internal;

public class TestHelper : IDisposable
{
    private readonly ITestOutputHelper _output;

    private TcpListener? _listener;

    public TestHelper(ITestOutputHelper output)
    {
        _output = output;
    }

    public void Dispose()
    {
        if (_listener != null)
        {
            _listener.Stop();
            _listener = null;
        }
    }

    public async ValueTask<(Stream, Stream)> CreateStreamPair()
    {
        foreach (var port in Enumerable.Range(50000, 1000))
        {
            if (_listener != null)
            {
                _listener.Stop();
                _listener = null;
            }

            try
            {
                _listener = new TcpListener(IPAddress.Loopback, port);
                _listener.Start();

                var client = new TcpClient();
                client.Connect(IPAddress.Loopback, port);

                var server = _listener.AcceptTcpClient();

                return (client.GetStream(), server.GetStream());
            }
            catch (Exception e)
            {
                _output.WriteLine(e.ToString());
                await Task.Delay(100);
            }
        }

        throw new Exception();
    }

    public async ValueTask<(YamuxMuxer, YamuxMuxer)> CreateYamuxMuxerPair(ILogger logger)
    {
        return await this.CreateYamuxMuxerPair(TimeProvider.System, logger);
    }

    public async ValueTask<(YamuxMuxer, YamuxMuxer)> CreateYamuxMuxerPair(TimeProvider timeProvider, ILogger logger)
    {
        return await this.CreateYamuxMuxerPair(new YamuxConfig(), timeProvider, logger);
    }

    public async ValueTask<(YamuxMuxer, YamuxMuxer)> CreateYamuxMuxerPair(YamuxConfig yamuxConfig, ILogger logger)
    {
        return await this.CreateYamuxMuxerPair(yamuxConfig, yamuxConfig, TimeProvider.System, logger);
    }

    public async ValueTask<(YamuxMuxer, YamuxMuxer)> CreateYamuxMuxerPair(YamuxConfig yamuxConfig, TimeProvider timeProvider, ILogger logger)
    {
        return await this.CreateYamuxMuxerPair(yamuxConfig, yamuxConfig, timeProvider, logger);
    }

    public async ValueTask<(YamuxMuxer, YamuxMuxer)> CreateYamuxMuxerPair(YamuxConfig serverYamuxConfig, YamuxConfig clientYamuxConfig, TimeProvider timeProvider, ILogger logger)
    {
        var (client, server) = await this.CreateStreamPair();

        var serverYamuxMuxer = new YamuxMuxer(serverYamuxConfig, YamuxSessionType.Server, client, timeProvider, logger);
        var clientYamuxMuxer = new YamuxMuxer(clientYamuxConfig, YamuxSessionType.Client, server, timeProvider, logger);

        return (clientYamuxMuxer, serverYamuxMuxer);
    }
}
