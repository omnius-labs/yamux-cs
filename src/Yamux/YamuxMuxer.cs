using System.Buffers;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NeoSmart.AsyncLock;
using Omnius.Yamux.Internal;

namespace Omnius.Yamux;

public enum YamuxSessionType
{
    Client,
    Server,
}

public class YamuxMuxer : IAsyncDisposable
{
    private readonly YamuxConfig _config;
    private readonly YamuxSessionType _sessionType;
    private readonly Stream _networkStream;
    private readonly ILogger _logger;

    private readonly ArrayPool<byte> _bytesPool = ArrayPool<byte>.Shared;

    private readonly Dictionary<uint, YamuxStream> _streams = new();
    private readonly AsyncLock _streamLock = new();

    private uint _nextStreamId;
    private readonly AsyncLock _connectLock = new();

    private readonly SemaphoreSlim _sendSemaphore = new(1);

    private readonly Channel<YamuxStream> _acceptedStreams = Channel.CreateUnbounded<YamuxStream>();

    private uint _pingId;
    private readonly Dictionary<uint, TaskCompletionSource> _pingTasks = new();
    private readonly SemaphoreSlim _pingAckSemaphore = new(1);
    private object _pingLock = new();

    private readonly Task _receiveTask;
    private Task? _keepAliveTask;

    private GoAwayCode _remoteGoAwayCode = GoAwayCode.None;
    private GoAwayCode _localGoAwayCode = GoAwayCode.None;
    private YamuxErrorCode _shutdownErrorCode;

    private int _closed = 0;

    public YamuxMuxer(YamuxConfig config, YamuxSessionType sessionType, Stream stream, ILogger logger)
    {
        _config = config;
        _config.Verify();
        _sessionType = sessionType;
        _networkStream = stream;
        _logger = logger;

        if (_sessionType == YamuxSessionType.Client) _nextStreamId = 1;
        else _nextStreamId = 2;

        _receiveTask = this.ReceiveLoop();

        if (config.EnableKeepAlive)
        {
            _keepAliveTask = this.KeepAliveLoop();
        }
    }

    public YamuxConfig Config => _config;

    public async ValueTask DisposeAsync()
    {
        await this.CloseAsync();

        _sendSemaphore.Dispose();
    }

    public async ValueTask<YamuxStream> ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (_shutdownErrorCode != YamuxErrorCode.None) throw new YamuxException(_shutdownErrorCode);
        if (_remoteGoAwayCode != GoAwayCode.None) throw new YamuxException(YamuxErrorCode.RemoteGoAway);

        using (await _connectLock.LockAsync(cancellationToken))
        {
            if (_nextStreamId >= uint.MaxValue - 1) throw new YamuxException(YamuxErrorCode.StreamsExhausted);
            var streamId = _nextStreamId += 2;

            YamuxStream stream;

            using (await _streamLock.LockAsync(cancellationToken))
            {
                stream = new YamuxStream(this, streamId, StreamState.Init, _bytesPool, _logger);
                _streams.Add(streamId, stream);
            }

            await stream.SendWindowUpdateAsync(cancellationToken);
            return stream;
        }
    }

    public async ValueTask<YamuxStream> AcceptAsync(CancellationToken cancellationToken = default)
    {
        if (_shutdownErrorCode != YamuxErrorCode.None) throw new YamuxException(_shutdownErrorCode);
        if (_remoteGoAwayCode != GoAwayCode.None) throw new YamuxException(YamuxErrorCode.RemoteGoAway);

        var stream = await _acceptedStreams.Reader.ReadAsync(cancellationToken);
        await stream.SendWindowUpdateAsync(cancellationToken);
        return stream;
    }

    internal void RemoveStream(uint id)
    {
        using (_streamLock.Lock())
        {
            _streams.Remove(id);
        }
    }

    public async ValueTask CloseAsync()
    {
        if (Interlocked.CompareExchange(ref _closed, 1, 0) != 0) return;

        _localGoAwayCode = GoAwayCode.Normal;

        var header = new Header(MessageType.GoAway, MessageFlag.None, 0, (uint)GoAwayCode.Normal);
        await this.SendFrameAsync(header, ReadOnlyMemory<byte>.Empty);

        _acceptedStreams.Writer.Complete();

        _networkStream.Close();

        foreach (var stream in _streams.Values)
        {
            stream.ForceClose();
        }

        await _receiveTask;
        if (_keepAliveTask != null) await _keepAliveTask;
    }

    private async Task KeepAliveLoop(CancellationToken cancellationToken = default)
    {
        await Task.Delay(1, cancellationToken).ConfigureAwait(false);

        try
        {
            for (; ; )
            {
                await Task.Delay(_config.KeepAliveInterval, cancellationToken);
                await this.PingAsync(cancellationToken);
            }
        }
        catch (YamuxException e)
        {
            await this.ExitAsync(e.ErrorCode);
        }
    }

    private async ValueTask PingAsync(CancellationToken cancellationToken = default)
    {
        uint pingId;
        var pingTask = new TaskCompletionSource();

        lock (_pingLock)
        {
            pingId = _pingId++;
            _pingTasks.Add(pingId, pingTask);
        }

        var header = new Header(MessageType.Ping, MessageFlag.SYN, 0, pingId);
        await this.SendFrameAsync(header, ReadOnlyMemory<byte>.Empty, cancellationToken);

        var timeout = Task.Delay(_config.PingTimeout, cancellationToken);

        var completedTask = await Task.WhenAny(pingTask.Task, timeout);

        lock (_pingLock)
        {
            _pingTasks.Remove(pingId);
        }

        if (completedTask == timeout)
        {
            throw new YamuxException(YamuxErrorCode.Timeout);
        }
    }

    internal async ValueTask SendFrameAsync(Header header, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        await Task.Delay(1, cancellationToken).ConfigureAwait(false);

        await _sendSemaphore.WaitAsync(cancellationToken);

        try
        {
            await _networkStream.WriteAsync(header.GetBytes(), cancellationToken);

            if (payload.Length > 0)
            {
                await _networkStream.WriteAsync(payload, cancellationToken);
            }

            await _networkStream.FlushAsync(cancellationToken);
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "yamux: failed to send frame");
            throw new YamuxException(YamuxErrorCode.ConnectionSendError);
        }
        finally
        {
            _sendSemaphore.Release();
        }
    }

    private async Task ReceiveLoop(CancellationToken cancellationToken = default)
    {
        try
        {
            for (; ; )
            {
                var header = await this.ReceiveHeaderAsync(cancellationToken);
                if (header == null) break;

                if (header.Version != Constants.PROTO_VERSION)
                {
                    throw new YamuxException(YamuxErrorCode.InvalidVersion);
                }

                switch (header.Type)
                {
                    case MessageType.Data:
                    case MessageType.WindowUpdate:
                        await this.HandleStreamMessageAsync(header, cancellationToken);
                        break;
                    case MessageType.Ping:
                        await this.HandlePingAsync(header, cancellationToken);
                        break;
                    case MessageType.GoAway:
                        await this.HandleGoAwayAsync(header, cancellationToken);
                        break;
                    default:
                        throw new YamuxException(YamuxErrorCode.InvalidFrameType);
                }
            }
        }
        catch (YamuxException e)
        {
            await this.ExitAsync(e.ErrorCode);
        }
    }

    private async ValueTask<Header?> ReceiveHeaderAsync(CancellationToken cancellationToken = default)
    {
        var buffer = new byte[Constants.HeaderSize.TOTAL];

        try
        {
            int remain = Constants.HeaderSize.TOTAL;
            while (remain > 0)
            {
                var read = await _networkStream.ReadAsync(buffer, Constants.HeaderSize.TOTAL - remain, remain, cancellationToken);
                if (read == 0) return null;
                remain -= read;
            }
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "yamux: failed to read header");
            throw new YamuxException(YamuxErrorCode.ConnectionReceiveError);
        }

        var header = new Header(buffer);
        return header;
    }

    private async ValueTask HandleStreamMessageAsync(Header header, CancellationToken cancellationToken = default)
    {
        if (header.Flags.HasFlag(MessageFlag.SYN))
        {
            await this.IncomingStreamAsync(header.StreamId, cancellationToken);
        }

        if (!_streams.TryGetValue(header.StreamId, out var stream))
        {
            if (header.Type == MessageType.Data && header.Length > 0)
            {
                _logger.LogWarning("yamux: discarding data for stream: {0}", header.StreamId);

                var buffer = _bytesPool.Rent(4096);
                try
                {
                    var remain = (int)header.Length;
                    while (remain > 0)
                    {
                        var readLength = await _networkStream.ReadAsync(buffer, 0, Math.Min(buffer.Length, remain), cancellationToken);
                        if (readLength == 0) throw new IOException("Stream closed");
                        remain -= readLength;
                    }
                }
                catch (Exception e)
                {
                    _logger.LogWarning(e, "yamux: failed to discard data: {0}", header.StreamId);
                }
                finally
                {
                    _bytesPool.Return(buffer);
                }
            }
            else
            {
                _logger.LogWarning("yamux: frame for missing stream: {0}", header);
            }

            return;
        }

        try
        {
            if (header.Type == MessageType.WindowUpdate)
            {
                stream.AddSendWindow(header);
            }
            else if (header.Type == MessageType.Data)
            {
                await stream.EnqueueReadBytesAsync(header, _networkStream, cancellationToken);
            }
        }
        catch (YamuxException e)
        {
            _logger.LogWarning(e, "yamux: failed to send go away");
            var header2 = new Header(MessageType.GoAway, MessageFlag.None, 0, (uint)GoAwayCode.ProtocolError);
            await this.SendFrameAsync(header2, ReadOnlyMemory<byte>.Empty, cancellationToken);
        }
    }

    private async ValueTask IncomingStreamAsync(uint id, CancellationToken cancellationToken = default)
    {
        if (_localGoAwayCode != GoAwayCode.None)
        {
            var header = new Header(MessageType.WindowUpdate, MessageFlag.RST, id, 0);
            await this.SendFrameAsync(header, ReadOnlyMemory<byte>.Empty, cancellationToken);
            return;
        }

        var stream = new YamuxStream(this, id, StreamState.SYNReceived, _bytesPool, _logger);

        using (await _streamLock.LockAsync(cancellationToken))
        {
            if (_streams.ContainsKey(id))
            {
                var header = new Header(MessageType.GoAway, MessageFlag.None, 0, (uint)GoAwayCode.ProtocolError);
                await this.SendFrameAsync(header, ReadOnlyMemory<byte>.Empty, cancellationToken);
                throw new YamuxException(YamuxErrorCode.DuplicateStreamId);
            }

            if (_acceptedStreams.Reader.Count >= _config.MaxAcceptBacklog)
            {
                var header = new Header(MessageType.WindowUpdate, MessageFlag.RST, id, 0);
                await this.SendFrameAsync(header, ReadOnlyMemory<byte>.Empty, cancellationToken);
                return;
            }

            _streams.Add(id, stream);
            await _acceptedStreams.Writer.WriteAsync(stream, cancellationToken);
        }
    }

    private async ValueTask HandlePingAsync(Header header, CancellationToken cancellationToken = default)
    {
        var pingId = header.Length;

        if (header.Flags.HasFlag(MessageFlag.SYN))
        {
            await _pingAckSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            _ = Task.Run(async () =>
            {
                try
                {
                    var header2 = new Header(MessageType.Ping, MessageFlag.ACK, 0, pingId);
                    await this.SendFrameAsync(header2, ReadOnlyMemory<byte>.Empty, cancellationToken);
                }
                finally
                {
                    _pingAckSemaphore.Release();
                }
            });
        }

        if (header.Flags.HasFlag(MessageFlag.ACK))
        {
            lock (_pingLock)
            {
                if (_pingTasks.TryGetValue(pingId, out var task))
                {
                    task.SetResult();
                }
            }
        }
    }

    private async ValueTask HandleGoAwayAsync(Header header, CancellationToken cancellationToken = default)
    {
        var code = header.Length;

        switch (code)
        {
            case (uint)GoAwayCode.Normal:
                _remoteGoAwayCode = GoAwayCode.Normal;
                break;
            case (uint)GoAwayCode.ProtocolError:
                _logger.LogWarning("yamux: received protocol error go away");
                throw new YamuxException(YamuxErrorCode.ProtocolError);
            case (uint)GoAwayCode.InternalError:
                _logger.LogWarning("yamux: received internal error go away");
                throw new YamuxException(YamuxErrorCode.InternalError);
            default:
                _logger.LogWarning("yamux: received unexpected go away code: {0}", code);
                throw new YamuxException(YamuxErrorCode.Unexpected);
        }
    }

    private async ValueTask ExitAsync(YamuxErrorCode errorCode)
    {
        if (_shutdownErrorCode != YamuxErrorCode.None) return;
        _shutdownErrorCode = errorCode;
        await this.CloseAsync();
    }
}
