using System.Buffers;
using Microsoft.Extensions.Logging;
using NeoSmart.AsyncLock;
using Omnius.Yamux.Internal;

namespace Omnius.Yamux;

public enum StreamState
{
    Init,
    SYNSent,
    SYNReceived,
    Established,
    LocalClose,
    RemoteClose,
    Closed,
    Reset
}

public partial class YamuxStream
{
    private readonly YamuxMuxer _muxer;
    private readonly uint _streamId;
    private StreamState _state;
    private readonly AsyncLock _stateLock = new();
    private readonly ArrayPool<byte> _bytesPool;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    private readonly Header _controlHeader = new();
    private readonly AsyncLock _controlHeaderLock = new();

    private uint _receiveWindow;
    private readonly CircularBuffer _receiveBuffer;
    private readonly AsyncLock _receiveLock = new();

    private uint _sendWindow;
    private readonly Header _sendHeader = new();
    private readonly AsyncLock _sendLock = new();
    private readonly AsyncLock _sendWindowLock = new();

    private readonly ManualResetEventSlim _receiveEvent = new ManualResetEventSlim(false);
    private readonly ManualResetEventSlim _sendEvent = new ManualResetEventSlim(false);
    private readonly ManualResetEventSlim _establishEvent = new ManualResetEventSlim(false);
    private ITimer? _closeTimer;

    private readonly CancellationToken _muxerCancellationToken;
    private readonly CancellationTokenSource _streamCancellationTokenSource = new();

    private int _closed = 0;

    internal YamuxStream(YamuxMuxer muxer, uint streamId, StreamState state, ArrayPool<byte> bytesPool, TimeProvider timeProvider, ILogger logger, CancellationToken muxerCancellationToken)
    {
        _muxer = muxer;
        _streamId = streamId;
        _state = state;
        _bytesPool = bytesPool;
        _timeProvider = timeProvider;
        _logger = logger;
        _muxerCancellationToken = muxerCancellationToken;

        _receiveBuffer = new CircularBuffer(_bytesPool);
    }

    public YamuxMuxer Muxer => _muxer;
    public uint StreamId => _streamId;
    public StreamState State => _state;

    private CancellationToken GetMixedCancellationToken(CancellationToken cancellationToken = default)
    {
        return CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _streamCancellationTokenSource.Token, _muxerCancellationToken).Token;
    }

    private void NotifyWaiting()
    {
        _receiveEvent.Set();
        _sendEvent.Set();
        _establishEvent.Set();
    }

    internal async ValueTask WaitForEstablishedAsync(CancellationToken cancellationToken = default)
    {
        await _establishEvent.WaitHandle.WaitAsync(cancellationToken);
    }

    private async ValueTask<int> InternalReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.Length == 0) throw new ArgumentOutOfRangeException(nameof(buffer));

        for (; ; )
        {
            if (_state == StreamState.LocalClose || _state == StreamState.RemoteClose || _state == StreamState.Closed) return 0;
            if (_state == StreamState.Reset) throw new YamuxException(YamuxErrorCode.StreamReset);

            bool available;

            using (_receiveLock.Lock(cancellationToken))
            {
                available = _receiveBuffer.Reader.Available();
                if (!available) _receiveEvent.Reset();
            }

            if (!available)
            {
                await _receiveEvent.WaitHandle.WaitAsync(cancellationToken);
                continue;
            }

            int readLength;

            using (_receiveLock.Lock(cancellationToken))
            {
                readLength = _receiveBuffer.Reader.Read(buffer);
            }

            await this.SendWindowUpdateAsync(cancellationToken);

            return readLength;
        }
    }

    private async ValueTask InternalWriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.Length == 0) throw new ArgumentOutOfRangeException(nameof(buffer));

        using (await _sendLock.LockAsync(cancellationToken))
        {
            int remain = buffer.Length;
            while (remain > 0)
            {
                var writeLength = await this.InternalWriteSubAsync(buffer.Slice(buffer.Length - remain), cancellationToken);
                remain -= writeLength;
            }
        }
    }

    private async ValueTask<int> InternalWriteSubAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        for (; ; )
        {
            if (_state == StreamState.LocalClose || _state == StreamState.RemoteClose || _state == StreamState.Closed) throw new YamuxException(YamuxErrorCode.StreamClosed);
            if (_state == StreamState.Reset) throw new YamuxException(YamuxErrorCode.StreamReset);

            uint sendWindow;

            using (_sendWindowLock.Lock())
            {
                sendWindow = _sendWindow;
                if (sendWindow == 0) _sendEvent.Reset();
            }

            if (sendWindow == 0)
            {
                await _sendEvent.WaitHandle.WaitAsync(cancellationToken);
                continue;
            }

            var length = Math.Min((int)sendWindow, buffer.Length);

            var flags = this.ComputeSendFlags();
            _sendHeader.encode(MessageType.Data, flags, _streamId, (uint)length);

            await _muxer.SendFrameAsync(_sendHeader, buffer.Slice(0, length), cancellationToken);

            using (_sendWindowLock.Lock())
            {
                _sendWindow -= (uint)length;
            }

            return length;
        }
    }

    public async ValueTask CloseAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _closed, 1, 0) != 0) return;

        bool removeStream = false;

        using (_stateLock.Lock())
        {
            switch (_state)
            {
                case StreamState.SYNSent:
                case StreamState.SYNReceived:
                case StreamState.Established:
                    _state = StreamState.LocalClose;
                    break;
                case StreamState.LocalClose:
                    break;
                case StreamState.RemoteClose:
                    _state = StreamState.Closed;
                    removeStream = true;
                    break;
                case StreamState.Closed:
                    break;
                case StreamState.Reset:
                default:
                    _logger.LogWarning("yamux: invalid state for close: {0}", _state);
                    throw new YamuxException(YamuxErrorCode.Unexpected);
            }
        }

        _closeTimer?.Dispose();
        _closeTimer = null;

        if (!removeStream && _muxer.Config.StreamCloseTimeout != Timeout.InfiniteTimeSpan)
        {
            _closeTimer = _timeProvider.CreateTimer((_) => this.OnCloseTimeout(), null, _muxer.Config.StreamCloseTimeout, Timeout.InfiniteTimeSpan);
        }

        using (await _controlHeaderLock.LockAsync(cancellationToken))
        {
            var flags = this.ComputeSendFlags();
            flags |= MessageFlag.FIN;
            _controlHeader.encode(MessageType.WindowUpdate, flags, _streamId, 0);
            await _muxer.SendFrameAsync(_controlHeader, default, cancellationToken);
        }

        this.NotifyWaiting();

        if (removeStream) _muxer.RemoveStream(_streamId);
    }

    private async void OnCloseTimeout()
    {
        this.ForceClose();

        _muxer.RemoveStream(_streamId);

        using (_sendLock.Lock())
        {
            var header = new Header(MessageType.WindowUpdate, MessageFlag.RST, _streamId, 0);
            await _muxer.SendFrameFireAndForgetAsync(header, ReadOnlyMemory<byte>.Empty);
        }
    }

    internal void ForceClose()
    {
        using (_stateLock.Lock())
        {
            _state = StreamState.Closed;
        }

        this.NotifyWaiting();
    }

    internal async ValueTask SendWindowUpdateAsync(CancellationToken cancellationToken = default)
    {
        using (await _controlHeaderLock.LockAsync(cancellationToken))
        {
            uint delta;
            MessageFlag flags;

            using (await _receiveLock.LockAsync(cancellationToken))
            {
                delta = (_muxer.Config.MaxStreamWindow - (uint)_receiveBuffer.Writer.WrittenBytes) - _receiveWindow;
                flags = this.ComputeSendFlags();

                if (delta < (_muxer.Config.MaxStreamWindow / 2) && flags == MessageFlag.None) return;

                _receiveWindow += delta;
            }

            _controlHeader.encode(MessageType.WindowUpdate, flags, _streamId, delta);
            await _muxer.SendFrameAsync(_controlHeader, default, cancellationToken);
        }
    }

    internal MessageFlag ComputeSendFlags()
    {
        using (_stateLock.Lock())
        {
            switch (_state)
            {
                case StreamState.Init:
                    _state = StreamState.SYNSent;
                    return MessageFlag.SYN;
                case StreamState.SYNReceived:
                    _state = StreamState.Established;
                    return MessageFlag.ACK;
                default:
                    return MessageFlag.None;
            }
        }
    }

    internal void AddSendWindow(Header header)
    {
        this.ProcessReceivedFlags(header.Flags);

        using (_sendWindowLock.Lock())
        {
            _sendWindow += header.Length;
            _sendEvent.Set();
        }
    }

    internal async ValueTask EnqueueReadBytesAsync(Header header, Stream reader, CancellationToken cancellationToken = default)
    {
        this.ProcessReceivedFlags(header.Flags);

        if (header.Length == 0) return;

        using (await _receiveLock.LockAsync(cancellationToken))
        {
            if (header.Length > _receiveWindow)
            {
                _logger.LogWarning("yamux: stream {0} receive window exceeded", _streamId);
                throw new YamuxException(YamuxErrorCode.StreamReceiveWindowExceeded);
            }

            var buffer = _receiveBuffer.Writer.GetMemory((int)header.Length);
            int remain = (int)header.Length;

            try
            {
                while (remain > 0)
                {
                    var readLength = await reader.ReadAsync(buffer.Slice((int)header.Length - remain, remain), cancellationToken);
                    _receiveBuffer.Writer.Advance(readLength);
                    _receiveWindow -= (uint)readLength;
                    remain -= readLength;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "yamux: stream {0} read error", _streamId);
                throw new YamuxException(YamuxErrorCode.ConnectionReceiveError);
            }

            _receiveEvent.Set();
        }
    }

    internal void ProcessReceivedFlags(MessageFlag flags)
    {
        using (_stateLock.Lock())
        {

            bool removeStream = false;

            if (flags.HasFlag(MessageFlag.ACK))
            {
                if (_state == StreamState.SYNSent) _state = StreamState.Established;
                _establishEvent.Set();
            }

            if (flags.HasFlag(MessageFlag.FIN))
            {
                switch (_state)
                {
                    case StreamState.SYNSent:
                    case StreamState.SYNReceived:
                    case StreamState.Established:
                        _state = StreamState.RemoteClose;
                        this.NotifyWaiting();
                        break;
                    case StreamState.LocalClose:
                        _state = StreamState.Closed;
                        removeStream = true;
                        this.NotifyWaiting();
                        break;
                    default:
                        _logger.LogWarning("yamux: invalid state for FIN: {0}", _state);
                        throw new YamuxException(YamuxErrorCode.Unexpected);
                }
            }

            if (flags.HasFlag(MessageFlag.RST))
            {
                _state = StreamState.Reset;
                removeStream = true;
                this.NotifyWaiting();
            }

            if (removeStream)
            {
                _closeTimer?.Dispose();
                _closeTimer = null;

                _muxer.RemoveStream(_streamId);
            }
        }
    }
}

public partial class YamuxStream : Stream
{
    protected override void Dispose(bool disposing)
    {
        if (!disposing) return;
        this.DisposeAsync().AsTask().Wait();
    }

    public override async ValueTask DisposeAsync()
    {
        await this.CloseAsync();

        _receiveBuffer.Dispose();
        _sendEvent.Dispose();
        _establishEvent.Dispose();

        _streamCancellationTokenSource.Cancel();
        _streamCancellationTokenSource.Dispose();
    }

    public override bool CanRead => true;
    public override bool CanWrite => true;
    public override bool CanSeek => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        return this.ReadAsync(buffer, offset, count).Result;
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        this.WriteAsync(buffer, offset, count).Wait();
    }

    public override void Flush()
    {
        this.FlushAsync().Wait();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        cancellationToken = this.GetMixedCancellationToken(cancellationToken);

        return await this.InternalReadAsync(buffer.AsMemory(offset, count), cancellationToken);
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        cancellationToken = this.GetMixedCancellationToken(cancellationToken);

        await this.InternalWriteAsync(buffer.AsMemory(offset, count), cancellationToken);
    }
}
