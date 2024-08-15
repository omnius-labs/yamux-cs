using System.Buffers;
using Microsoft.Extensions.Logging;
using NeoSmart.AsyncLock;
using Omnius.Yamux.Internal;

namespace Omnius.Yamux;

enum StreamState
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

public partial class YamuxStream : IAsyncDisposable
{
    private readonly YamuxMuxer _muxer;
    private readonly uint _streamId;
    private StreamState _state;
    private readonly AsyncLock _stateLock = new();
    private readonly ArrayPool<byte> _bytesPool;
    private readonly ILogger _logger;

    private readonly Header _controlHeader = new();
    private readonly AsyncLock _controlHeaderLock = new();

    private uint _receiveWindow;
    private readonly CircularBuffer _receiveBuffer;
    private readonly AsyncLock _receiveLock = new();

    private uint _sendWindow;
    private readonly AsyncLock _sendLock = new();
    private readonly Header _sendHeader = new();
    private readonly ManualResetEventSlim _sendEvent = new ManualResetEventSlim(false);

    private int _closed = 0;

    internal YamuxStream(YamuxMuxer muxer, uint streamId, StreamState state, ArrayPool<byte> bytesPool, ILogger logger)
    {
        _muxer = muxer;
        _streamId = streamId;
        _state = state;
        _bytesPool = bytesPool;
        _logger = logger;

        _receiveBuffer = new CircularBuffer(_bytesPool);
    }

    public async ValueTask DisposeAsync()
    {
        await this.CloseAsync();

        _receiveBuffer.Dispose();
        _sendEvent.Dispose();
    }

    public YamuxMuxer Muxer => _muxer;
    public uint StreamId => _streamId;

    private async ValueTask<int> InternalReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_state == StreamState.LocalClose || _state == StreamState.RemoteClose || _state == StreamState.Closed) return 0;
        if (_state == StreamState.Reset) throw new YamuxException(YamuxErrorCode.StreamReset);
        if (buffer.Length == 0) throw new ArgumentOutOfRangeException(nameof(buffer));

        var readLength = await _receiveBuffer.Reader.ReadAsync(buffer);
        await this.SendWindowUpdateAsync(cancellationToken);
        return readLength;
    }

    private async ValueTask InternalWriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_state == StreamState.LocalClose || _state == StreamState.RemoteClose || _state == StreamState.Closed) throw new YamuxException(YamuxErrorCode.StreamClosed);
        if (_state == StreamState.Reset) throw new YamuxException(YamuxErrorCode.StreamReset);
        if (buffer.Length == 0) throw new ArgumentOutOfRangeException(nameof(buffer));

        int remain = buffer.Length;
        while (remain > 0)
        {
            var writeLength = await this.InternalWriteSubAsync(buffer.Slice(buffer.Length - remain), cancellationToken);
            remain -= writeLength;
        }
    }

    private async ValueTask<int> InternalWriteSubAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        for (; ; )
        {
            await _sendEvent.WaitHandle.WaitAsync(cancellationToken);

            using (await _sendLock.LockAsync(cancellationToken))
            {
                var window = _sendWindow;
                if (window == 0)
                {
                    _sendEvent.Reset();
                    continue;
                }

                var length = Math.Min((int)window, buffer.Length);

                var flags = this.ComputeSendFlags();
                _sendHeader.encode(MessageType.Data, flags, _streamId, (uint)length);

                await _muxer.SendFrameAsync(_sendHeader, buffer.Slice(0, length), cancellationToken);
                Interlocked.Add(ref _sendWindow, (uint)~(length - 1));
                return length;
            }
        }
    }

    public async ValueTask CloseAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _closed, 1, 0) != 0) return;

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
                case StreamState.RemoteClose:
                    _state = StreamState.Closed;
                    break;
                case StreamState.Closed:
                    break;
                case StreamState.Reset:
                    throw new YamuxException(YamuxErrorCode.StreamReset);
                default:
                    _logger.LogWarning("yamux: invalid state for close: {0}", _state);
                    throw new YamuxException(YamuxErrorCode.Unexpected);
            }
        }

        var flags = this.ComputeSendFlags();
        flags |= MessageFlag.FIN;
        _controlHeader.encode(MessageType.WindowUpdate, flags, _streamId, 0);
        await _muxer.SendFrameAsync(_controlHeader, default, cancellationToken);

        _muxer.RemoveStream(_streamId);
    }

    internal void ForceClose()
    {
        using (_stateLock.Lock())
        {
            _state = StreamState.Closed;
        }
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

        using (_sendLock.Lock())
        {
            Interlocked.Add(ref _sendWindow, header.Length);
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
                    var readLength = await reader.ReadAsync(buffer.Slice((int)header.Length - remain, remain));
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
        }
    }

    internal void ProcessReceivedFlags(MessageFlag flags)
    {
        using (_stateLock.Lock())
        {
            if (flags.HasFlag(MessageFlag.ACK))
            {
                if (_state == StreamState.SYNSent) _state = StreamState.Established;
            }

            if (flags.HasFlag(MessageFlag.FIN))
            {
                switch (_state)
                {
                    case StreamState.SYNSent:
                    case StreamState.SYNReceived:
                    case StreamState.Established:
                        _state = StreamState.RemoteClose;
                        break;
                    case StreamState.LocalClose:
                        _state = StreamState.Closed;
                        break;
                    default:
                        _logger.LogWarning("yamux: invalid state for FIN: {0}", _state);
                        throw new YamuxException(YamuxErrorCode.Unexpected);
                }
            }

            if (flags.HasFlag(MessageFlag.RST))
            {
                _state = StreamState.Reset;
            }
        }
    }
}

public partial class YamuxStream : Stream
{
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
        return await this.InternalReadAsync(buffer.AsMemory(offset, count), cancellationToken);
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        await this.InternalWriteAsync(buffer.AsMemory(offset, count), cancellationToken);
    }
}
