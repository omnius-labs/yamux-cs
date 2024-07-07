using System.Buffers;
using NeoSmart.AsyncLock;

namespace Omnius.Yamux.Internal;

internal class CircularBuffer : IDisposable
{
    private readonly ArrayPool<byte> _pool;

    private readonly LinkedList<Buffer> _buffers = new LinkedList<Buffer>();
    private readonly ManualResetEventSlim _writeEvent = new ManualResetEventSlim(false);
    private readonly AsyncLock _lock = new AsyncLock();

    public CircularBuffer(ArrayPool<byte> pool)
    {
        _pool = pool;

        var buffer = new Buffer(_pool.Rent(4096));
        _buffers.AddLast(buffer);

        this.Reader = new BufferReader(this);
        this.Writer = new BufferWriter(this);
    }

    public BufferReader Reader { get; }
    public BufferWriter Writer { get; }

    public void Dispose()
    {
        foreach (var buffer in _buffers)
        {
            _pool.Return(buffer.Bytes);
        }

        _buffers.Clear();

        _writeEvent.Dispose();
    }

    public class BufferReader
    {
        private readonly CircularBuffer _cb;

        public BufferReader(CircularBuffer cb)
        {
            _cb = cb;
        }

        public void Advance(int count)
        {
            using (_cb._lock.Lock())
            {
                if (count == 0) return;
                if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));

                var buffer = _cb._buffers.First!.Value;
                if (count > buffer.WrittenBytes - buffer.ReadBytes) throw new InvalidOperationException("Cannot advance past the end of the current buffer");

                buffer.ReadBytes += count;

                this.Shrink();
            }
        }

        public async ValueTask<Memory<byte>> GetMemoryAsync(CancellationToken cancellationToken = default)
        {
            for (; ; )
            {
                await _cb._writeEvent.WaitHandle.WaitAsync(cancellationToken);

                using (await _cb._lock.LockAsync(cancellationToken))
                {
                    if (!this.Available())
                    {
                        _cb._writeEvent.Reset();
                        continue;
                    }

                    var buffer = _cb._buffers.First!.Value;
                    return new Memory<byte>(buffer.Bytes, buffer.ReadBytes, buffer.WrittenBytes - buffer.ReadBytes);
                }
            }
        }

        public async ValueTask<int> ReadAsync(Memory<byte> memory)
        {
            var buffer = await this.GetMemoryAsync();
            var readLength = Math.Min(memory.Length, buffer.Length);
            buffer.Slice(0, readLength).CopyTo(memory);
            this.Advance(readLength);
            return readLength;
        }

        private bool Available()
        {
            using (_cb._lock.Lock())
            {
                this.Shrink();

                if (_cb._buffers.Count == 1)
                {
                    var buffer = _cb._buffers.First!.Value;

                    if (buffer.ReadBytes == buffer.WrittenBytes)
                    {
                        return false;
                    }
                }

                return true;
            }
        }

        private void Shrink()
        {
            using (_cb._lock.Lock())
            {
                while (_cb._buffers.Count > 1)
                {
                    var buffer = _cb._buffers.First!.Value;

                    if (buffer.ReadBytes == buffer.WrittenBytes)
                    {
                        _cb._pool.Return(buffer.Bytes);
                        _cb._buffers.RemoveFirst();
                    }
                }
            }
        }
    }

    public class BufferWriter : IBufferWriter<byte>
    {
        private readonly CircularBuffer _cb;

        public BufferWriter(CircularBuffer cb)
        {
            _cb = cb;
        }

        public long WrittenBytes => _cb._buffers.Sum(b => (long)(b.WrittenBytes - b.ReadBytes));

        public void Advance(int count)
        {
            using (_cb._lock.Lock())
            {
                if (count == 0) return;
                if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));

                var buffer = _cb._buffers.Last!.Value;
                if (count > buffer.Length - buffer.WrittenBytes) throw new InvalidOperationException("Cannot advance past the end of the current buffer");

                buffer.WrittenBytes += count;
                _cb._writeEvent.Set();
            }
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            using (_cb._lock.Lock())
            {
                if (sizeHint < 0) throw new ArgumentOutOfRangeException(nameof(sizeHint));
                if (sizeHint == 0) sizeHint = 1;

                var buffer = _cb._buffers.Last!.Value;
                if (sizeHint > buffer.Length - buffer.WrittenBytes)
                {
                    buffer = new Buffer(_cb._pool.Rent(sizeHint));
                    _cb._buffers.AddLast(buffer);
                }

                return new Memory<byte>(buffer.Bytes, buffer.WrittenBytes, buffer.Length - buffer.WrittenBytes);
            }
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            using (_cb._lock.Lock())
            {
                if (sizeHint < 0) throw new ArgumentOutOfRangeException(nameof(sizeHint));
                if (sizeHint == 0) sizeHint = 1;

                var buffer = _cb._buffers.Last!.Value;
                if (sizeHint > buffer.Length - buffer.WrittenBytes)
                {
                    buffer = new Buffer(_cb._pool.Rent(sizeHint));
                    _cb._buffers.AddLast(buffer);
                }

                return new Span<byte>(buffer.Bytes, buffer.WrittenBytes, buffer.Length - buffer.WrittenBytes);
            }
        }

        public void Write(ReadOnlySpan<byte> span)
        {
            using (_cb._lock.Lock())
            {
                var buffer = this.GetSpan(span.Length);
                span.CopyTo(buffer);
                this.Advance(span.Length);
            }
        }
    }

    private record Buffer
    {
        public Buffer(byte[] value)
        {
            this.Bytes = value;
            this.WrittenBytes = 0;
            this.ReadBytes = 0;
        }

        public byte[] Bytes { get; }
        public int Length => Bytes.Length;
        public int WrittenBytes { get; set; }
        public int ReadBytes { get; set; }
    }
}
