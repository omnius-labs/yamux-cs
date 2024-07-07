using Omnius.Yamux.Internal;

namespace Omnius.Yamux;

public record YamuxConfig
{
    public int MaxAcceptBacklog { get; init; } = 100;
    public bool EnableKeepAlive { get; init; } = false;
    public TimeSpan KeepAliveInterval { get; init; } = TimeSpan.FromSeconds(10);
    public uint MaxStreamWindow { get; init; } = 1024 * 1024;
    public int PingTimeout { get; init; } = 5000;

    public void Verify()
    {
        if (this.MaxAcceptBacklog < 0) throw new ArgumentOutOfRangeException(nameof(this.MaxAcceptBacklog));
        if (this.KeepAliveInterval < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(this.KeepAliveInterval));
        if (this.MaxStreamWindow < Constants.INITIAL_STREAM_WINDOW) throw new ArgumentOutOfRangeException(nameof(this.MaxStreamWindow));
    }
}
