using System.Threading.Channels;

namespace TransactionCloseExchange.Api.Services;

public sealed class InMemoryProcessQueue : IProcessQueue
{
    private readonly Channel<Guid> _queue = Channel.CreateUnbounded<Guid>(
        new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

    public ValueTask EnqueueAsync(Guid processId, CancellationToken cancellationToken = default)
        => _queue.Writer.WriteAsync(processId, cancellationToken);

    public ValueTask<Guid> DequeueAsync(CancellationToken cancellationToken)
        => _queue.Reader.ReadAsync(cancellationToken);
}
