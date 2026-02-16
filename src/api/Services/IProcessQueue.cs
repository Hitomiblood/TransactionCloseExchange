namespace TransactionCloseExchange.Api.Services;

public interface IProcessQueue
{
    ValueTask EnqueueAsync(Guid processId, CancellationToken cancellationToken = default);
    ValueTask<Guid> DequeueAsync(CancellationToken cancellationToken);
}
