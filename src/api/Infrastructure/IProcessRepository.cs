using TransactionCloseExchange.Api.Models;

namespace TransactionCloseExchange.Api.Infrastructure;

public interface IProcessRepository
{
    Task<ProcessControlRecord> CreateOrGetBySourceHashAsync(Guid processId, string sourceHash, string sourcePath, string sourceSystem, CancellationToken cancellationToken);
    Task<ProcessControlRecord?> GetByIdAsync(Guid processId, CancellationToken cancellationToken);
    Task<bool> TryMarkRunningAsync(Guid processId, CancellationToken cancellationToken);
    Task UpdateRunningMessageAsync(Guid processId, string message, CancellationToken cancellationToken);
    Task MarkCompletedAsync(Guid processId, long rowsRead, long rowsLoaded, long rowsRejected, string message, CancellationToken cancellationToken);
    Task MarkFailedAsync(Guid processId, string message, string errorDetails, CancellationToken cancellationToken);
    Task<(long InsertedCount, long UpdatedCount)> MergeToProductionAsync(Guid processId, CancellationToken cancellationToken);
}
