namespace TransactionCloseExchange.Api.Models;

public sealed record ProcessRequest(string? SourcePath, string? SourceSystem);

public sealed record ProcessAcceptedResponse(Guid ProcessId, string Status, string SourceHash);

public sealed record ProcessStatusResponse(
    Guid ProcessId,
    string SourceHash,
    string SourcePath,
    string SourceSystem,
    string Status,
    DateTimeOffset StartedAt,
    DateTimeOffset? FinishedAt,
    long RowsRead,
    long RowsLoaded,
    long RowsRejected,
    string? Message,
    string? ErrorDetails);

public sealed class ProcessControlRecord
{
    public Guid ProcessId { get; init; }
    public string SourceHash { get; init; } = string.Empty;
    public string SourcePath { get; init; } = string.Empty;
    public string SourceSystem { get; init; } = string.Empty;
    public string Status { get; init; } = string.Empty;
    public DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset? FinishedAt { get; init; }
    public long RowsRead { get; init; }
    public long RowsLoaded { get; init; }
    public long RowsRejected { get; init; }
    public string? Message { get; init; }
    public string? ErrorDetails { get; init; }
}

public sealed record EtlMetrics(Guid ProcessId, string SourceHash, long RowsRead, long RowsLoaded, long RowsRejected);
