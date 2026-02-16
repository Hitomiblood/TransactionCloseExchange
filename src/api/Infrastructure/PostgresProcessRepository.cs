using Npgsql;
using TransactionCloseExchange.Api.Models;

namespace TransactionCloseExchange.Api.Infrastructure;

public sealed class PostgresProcessRepository : IProcessRepository
{
    private const int MergeCommandTimeoutSeconds = 0;
    private readonly NpgsqlDataSource _dataSource;

    public PostgresProcessRepository(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource;
    }

    public async Task<ProcessControlRecord> CreateOrGetBySourceHashAsync(
        Guid processId,
        string sourceHash,
        string sourcePath,
        string sourceSystem,
        CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO process_control (process_id, source_hash, source_path, source_system, status)
            VALUES (@process_id, @source_hash, @source_path, @source_system, 'Pending')
            ON CONFLICT (source_hash)
            DO UPDATE SET
                source_path = EXCLUDED.source_path,
                source_system = EXCLUDED.source_system,
                status = CASE
                    WHEN process_control.status = 'Failed' THEN 'Pending'
                    ELSE process_control.status
                END,
                started_at = CASE
                    WHEN process_control.status = 'Failed' THEN NOW()
                    ELSE process_control.started_at
                END,
                finished_at = CASE
                    WHEN process_control.status = 'Failed' THEN NULL
                    ELSE process_control.finished_at
                END,
                message = CASE
                    WHEN process_control.status = 'Failed' THEN 'Reintento solicitado'
                    ELSE process_control.message
                END,
                error_details = CASE
                    WHEN process_control.status = 'Failed' THEN NULL
                    ELSE process_control.error_details
                END
            RETURNING process_id, source_hash, source_path, source_system, status, started_at, finished_at, rows_read, rows_loaded, rows_rejected, message, error_details;
            """;

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("process_id", processId);
        cmd.Parameters.AddWithValue("source_hash", sourceHash);
        cmd.Parameters.AddWithValue("source_path", sourcePath);
        cmd.Parameters.AddWithValue("source_system", sourceSystem);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException("No se pudo crear o recuperar el proceso.");
        }

        return MapRecord(reader);
    }

    public async Task<ProcessControlRecord?> GetByIdAsync(Guid processId, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT process_id, source_hash, source_path, source_system, status, started_at, finished_at, rows_read, rows_loaded, rows_rejected, message, error_details
            FROM process_control
            WHERE process_id = @process_id;
            """;

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("process_id", processId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return MapRecord(reader);
    }

    public async Task<bool> TryMarkRunningAsync(Guid processId, CancellationToken cancellationToken)
    {
        const string sql = """
            UPDATE process_control
            SET status = 'Running',
                message = 'Iniciando ETL',
                error_details = NULL
            WHERE process_id = @process_id
              AND status = 'Pending';
            """;

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("process_id", processId);

        var affected = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return affected == 1;
    }

    public async Task MarkCompletedAsync(
        Guid processId,
        long rowsRead,
        long rowsLoaded,
        long rowsRejected,
        string message,
        CancellationToken cancellationToken)
    {
        const string sql = """
            UPDATE process_control
            SET status = 'Completed',
                finished_at = NOW(),
                rows_read = @rows_read,
                rows_loaded = @rows_loaded,
                rows_rejected = @rows_rejected,
                message = @message,
                error_details = NULL
            WHERE process_id = @process_id;
            """;

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("process_id", processId);
        cmd.Parameters.AddWithValue("rows_read", rowsRead);
        cmd.Parameters.AddWithValue("rows_loaded", rowsLoaded);
        cmd.Parameters.AddWithValue("rows_rejected", rowsRejected);
        cmd.Parameters.AddWithValue("message", message);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkFailedAsync(Guid processId, string message, string errorDetails, CancellationToken cancellationToken)
    {
        const string sql = """
            UPDATE process_control
            SET status = 'Failed',
                finished_at = NOW(),
                message = @message,
                error_details = @error_details
            WHERE process_id = @process_id;
            """;

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("process_id", processId);
        cmd.Parameters.AddWithValue("message", message);
        cmd.Parameters.AddWithValue("error_details", errorDetails);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<(long InsertedCount, long UpdatedCount)> MergeToProductionAsync(Guid processId, CancellationToken cancellationToken)
    {
        const string sql = "SELECT inserted_count, updated_count FROM merge_staging_to_transactions(@process_id);";

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("process_id", processId);
        cmd.CommandTimeout = MergeCommandTimeoutSeconds;

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return (0, 0);
        }

        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    private static ProcessControlRecord MapRecord(NpgsqlDataReader reader)
    {
        return new ProcessControlRecord
        {
            ProcessId = reader.GetGuid(0),
            SourceHash = reader.GetString(1),
            SourcePath = reader.GetString(2),
            SourceSystem = reader.GetString(3),
            Status = reader.GetString(4),
            StartedAt = reader.GetFieldValue<DateTimeOffset>(5),
            FinishedAt = reader.IsDBNull(6) ? null : reader.GetFieldValue<DateTimeOffset>(6),
            RowsRead = reader.GetInt64(7),
            RowsLoaded = reader.GetInt64(8),
            RowsRejected = reader.GetInt64(9),
            Message = reader.IsDBNull(10) ? null : reader.GetString(10),
            ErrorDetails = reader.IsDBNull(11) ? null : reader.GetString(11)
        };
    }
}
