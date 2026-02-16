using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Npgsql;
using TransactionCloseExchange.Api.Infrastructure;
using TransactionCloseExchange.Api.Models;
using TransactionCloseExchange.Api.Options;

namespace TransactionCloseExchange.Api.Services;

public sealed class PortfolioProcessWorker : BackgroundService
{
	private readonly IProcessQueue _queue;
	private readonly IProcessRepository _repository;
	private readonly PipelineOptions _options;
	private readonly ILogger<PortfolioProcessWorker> _logger;
	private readonly string _workspaceRoot;
	private readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web);

	public PortfolioProcessWorker(
		IProcessQueue queue,
		IProcessRepository repository,
		IOptions<PipelineOptions> options,
		IWebHostEnvironment env,
		ILogger<PortfolioProcessWorker> logger)
	{
		_queue = queue;
		_repository = repository;
		_options = options.Value;
		_logger = logger;
		_workspaceRoot = Path.GetFullPath(Path.Combine(env.ContentRootPath, "..", ".."));
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		while (!stoppingToken.IsCancellationRequested)
		{
			var processId = await _queue.DequeueAsync(stoppingToken);
			await ProcessAsync(processId, stoppingToken);
		}
	}

	private async Task ProcessAsync(Guid processId, CancellationToken cancellationToken)
	{
		var lockAcquired = await _repository.TryMarkRunningAsync(processId, cancellationToken);
		if (!lockAcquired)
		{
			_logger.LogInformation("Proceso {ProcessId} no está pendiente, se omite.", processId);
			return;
		}

		var process = await _repository.GetByIdAsync(processId, cancellationToken);
		if (process is null)
		{
			_logger.LogWarning("Proceso {ProcessId} no encontrado tras adquirir lock.", processId);
			return;
		}

		try
		{
			var sourcePath = ResolvePath(process.SourcePath);
			var sourceHash = ComputeFileHash(sourcePath);

			var metrics = await ExecuteEtlAsync(processId, sourcePath, sourceHash, process.SourceSystem, cancellationToken);
			var mergeResult = await _repository.MergeToProductionAsync(processId, cancellationToken);

			var message = $"Merge completado. Insertados: {mergeResult.InsertedCount}, Actualizados: {mergeResult.UpdatedCount}";
			await _repository.MarkCompletedAsync(
				processId,
				metrics.RowsRead,
				mergeResult.InsertedCount + mergeResult.UpdatedCount,
				metrics.RowsRejected,
				message,
				cancellationToken);

			_logger.LogInformation("Proceso {ProcessId} completado. {Message}", processId, message);
		}
		catch (Exception ex)
		{
			var error = ex.ToString();
			await _repository.MarkFailedAsync(processId, "Fallo durante ejecución de ETL/merge", error, cancellationToken);
			_logger.LogError(ex, "Proceso {ProcessId} finalizó con error.", processId);
		}
	}

	private async Task<EtlMetrics> ExecuteEtlAsync(Guid processId, string sourcePath, string sourceHash, string sourceSystem, CancellationToken cancellationToken)
	{
		var etlScriptPath = ResolvePath(_options.EtlScriptPath);
		var postgresConnection = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION")
			?? throw new InvalidOperationException("Variable de entorno POSTGRES_CONNECTION no definida.");
		var pythonConnInfo = BuildPythonConnInfo(postgresConnection);

		var args = new[]
		{
			Quote(etlScriptPath),
			"--parquet-path", Quote(sourcePath),
			"--process-id", processId.ToString(),
			"--source-hash", sourceHash,
			"--source-system", sourceSystem,
			"--chunk-size", _options.ChunkSize.ToString(),
			"--connection-string", Quote(pythonConnInfo)
		};

		var startInfo = new ProcessStartInfo
		{
			FileName = _options.PythonExecutable,
			Arguments = string.Join(' ', args),
			WorkingDirectory = _workspaceRoot,
			RedirectStandardOutput = true,
			RedirectStandardError = true,
			UseShellExecute = false,
			CreateNoWindow = true,
		};

		using var process = Process.Start(startInfo)
			?? throw new InvalidOperationException("No fue posible iniciar el proceso Python.");

		var stdout = await process.StandardOutput.ReadToEndAsync(cancellationToken);
		var stderr = await process.StandardError.ReadToEndAsync(cancellationToken);
		await process.WaitForExitAsync(cancellationToken);

		if (process.ExitCode != 0)
		{
			throw new InvalidOperationException($"ETL Python falló con código {process.ExitCode}: {stderr}");
		}

		var jsonLine = stdout
			.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries)
			.LastOrDefault() ?? throw new InvalidOperationException("ETL no devolvió métricas en JSON.");

		var metrics = JsonSerializer.Deserialize<EtlMetrics>(jsonLine, _jsonOptions)
			?? throw new InvalidOperationException("No fue posible deserializar métricas ETL.");

		return metrics;
	}

	private string ResolvePath(string path)
	{
		if (Path.IsPathRooted(path))
		{
			return path;
		}

		return Path.GetFullPath(Path.Combine(_workspaceRoot, path));
	}

	private static string ComputeFileHash(string path)
	{
		using var sha = SHA256.Create();
		using var stream = File.OpenRead(path);
		var hash = sha.ComputeHash(stream);
		return Convert.ToHexString(hash).ToLowerInvariant();
	}

	private static string Quote(string value)
	{
		if (string.IsNullOrWhiteSpace(value))
		{
			return "\"\"";
		}

		if (value.Contains(' '))
		{
			return $"\"{value.Replace("\"", "\\\"")}\"";
		}

		return value;
	}

	private static string BuildPythonConnInfo(string connectionString)
	{
		if (connectionString.StartsWith("postgres://", StringComparison.OrdinalIgnoreCase)
			|| connectionString.StartsWith("postgresql://", StringComparison.OrdinalIgnoreCase))
		{
			return connectionString;
		}

		var builder = new NpgsqlConnectionStringBuilder(connectionString);
		var host = string.IsNullOrWhiteSpace(builder.Host) ? "localhost" : builder.Host;
		var port = builder.Port > 0 ? builder.Port : 5432;
		var database = string.IsNullOrWhiteSpace(builder.Database) ? "postgres" : builder.Database;
		var userInfo = string.Empty;

		if (!string.IsNullOrWhiteSpace(builder.Username))
		{
			userInfo = Uri.EscapeDataString(builder.Username);
			if (!string.IsNullOrWhiteSpace(builder.Password))
			{
				userInfo += $":{Uri.EscapeDataString(builder.Password)}";
			}

			userInfo += "@";
		}

		var sslQuery = builder.SslMode == SslMode.Disable
			? string.Empty
			: $"?sslmode={builder.SslMode.ToString().ToLowerInvariant()}";

		return $"postgresql://{userInfo}{host}:{port}/{Uri.EscapeDataString(database)}{sslQuery}";
	}
}
