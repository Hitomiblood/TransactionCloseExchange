using System.Security.Cryptography;
using TransactionCloseExchange.Api.Infrastructure;
using TransactionCloseExchange.Api.Models;
using TransactionCloseExchange.Api.Options;
using TransactionCloseExchange.Api.Services;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<PipelineOptions>(builder.Configuration.GetSection("Pipeline"));
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var postgresConnection = builder.Configuration.GetConnectionString("Postgres")
    ?? throw new InvalidOperationException("ConnectionStrings:Postgres es requerido.");

Environment.SetEnvironmentVariable("POSTGRES_CONNECTION", postgresConnection);

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(postgresConnection));
builder.Services.AddSingleton<IProcessRepository, PostgresProcessRepository>();
builder.Services.AddSingleton<IProcessQueue, InMemoryProcessQueue>();
builder.Services.AddHostedService<PortfolioProcessWorker>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/", () => Results.Redirect("/swagger")).ExcludeFromDescription();

app.MapPost("/api/portfolio/process", async (
    ProcessRequest request,
    IProcessRepository repository,
    IProcessQueue queue,
    IConfiguration configuration,
    CancellationToken cancellationToken) =>
{
    var sourcePath = request.SourcePath;
    if (string.IsNullOrWhiteSpace(sourcePath))
    {
        sourcePath = configuration.GetSection("Pipeline").GetValue<string>("DefaultParquetPath")
            ?? throw new InvalidOperationException("Pipeline:DefaultParquetPath es requerido.");
    }

    var normalizedPath = Path.IsPathRooted(sourcePath)
        ? sourcePath
        : Path.GetFullPath(Path.Combine(app.Environment.ContentRootPath, "..", "..", sourcePath));

    if (!File.Exists(normalizedPath))
    {
        return Results.BadRequest(new { error = $"Archivo no encontrado: {normalizedPath}" });
    }

    var sourceHash = ComputeFileHash(normalizedPath);
    var sourceSystem = string.IsNullOrWhiteSpace(request.SourceSystem)
        ? configuration.GetSection("Pipeline").GetValue<string>("SourceSystem") ?? "ExternalProvider"
        : request.SourceSystem;
    var candidateProcessId = Guid.NewGuid();

    var process = await repository.CreateOrGetBySourceHashAsync(
        candidateProcessId,
        sourceHash,
        normalizedPath,
        sourceSystem,
        cancellationToken);

    if (process.Status is "Pending")
    {
        await queue.EnqueueAsync(process.ProcessId, cancellationToken);
    }

    var accepted = new ProcessAcceptedResponse(process.ProcessId, process.Status, process.SourceHash);
    return Results.Accepted($"/api/portfolio/status/{process.ProcessId}", accepted);
})
.WithName("StartPortfolioProcess")
.WithSummary("Inicia el proceso asíncrono de ingesta y valoración")
.WithDescription("Retorna 202 Accepted con ProcessId y dispara el pipeline en background sin bloquear la solicitud.")
.Produces<ProcessAcceptedResponse>(StatusCodes.Status202Accepted)
.Produces(StatusCodes.Status400BadRequest);

app.MapGet("/api/portfolio/status/{id:guid}", async (
    Guid id,
    IProcessRepository repository,
    CancellationToken cancellationToken) =>
{
    var process = await repository.GetByIdAsync(id, cancellationToken);
    if (process is null)
    {
        return Results.NotFound(new { error = "ProcessId no encontrado" });
    }

    var response = new ProcessStatusResponse(
        process.ProcessId,
        process.SourceHash,
        process.SourcePath,
        process.SourceSystem,
        process.Status,
        process.StartedAt,
        process.FinishedAt,
        process.RowsRead,
        process.RowsLoaded,
        process.RowsRejected,
        process.Message,
        process.ErrorDetails);

    return Results.Ok(response);
})
.WithName("GetPortfolioProcessStatus")
.WithSummary("Consulta el estado de un proceso")
.WithDescription("Permite verificar si el proceso está Pending, Running, Completed o Failed.")
.Produces<ProcessStatusResponse>(StatusCodes.Status200OK)
.Produces(StatusCodes.Status404NotFound);

app.Run();

static string ComputeFileHash(string path)
{
    using var sha = SHA256.Create();
    using var stream = File.OpenRead(path);
    var hash = sha.ComputeHash(stream);
    return Convert.ToHexString(hash).ToLowerInvariant();
}
