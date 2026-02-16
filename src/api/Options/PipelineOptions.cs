namespace TransactionCloseExchange.Api.Options;

public sealed class PipelineOptions
{
    public string PythonExecutable { get; set; } = "python";
    public string EtlScriptPath { get; set; } = "scripts/python/etl_processor.py";
    public string DefaultParquetPath { get; set; } = "fondos_500mb.parquet";
    public string SourceSystem { get; set; } = "ExternalProvider";
    public int ChunkSize { get; set; } = 200000;
}
