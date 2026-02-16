import argparse
import csv
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import polars as pl
import psycopg

CRITICAL_COLUMNS = [
    "CodFondo",
    "Identificacion",
    "CodDireccion",
    "Valor",
    "Unidades",
    "ObjetivoInversion",
    "Asesor",
    "Oficina",
    "Bloqueo",
    "DiasPermanencia",
]

DATE_COLUMNS = [
    "FechaConstitucion",
    "FechaVencimiento",
    "FechaVtoTeorica",
    "FechaUltimaCancelacion",
]

SOURCE_COLUMNS = [
    "CodFondo",
    "CodClase",
    "NumeroCuentaInversion",
    "TipoIdentificacion",
    "Identificacion",
    "CodDireccion",
    "Valor",
    "Unidades",
    "FechaConstitucion",
    "FechaVencimiento",
    "ObjetivoInversion",
    "Asesor",
    "Referido",
    "CanalApertura",
    "Oficina",
    "OficinaApertura",
    "OficinaActual",
    "Bloqueo",
    "IdCausalBloqueo",
    "DiasPermanencia",
    "FechaVtoTeorica",
    "FechaUltimaCancelacion",
]

COPY_COLUMNS = [
    "process_id",
    "source_hash",
    "source_system",
    "transaction_id",
    "payload_hash",
    "cod_fondo",
    "cod_clase",
    "numero_cuenta_inversion",
    "tipo_identificacion",
    "identificacion",
    "cod_direccion",
    "valor",
    "unidades",
    "fecha_constitucion",
    "fecha_vencimiento",
    "objetivo_inversion",
    "asesor",
    "referido",
    "canal_apertura",
    "oficina",
    "oficina_apertura",
    "oficina_actual",
    "bloqueo",
    "id_causal_bloqueo",
    "dias_permanencia",
    "fecha_vto_teorica",
    "fecha_ultima_cancelacion",
]


@dataclass
class Metrics:
    rows_read: int = 0
    rows_loaded: int = 0
    rows_rejected: int = 0


def normalize_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value).strip()


def sha256_from_values(values: Iterable[object]) -> str:
    canonical = "|".join(normalize_value(v) for v in values)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_file_hash(file_path: Path) -> str:
    hasher = hashlib.sha256()
    with file_path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def build_copy_rows(df: pl.DataFrame, process_id: str, source_hash: str, source_system: str) -> list[list[object]]:
    rows: list[list[object]] = []
    records = df.to_dicts()

    for record in records:
        business_key = [
            record["CodFondo"],
            record["NumeroCuentaInversion"],
            record["Identificacion"],
            record["CodDireccion"],
            record["FechaConstitucion"],
            source_system,
        ]
        payload_values = [record[col] for col in SOURCE_COLUMNS] + [source_system]

        row = [
            process_id,
            source_hash,
            source_system,
            sha256_from_values(business_key),
            sha256_from_values(payload_values),
            record["CodFondo"],
            record["CodClase"],
            record["NumeroCuentaInversion"],
            record["TipoIdentificacion"],
            record["Identificacion"],
            record["CodDireccion"],
            record["Valor"],
            record["Unidades"],
            record["FechaConstitucion"],
            record["FechaVencimiento"],
            record["ObjetivoInversion"],
            record["Asesor"],
            record["Referido"],
            record["CanalApertura"],
            record["Oficina"],
            record["OficinaApertura"],
            record["OficinaActual"],
            record["Bloqueo"],
            record["IdCausalBloqueo"],
            record["DiasPermanencia"],
            record["FechaVtoTeorica"],
            record["FechaUltimaCancelacion"],
        ]
        rows.append(row)

    return rows


def transform_chunk(df: pl.DataFrame) -> pl.DataFrame:
    transformed = (
        df.select(SOURCE_COLUMNS)
        .drop_nulls(CRITICAL_COLUMNS)
        .with_columns([
            pl.col("CodFondo").cast(pl.Int32, strict=False),
            pl.col("Identificacion").cast(pl.Int32, strict=False),
            pl.col("CodDireccion").cast(pl.Int32, strict=False),
            pl.col("Valor").cast(pl.Decimal(18, 2), strict=False),
            pl.col("Unidades").cast(pl.Int64, strict=False),
            pl.col("ObjetivoInversion").cast(pl.Int64, strict=False),
            pl.col("Asesor").cast(pl.Int32, strict=False),
            pl.col("Oficina").cast(pl.Int32, strict=False),
            pl.col("Bloqueo").cast(pl.Int32, strict=False),
            pl.col("DiasPermanencia").cast(pl.Int64, strict=False),
            pl.col("OficinaApertura").cast(pl.Int32, strict=False),
            pl.col("OficinaActual").cast(pl.Int32, strict=False),
            pl.col("IdCausalBloqueo").cast(pl.Int32, strict=False),
        ])
        .drop_nulls(CRITICAL_COLUMNS)
    )

    for date_col in DATE_COLUMNS:
        transformed = transformed.with_columns(
            pl.when(pl.col(date_col).is_null())
            .then(None)
            .otherwise(
                pl.col(date_col)
                .cast(pl.Utf8)
                .str.strptime(pl.Datetime, strict=False, exact=False)
            )
            .alias(date_col)
        )

    return transformed


def copy_to_staging(conn: psycopg.Connection, rows: list[list[object]]) -> None:
    if not rows:
        return

    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerows(rows)
    buffer.seek(0)

    copy_sql = f"""
        COPY staging_transactions ({", ".join(COPY_COLUMNS)})
        FROM STDIN WITH (FORMAT CSV)
    """
    with conn.cursor() as cursor:
        with cursor.copy(copy_sql) as copy:
            copy.write(buffer.read())


def scan_input_file(input_path: Path) -> pl.LazyFrame:
    extension = input_path.suffix.lower()

    if extension == ".parquet":
        return pl.scan_parquet(input_path.as_posix())

    if extension == ".csv":
        return pl.scan_csv(
            input_path.as_posix(),
            has_header=True,
            infer_schema_length=10000,
            ignore_errors=False,
        )

    raise ValueError(
        f"Formato no soportado: {input_path.suffix or '[sin extensión]'}. "
        "Use un archivo .parquet o .csv"
    )


def run_etl(
    input_path: Path,
    process_id: str,
    source_hash: str,
    source_system: str,
    connection_string: str,
    chunk_size: int,
) -> Metrics:
    metrics = Metrics()
    lazy_frame = scan_input_file(input_path)

    with psycopg.connect(connection_string, autocommit=False) as conn:
        offset = 0
        while True:
            chunk = lazy_frame.slice(offset, chunk_size).collect(engine="streaming")
            if chunk.height == 0:
                break

            metrics.rows_read += chunk.height
            transformed = transform_chunk(chunk)
            rows = build_copy_rows(transformed, process_id, source_hash, source_system)
            copy_to_staging(conn, rows)

            metrics.rows_loaded += len(rows)
            metrics.rows_rejected += chunk.height - len(rows)
            conn.commit()

            print(
                f"PROGRESS rows_read={metrics.rows_read} "
                f"rows_loaded={metrics.rows_loaded} "
                f"rows_rejected={metrics.rows_rejected}",
                flush=True,
            )

            offset += chunk_size

    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL masivo (Parquet/CSV) -> PostgreSQL staging")
    parser.add_argument("--parquet-path", required=True, help="Ruta del archivo de entrada (.parquet o .csv)")
    parser.add_argument("--process-id", required=True, help="Identificador del proceso")
    parser.add_argument("--connection-string", required=True, help="Cadena de conexión de PostgreSQL")
    parser.add_argument("--source-system", default="ExternalProvider", help="Sistema origen")
    parser.add_argument("--source-hash", default="", help="Hash SHA-256 del archivo")
    parser.add_argument("--chunk-size", type=int, default=200000, help="Tamaño de bloque para procesar")
    args = parser.parse_args()

    input_path = Path(args.parquet_path)
    source_hash = args.source_hash or compute_file_hash(input_path)

    metrics = run_etl(
        input_path=input_path,
        process_id=args.process_id,
        source_hash=source_hash,
        source_system=args.source_system,
        connection_string=args.connection_string,
        chunk_size=args.chunk_size,
    )

    print(
        json.dumps(
            {
                "processId": args.process_id,
                "sourceHash": source_hash,
                "rowsRead": metrics.rows_read,
                "rowsLoaded": metrics.rows_loaded,
                "rowsRejected": metrics.rows_rejected,
            }
        )
    )


if __name__ == "__main__":
    main()
