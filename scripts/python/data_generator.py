import argparse
from datetime import datetime, timedelta, UTC
import random

import polars as pl


def build_dataframe(rows: int) -> pl.DataFrame:
    random.seed(42)
    now = datetime.now(UTC)

    values = {
        "CodFondo": [random.randint(100, 999) for _ in range(rows)],
        "CodClase": [random.choice(["A", "B", "C", None]) for _ in range(rows)],
        "NumeroCuentaInversion": [f"ACCT-{random.randint(10_000_000, 99_999_999)}" for _ in range(rows)],
        "TipoIdentificacion": [random.choice(["CC", "NIT", "CE", None]) for _ in range(rows)],
        "Identificacion": [random.randint(1_000_000, 99_999_999) for _ in range(rows)],
        "CodDireccion": [random.randint(1, 50) for _ in range(rows)],
        "Valor": [round(random.uniform(1_000, 999_999), 2) for _ in range(rows)],
        "Unidades": [random.randint(1, 5_000) for _ in range(rows)],
        "FechaConstitucion": [now - timedelta(days=random.randint(30, 3650)) for _ in range(rows)],
        "FechaVencimiento": [now + timedelta(days=random.randint(30, 3650)) for _ in range(rows)],
        "ObjetivoInversion": [random.randint(1, 9) for _ in range(rows)],
        "Asesor": [random.randint(1000, 9999) for _ in range(rows)],
        "Referido": [random.choice(["WEB", "APP", "AGENCIA", None]) for _ in range(rows)],
        "CanalApertura": [random.choice(["DIGITAL", "PRESENCIAL", None]) for _ in range(rows)],
        "Oficina": [random.randint(1, 300) for _ in range(rows)],
        "OficinaApertura": [random.choice([random.randint(1, 300), None]) for _ in range(rows)],
        "OficinaActual": [random.choice([random.randint(1, 300), None]) for _ in range(rows)],
        "Bloqueo": [random.randint(0, 1) for _ in range(rows)],
        "IdCausalBloqueo": [random.choice([random.randint(1, 20), None]) for _ in range(rows)],
        "DiasPermanencia": [random.randint(1, 2000) for _ in range(rows)],
        "FechaVtoTeorica": [now + timedelta(days=random.randint(5, 2000)) for _ in range(rows)],
        "FechaUltimaCancelacion": [random.choice([now - timedelta(days=random.randint(1, 1000)), None]) for _ in range(rows)],
    }

    return pl.DataFrame(values)


def main() -> None:
    parser = argparse.ArgumentParser(description="Genera CSV de prueba para ingesta masiva")
    parser.add_argument("--output", default="transactions_5000000.csv", help="Ruta del CSV de salida")
    parser.add_argument("--rows", type=int, default=5_000_000, help="Número de filas a generar")
    args = parser.parse_args()

    df = build_dataframe(args.rows)
    df.write_csv(args.output)
    print(f"CSV generado: {args.output} ({args.rows} filas)")


if __name__ == "__main__":
    main()
