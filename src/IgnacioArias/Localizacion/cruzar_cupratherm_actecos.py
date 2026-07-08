#!/usr/bin/env python3
"""Cruce de establecimientos RetC con actividades económicas del SII."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cupratherm.io import (  # noqa: E402
    load_paths,
    read_resumen_establecimientos,
    resolve_path,
)
from cupratherm.matching import (  # noqa: E402
    aggregate_actecos_by_rut,
    merge_establecimientos_actecos,
    validate_output,
)


def parse_args() -> argparse.Namespace:
    paths = load_paths()
    parser = argparse.ArgumentParser(
        description="Cruza Resumen_Establecimientos con actividades económicas SII (ACTECOS)."
    )
    parser.add_argument(
        "--resumen",
        default=paths["resumen_establecimientos"],
        help="Ruta al CSV de establecimientos RetC",
    )
    parser.add_argument(
        "--actecos",
        default=paths["actecos_sii"],
        help="Ruta al archivo PUB_NOM_ACTECOS.txt del SII",
    )
    parser.add_argument(
        "--salida",
        default=paths["output_cruce_actecos"],
        help="Ruta de salida del cruce",
    )
    parser.add_argument(
        "--log",
        default=paths["log_no_parseadas"],
        help="Ruta del log de líneas ACTECOS no parseadas",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    resumen_path = resolve_path(args.resumen)
    actecos_path = resolve_path(args.actecos)
    salida_path = resolve_path(args.salida)
    log_path = resolve_path(args.log)

    print("Leyendo Resumen_Establecimientos...")
    resumen = read_resumen_establecimientos(resumen_path)
    n_filas = len(resumen)
    n_rut_unicos = resumen["RUT_RAZON_SOCIAL"].nunique()

    print("Procesando ACTECOS SII (lectura línea por línea)...")
    actecos_agg, n_rut_sii = aggregate_actecos_by_rut(actecos_path, log_path=log_path)

    print("Realizando cruce por RUT...")
    resultado = merge_establecimientos_actecos(resumen, actecos_agg)

    validate_output(resumen, resultado)

    salida_path.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(salida_path, index=False)

    n_match = int(resultado["MATCH_ACTECOS_SII"].sum())
    pct_match = (n_match / n_filas * 100) if n_filas else 0.0

    log_generado = log_path.exists() and log_path.stat().st_size > 0

    print("\n" + "=" * 60)
    print("RESUMEN DEL CRUCE ACTECOS")
    print("=" * 60)
    print(f"Filas en Resumen_Establecimientos:     {n_filas:,}")
    print(f"RUT únicos en Resumen_Establecimientos: {n_rut_unicos:,}")
    print(f"RUT únicos en SII (ACTECOS):            {n_rut_sii:,}")
    print(f"Establecimientos con match:             {n_match:,}")
    print(f"Porcentaje de match:                    {pct_match:.2f}%")
    print(f"Archivo de salida:                      {salida_path}")
    if log_generado:
        print(f"Log de líneas no parseadas:             {log_path}")
    else:
        print("Log de líneas no parseadas:             (sin líneas problemáticas)")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
