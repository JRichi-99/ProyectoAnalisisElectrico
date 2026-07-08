#!/usr/bin/env python3
"""Filtra la base CupraTherm cruzada (SII + CEN) a registros que cumplen ambas condiciones."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd  # noqa: E402

from cupratherm.filtering import parse_bool_series, read_csv_robust  # noqa: E402
from cupratherm.io import load_paths, resolve_path  # noqa: E402

REQUIRED_COLUMNS = ("MATCH_ACTECOS_SII", "CLIENTE_LIBRE_CEN")
RUT_COLUMN = "RUT_RAZON_SOCIAL"


def parse_args() -> argparse.Namespace:
    paths = load_paths()
    parser = argparse.ArgumentParser(
        description=(
            "Genera la base final CupraTherm filtrando establecimientos con "
            "MATCH_ACTECOS_SII == TRUE y CLIENTE_LIBRE_CEN == TRUE."
        )
    )
    parser.add_argument(
        "--input",
        default=paths["output_cruce_clientes_libres_cen"],
        help="Ruta al CSV cruzado con ACTECOS y Clientes Libres CEN",
    )
    parser.add_argument(
        "--output",
        default=paths["output_base_final"],
        help="Ruta del CSV de la base final filtrada",
    )
    parser.add_argument(
        "--report",
        default=paths["reporte_base_final"],
        help="Ruta del reporte de control en texto plano",
    )
    return parser.parse_args()


def _require_columns(df: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        available = ", ".join(map(str, df.columns))
        raise KeyError(
            f"Faltan columnas obligatorias: {', '.join(missing)}. "
            f"Columnas disponibles: {available}"
        )


def _build_report(
    *,
    input_path: Path,
    output_path: Path,
    report_path: Path,
    n_filas_entrada: int,
    n_rut_entrada: int,
    n_match_actecos: int,
    n_cliente_libre: int,
    n_filas_final: int,
    n_rut_final: int,
) -> str:
    lines = [
        "REPORTE BASE FINAL CUPRATHERM",
        f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"Archivo entrada: {input_path}",
        f"Archivo salida:  {output_path}",
        "",
        f"Filas base entrada: {n_filas_entrada}",
        f"RUT únicos entrada: {n_rut_entrada}",
        f"Filas con MATCH_ACTECOS_SII == TRUE: {n_match_actecos}",
        f"Filas con CLIENTE_LIBRE_CEN == TRUE: {n_cliente_libre}",
        f"Filas base final, ambas condiciones TRUE: {n_filas_final}",
        f"RUT únicos base final: {n_rut_final}",
        "",
        "Criterio de filtro:",
        "  MATCH_ACTECOS_SII == TRUE",
        "  CLIENTE_LIBRE_CEN == TRUE",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()

    input_path = resolve_path(args.input)
    output_path = resolve_path(args.output)
    report_path = resolve_path(args.report)

    if not input_path.exists():
        raise FileNotFoundError(
            f"No existe la base de entrada: {input_path}\n"
            "Ejecute primero: python scripts/cruzar_clientes_libres_cen.py"
        )

    print(f"Leyendo base cruzada: {input_path}")
    df = read_csv_robust(input_path)
    _require_columns(df)

    n_filas_entrada = len(df)
    n_rut_entrada = df[RUT_COLUMN].nunique() if RUT_COLUMN in df.columns else 0

    match_actecos = parse_bool_series(df["MATCH_ACTECOS_SII"])
    cliente_libre = parse_bool_series(df["CLIENTE_LIBRE_CEN"])

    n_match_actecos = int(match_actecos.sum())
    n_cliente_libre = int(cliente_libre.sum())

    mask_final = match_actecos & cliente_libre
    base_final = df.loc[mask_final].copy()

    n_filas_final = len(base_final)
    n_rut_final = (
        base_final[RUT_COLUMN].nunique() if RUT_COLUMN in base_final.columns else 0
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    base_final.to_csv(output_path, index=False, encoding="utf-8-sig")

    report_text = _build_report(
        input_path=input_path,
        output_path=output_path,
        report_path=report_path,
        n_filas_entrada=n_filas_entrada,
        n_rut_entrada=n_rut_entrada,
        n_match_actecos=n_match_actecos,
        n_cliente_libre=n_cliente_libre,
        n_filas_final=n_filas_final,
        n_rut_final=n_rut_final,
    )
    report_path.write_text(report_text, encoding="utf-8")

    print("\n" + "=" * 60)
    print("RESUMEN FILTRO BASE FINAL CUPRATHERM")
    print("=" * 60)
    print(f"Filas base entrada:                        {n_filas_entrada:,}")
    print(f"RUT únicos entrada:                        {n_rut_entrada:,}")
    print(f"Filas con MATCH_ACTECOS_SII == TRUE:       {n_match_actecos:,}")
    print(f"Filas con CLIENTE_LIBRE_CEN == TRUE:       {n_cliente_libre:,}")
    print(f"Filas base final, ambas condiciones TRUE:  {n_filas_final:,}")
    print(f"RUT únicos base final:                     {n_rut_final:,}")
    print(f"Archivo CSV generado:                      {output_path}")
    print(f"Reporte de control:                        {report_path}")
    print("=" * 60)

    if n_filas_final == 0:
        print("\nAdvertencia: la base final quedó vacía. Revise los criterios de filtro.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
