#!/usr/bin/env python3
"""Cruce de establecimientos con Clientes Libres del Coordinador Eléctrico Nacional."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cupratherm.cen import (  # noqa: E402
    consolidate_clientes_libres,
    load_clientes_libres,
    merge_establecimientos_clientes_libres,
    validate_output,
)
from cupratherm.io import load_paths, resolve_path  # noqa: E402
import pandas as pd  # noqa: E402


def parse_args() -> argparse.Namespace:
    paths = load_paths()
    parser = argparse.ArgumentParser(
        description=(
            "Cruza Resumen_Establecimientos_con_ACTECOS con "
            "Clientes Libres del Coordinador Eléctrico Nacional."
        )
    )
    parser.add_argument(
        "--establecimientos",
        default=paths["output_cruce_actecos"],
        help="Ruta al CSV procesado con ACTECOS",
    )
    parser.add_argument(
        "--clientes-libres",
        default=paths["clientes_libres_cen"],
        help="Ruta al Excel de Clientes Libres CEN",
    )
    parser.add_argument(
        "--salida",
        default=paths["output_cruce_clientes_libres_cen"],
        help="Ruta del CSV de salida",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    establecimientos_path = resolve_path(args.establecimientos)
    clientes_path = resolve_path(args.clientes_libres)
    salida_path = resolve_path(args.salida)

    if not establecimientos_path.exists():
        raise FileNotFoundError(
            f"No existe el CSV procesado de establecimientos: {establecimientos_path}"
        )

    print("Leyendo base procesada con ACTECOS...")
    establecimientos = pd.read_csv(establecimientos_path, dtype=str, keep_default_na=False)
    n_filas = len(establecimientos)
    n_rut_base = establecimientos["RUT_RAZON_SOCIAL"].nunique()

    print("Leyendo y consolidando Clientes Libres CEN...")
    clientes = load_clientes_libres(clientes_path)
    n_rut_cen_raw = clientes["RUT_MATCH"].nunique()
    cen = consolidate_clientes_libres(clientes)
    n_rut_cen = len(cen)

    print("Realizando cruce por RUT...")
    resultado = merge_establecimientos_clientes_libres(establecimientos, cen)
    validate_output(establecimientos, resultado)

    salida_path.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(salida_path, index=False)

    n_match_filas = int(resultado["CLIENTE_LIBRE_CEN"].sum())
    n_match_ruts = resultado.loc[
        resultado["CLIENTE_LIBRE_CEN"], "RUT_RAZON_SOCIAL"
    ].nunique()

    print("\n" + "=" * 60)
    print("RESUMEN DEL CRUCE CLIENTES LIBRES CEN")
    print("=" * 60)
    print(f"Filas base establecimientos:              {n_filas:,}")
    print(f"RUT únicos base establecimientos:         {n_rut_base:,}")
    print(f"RUT únicos clientes libres CEN:           {n_rut_cen:,}")
    print(f"Filas identificadas como clientes libres: {n_match_filas:,}")
    print(f"RUT únicos identificados como clientes libres: {n_match_ruts:,}")
    print(f"Archivo de salida:                        {salida_path}")
    print("=" * 60)
    print(
        "\nNota: el cruce es por RUT/razón social. Si una empresa tiene varios "
        "establecimientos, la información CEN consolidada se repite en cada fila."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
