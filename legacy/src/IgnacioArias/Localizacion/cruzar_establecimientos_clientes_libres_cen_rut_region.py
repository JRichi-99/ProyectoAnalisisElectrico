#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cruce CupraTherm: Establecimientos ACTECOS/SII x Clientes Libres CEN georreferenciados.

Objetivo
--------
Cruzar la base de establecimientos:
    data/processed/Resumen_Establecimientos_con_ACTECOS.csv

con la base de clientes libres CEN ya enriquecida con subestaciones:
    data/processed/coordinador_electrico/Clientes_Libres_CEN_2025_georreferenciados_subestaciones.csv

usando una llave compuesta:
    RUT CLIENTE + SUBESTACION_REGION  <->  RUT_RAZON_SOCIAL + REGION

Esto corrige el problema metodológico de usar solo el RUT, ya que un mismo RUT puede tener
múltiples barras/subestaciones en distintas regiones.

Salidas principales
-------------------
1) Base_Establecimientos_ACTECOS_SII_clientes_libres_CEN_RUT_REGION_2025.csv
   - Base de establecimientos enriquecida sin multiplicar filas por barra.
2) Base_Final_CupraTherm_ACTECOS_SII_Clientes_Libres_CEN_RUT_REGION_2025.csv
   - Base final filtrada: MATCH_ACTECOS_SII == True y CLIENTE_LIBRE_CEN_REGION == True.
3) CEN_Clientes_Libres_consolidado_RUT_REGION_2025.csv
   - Consumos CEN agregados por RUT + región. Usar esta tabla para análisis agregados de consumo.
4) CEN_Clientes_Libres_consolidado_RUT_REGION_BARRA_2025.csv
   - Consumos CEN agregados por RUT + región + barra.
5) Puente_Establecimientos_CEN_RUT_REGION_BARRA_2025_para_revision.csv
   - Tabla puente establecimiento x barra candidata, incluyendo distancia geográfica referencial.
6) reporte_cruce_clientes_libres_CEN_RUT_REGION_2025.txt
   - Reporte de control.
"""

from __future__ import annotations

import argparse
import math
import re
import unicodedata
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd


MONTH_COLS = [
    "ene-25", "feb-25", "mar-25", "abr-25", "may-25", "jun-25",
    "jul-25", "ago-25", "sept-25", "oct-25", "nov-25", "dic-25",
]


def strip_accents(value: object) -> str:
    """Remove accents while preserving plain characters."""
    if pd.isna(value):
        return ""
    text = str(value)
    return "".join(
        char for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )


def normalize_rut(value: object) -> str:
    """
    Normalize Chilean RUT to BODY-DV, without dots and with uppercase K.
    Examples:
        88.680.500-4 -> 88680500-4
        76101812-4   -> 76101812-4
    """
    if pd.isna(value):
        return ""
    text = str(value).strip().upper()
    text = text.replace(".", "").replace("-", "").replace(" ", "")
    text = re.sub(r"[^0-9K]", "", text)
    if len(text) < 2:
        return ""
    body, dv = text[:-1], text[-1]
    body = body.lstrip("0") or "0"
    return f"{body}-{dv}"


def normalize_region(value: object) -> str:
    """
    Normalize Chilean region names to stable keys.

    Handles variants such as:
    - La Araucanía vs Araucanía
    - Metropolitana de Santiago vs Región Metropolitana
    - Libertador Gral. Bernardo O'Higgins vs O'Higgins
    """
    text = strip_accents(value).upper()
    text = re.sub(r"[^A-Z0-9Ñ ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    if not text:
        return ""

    if "ARICA" in text and "PARINACOTA" in text:
        return "ARICA Y PARINACOTA"
    if "TARAPACA" in text:
        return "TARAPACA"
    if "ANTOFAGASTA" in text:
        return "ANTOFAGASTA"
    if "ATACAMA" in text:
        return "ATACAMA"
    if "COQUIMBO" in text:
        return "COQUIMBO"
    if "VALPARAISO" in text:
        return "VALPARAISO"
    if "METROPOLITANA" in text or "SANTIAGO" in text:
        return "METROPOLITANA"
    if "O HIGGINS" in text or "OHIGGINS" in text or "BERNARDO" in text:
        return "OHIGGINS"
    if "MAULE" in text:
        return "MAULE"
    if "NUBLE" in text or "ÑUBLE" in text:
        return "NUBLE"
    if "BIOBIO" in text or "BIO BIO" in text:
        return "BIOBIO"
    if "ARAUCANIA" in text:
        return "ARAUCANIA"
    if "LOS RIOS" in text or "RIOS" in text:
        return "LOS RIOS"
    if "LOS LAGOS" in text or "LAGOS" in text:
        return "LOS LAGOS"
    if "AYSEN" in text or "IBANEZ" in text or "IBAÑEZ" in text:
        return "AYSEN"
    if "MAGALLANES" in text or "ANTARTICA" in text:
        return "MAGALLANES"

    return text


def unique_join(series: Iterable[object], sep: str = " | ", max_items: int = 80) -> str:
    """Join unique non-empty values preserving order."""
    values: list[str] = []
    seen: set[str] = set()
    for item in series:
        if pd.isna(item):
            continue
        text = str(item).strip()
        if not text or text.lower() == "nan":
            continue
        if text not in seen:
            values.append(text)
            seen.add(text)
    if len(values) > max_items:
        return sep.join(values[:max_items]) + f" | ... (+{len(values) - max_items})"
    return sep.join(values)


def first_non_null(series: Iterable[object]) -> object:
    """Return first non-null/non-empty value."""
    for item in series:
        if pd.notna(item) and str(item).strip() != "":
            return item
    return np.nan


def parse_bool(value: object) -> bool:
    """Robust boolean parser for CSV values."""
    text = str(value).strip().upper()
    return text in {"TRUE", "1", "SI", "SÍ", "YES", "Y", "T"}


def haversine_km(lat1: object, lon1: object, lat2: object, lon2: object) -> float:
    """Great-circle distance in kilometers."""
    try:
        lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    except Exception:
        return np.nan

    if any(pd.isna(x) for x in [lat1, lon1, lat2, lon2]):
        return np.nan

    radius_km = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * radius_km * math.asin(math.sqrt(a))


def read_csv_utf8(path: Path) -> pd.DataFrame:
    """Read CSV trying common encodings."""
    for encoding in ("utf-8-sig", "utf-8", "latin1"):
        try:
            return pd.read_csv(path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, encoding="latin1")


def validate_required_columns(df: pd.DataFrame, required: list[str], file_label: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(
            f"Faltan columnas requeridas en {file_label}: {missing}\n"
            f"Columnas disponibles: {list(df.columns)}"
        )


def build_granularity_case(row: pd.Series) -> str:
    """Classify match ambiguity at establishment/RUT/region level."""
    if not row["CLIENTE_LIBRE_CEN_REGION"]:
        return "SIN_MATCH_CEN_RUT_REGION"

    try:
        n_est = int(row.get("N_ESTABLECIMIENTOS_RUT_REGION", 0))
        n_bar = int(row.get("N_BARRAS_CEN_RUT_REGION", 0))
    except Exception:
        return "MATCH_CEN_RUT_REGION_SIN_CONTEOS"

    if n_est == 1 and n_bar == 1:
        return "1_EST_1_BARRA_MISMA_REGION"
    if n_est > 1 and n_bar == 1:
        return "N_EST_1_BARRA_MISMA_REGION"
    if n_est == 1 and n_bar > 1:
        return "1_EST_N_BARRAS_MISMA_REGION"
    if n_est > 1 and n_bar > 1:
        return "N_EST_N_BARRAS_MISMA_REGION"

    return "MATCH_CEN_RUT_REGION_REVISAR"


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cruzar establecimientos ACTECOS/SII con Clientes Libres CEN georreferenciados por RUT + región."
    )
    parser.add_argument(
        "--establecimientos",
        type=Path,
        default=ROOT / "data/processed/Resumen_Establecimientos_con_ACTECOS.csv",
        help="Ruta al CSV Resumen_Establecimientos_con_ACTECOS.csv",
    )
    parser.add_argument(
        "--cen-georef",
        type=Path,
        default=ROOT / "data/processed/coordinador_electrico/Clientes_Libres_CEN_2025_georreferenciados_subestaciones.csv",
        help="Ruta al CSV de Clientes Libres CEN georreferenciados con subestaciones.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=ROOT / "data/processed/cruces_cen",
        help="Carpeta de salida.",
    )
    parser.add_argument(
        "--diagnostico-rut",
        type=str,
        default="",
        help="RUT para generar diagnóstico específico (ej. 88680500-4). Omitir para no generar.",
    )
    args = parser.parse_args()

    establecimientos_path = args.establecimientos
    cen_path = args.cen_georef
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if not establecimientos_path.exists():
        raise FileNotFoundError(f"No existe la base de establecimientos: {establecimientos_path}")

    if not cen_path.exists():
        raise FileNotFoundError(f"No existe la base CEN georreferenciada: {cen_path}")

    est = read_csv_utf8(establecimientos_path)
    cen = read_csv_utf8(cen_path)

    # Remover columnas índice accidentales, por ejemplo Unnamed: 0.
    est = est.loc[:, ~est.columns.astype(str).str.match(r"^Unnamed")]
    cen = cen.loc[:, ~cen.columns.astype(str).str.match(r"^Unnamed")]

    validate_required_columns(
        est,
        ["CODIGO_VU", "RAZON_SOCIAL", "RUT_RAZON_SOCIAL", "NOMBRE_ESTABLECIMIENTO", "REGION", "MATCH_ACTECOS_SII"],
        "Resumen_Establecimientos_con_ACTECOS.csv",
    )

    validate_required_columns(
        cen,
        [
            "RUT CLIENTE", "BARRA", "NOMBRE O RAZÓN SOCIAL DEL CLIENTE",
            "SUMINISTRADOR", "CONECTADO A LA RED DE", "SUBESTACION_REGION",
            "SUBESTACION_NOMBRE", "SUBESTACION_NEMOTECNICO",
            "SUBESTACION_LATITUD_WGS84", "SUBESTACION_LONGITUD_WGS84",
        ] + MONTH_COLS,
        "Clientes_Libres_CEN_2025_georreferenciados_subestaciones.csv",
    )

    # Normalización de consumos mensuales.
    for col in MONTH_COLS:
        cen[col] = pd.to_numeric(cen[col], errors="coerce").fillna(0.0)

    if "CONSUMO_TOTAL_MWH_2025_CEN" not in cen.columns:
        cen["CONSUMO_TOTAL_MWH_2025_CEN"] = cen[MONTH_COLS].sum(axis=1)
    else:
        cen["CONSUMO_TOTAL_MWH_2025_CEN"] = pd.to_numeric(
            cen["CONSUMO_TOTAL_MWH_2025_CEN"], errors="coerce"
        ).fillna(cen[MONTH_COLS].sum(axis=1))

    # Llaves normalizadas.
    cen["RUT_CLEAN"] = cen["RUT CLIENTE"].map(normalize_rut)
    est["RUT_CLEAN"] = est["RUT_RAZON_SOCIAL"].map(normalize_rut)

    cen["REGION_KEY"] = cen["SUBESTACION_REGION"].map(normalize_region)
    est["REGION_KEY"] = est["REGION"].map(normalize_region)

    est["MATCH_ACTECOS_SII_BOOL"] = est["MATCH_ACTECOS_SII"].map(parse_bool)

    # Solo se pueden usar filas CEN con RUT y región de subestación disponibles.
    cen["CEN_REGION_GEOREFERENCIADA"] = cen["REGION_KEY"].ne("")
    cen_usado = cen[cen["RUT_CLEAN"].ne("") & cen["REGION_KEY"].ne("")].copy()

    # -------------------------------------------------------------------------
    # 1) Consolidado CEN por RUT + región + barra.
    # -------------------------------------------------------------------------
    detail_keys = ["RUT_CLEAN", "REGION_KEY", "BARRA"]

    detail_agg: dict[str, object] = {
        "RUT CLIENTE": first_non_null,
        "NOMBRE O RAZÓN SOCIAL DEL CLIENTE": first_non_null,
        "SUBESTACION_REGION": first_non_null,
        "SUMINISTRADOR": unique_join,
        "CONECTADO A LA RED DE": unique_join,
        "SUBESTACION_ID": first_non_null if "SUBESTACION_ID" in cen_usado.columns else first_non_null,
        "SUBESTACION_NOMBRE": first_non_null,
        "SUBESTACION_NEMOTECNICO": first_non_null,
        "SUBESTACION_PROVINCIA": first_non_null if "SUBESTACION_PROVINCIA" in cen_usado.columns else first_non_null,
        "SUBESTACION_COMUNA": first_non_null if "SUBESTACION_COMUNA" in cen_usado.columns else first_non_null,
        "SUBESTACION_COORD_ESTE": first_non_null if "SUBESTACION_COORD_ESTE" in cen_usado.columns else first_non_null,
        "SUBESTACION_COORD_NORTE": first_non_null if "SUBESTACION_COORD_NORTE" in cen_usado.columns else first_non_null,
        "SUBESTACION_HUSO": first_non_null if "SUBESTACION_HUSO" in cen_usado.columns else first_non_null,
        "SUBESTACION_LONGITUD_WGS84": first_non_null,
        "SUBESTACION_LATITUD_WGS84": first_non_null,
        "SUBESTACION_MATCH_STATUS": unique_join if "SUBESTACION_MATCH_STATUS" in cen_usado.columns else first_non_null,
        "CONSUMO_TOTAL_MWH_2025_CEN": "sum",
    }

    if "SUBESTACION_MATCH_SCORE" in cen_usado.columns:
        detail_agg["SUBESTACION_MATCH_SCORE"] = "max"

    for col in MONTH_COLS:
        detail_agg[col] = "sum"

    # Evitar columnas opcionales no existentes.
    detail_agg = {col: agg for col, agg in detail_agg.items() if col in cen_usado.columns}

    cen_rut_region_barra = (
        cen_usado
        .groupby(detail_keys, dropna=False)
        .agg(detail_agg)
        .reset_index()
    )

    raw_detail_counts = (
        cen_usado.groupby(detail_keys, dropna=False)
        .size()
        .reset_index(name="N_FILAS_CEN_ORIGEN_RUT_REGION_BARRA")
    )

    cen_rut_region_barra = cen_rut_region_barra.merge(
        raw_detail_counts, on=detail_keys, how="left"
    )

    cen_rut_region_barra = cen_rut_region_barra.rename(
        columns={
            "RUT CLIENTE": "RUT_CLIENTE_CEN",
            "NOMBRE O RAZÓN SOCIAL DEL CLIENTE": "RAZON_SOCIAL_CLIENTE_CEN",
            "SUBESTACION_REGION": "SUBESTACION_REGION_CEN",
            "CONSUMO_TOTAL_MWH_2025_CEN": "CONSUMO_TOTAL_MWH_2025_CEN_BARRA",
            **{col: f"{col}_MWH_CEN_BARRA" for col in MONTH_COLS},
        }
    )

    # -------------------------------------------------------------------------
    # 2) Consolidado CEN por RUT + región.
    # -------------------------------------------------------------------------
    region_agg: dict[str, object] = {
        "RUT CLIENTE": first_non_null,
        "NOMBRE O RAZÓN SOCIAL DEL CLIENTE": first_non_null,
        "SUBESTACION_REGION": first_non_null,
        "SUMINISTRADOR": unique_join,
        "CONECTADO A LA RED DE": unique_join,
        "BARRA": unique_join,
        "SUBESTACION_NOMBRE": unique_join,
        "SUBESTACION_NEMOTECNICO": unique_join,
        "SUBESTACION_COMUNA": unique_join if "SUBESTACION_COMUNA" in cen_usado.columns else first_non_null,
        "SUBESTACION_PROVINCIA": unique_join if "SUBESTACION_PROVINCIA" in cen_usado.columns else first_non_null,
        "SUBESTACION_MATCH_STATUS": unique_join if "SUBESTACION_MATCH_STATUS" in cen_usado.columns else first_non_null,
        "CONSUMO_TOTAL_MWH_2025_CEN": "sum",
    }

    for col in MONTH_COLS:
        region_agg[col] = "sum"

    region_agg = {col: agg for col, agg in region_agg.items() if col in cen_usado.columns}

    cen_rut_region = (
        cen_usado
        .groupby(["RUT_CLEAN", "REGION_KEY"], dropna=False)
        .agg(region_agg)
        .reset_index()
    )

    cen_rut_region = cen_rut_region.rename(
        columns={
            "RUT CLIENTE": "RUT_CLIENTE_CEN",
            "NOMBRE O RAZÓN SOCIAL DEL CLIENTE": "RAZON_SOCIAL_CLIENTE_CEN",
            "SUBESTACION_REGION": "SUBESTACION_REGION_CEN",
            "SUMINISTRADOR": "SUMINISTRADOR_CEN_RUT_REGION",
            "CONECTADO A LA RED DE": "CONECTADO_A_LA_RED_DE_CEN_RUT_REGION",
            "BARRA": "BARRAS_CEN_RUT_REGION",
            "SUBESTACION_NOMBRE": "SUBESTACIONES_CEN_RUT_REGION",
            "SUBESTACION_NEMOTECNICO": "NEMOTECNICOS_SUBESTACIONES_CEN_RUT_REGION",
            "SUBESTACION_COMUNA": "COMUNAS_SUBESTACIONES_CEN_RUT_REGION",
            "SUBESTACION_PROVINCIA": "PROVINCIAS_SUBESTACIONES_CEN_RUT_REGION",
            "SUBESTACION_MATCH_STATUS": "SUBESTACION_MATCH_STATUS_CEN_RUT_REGION",
            "CONSUMO_TOTAL_MWH_2025_CEN": "CONSUMO_TOTAL_MWH_2025_CEN_RUT_REGION",
            **{col: f"{col}_MWH_CEN_RUT_REGION" for col in MONTH_COLS},
        }
    )

    counts_barra = (
        cen_rut_region_barra
        .groupby(["RUT_CLEAN", "REGION_KEY"])
        .agg(
            N_BARRAS_CEN_RUT_REGION=("BARRA", "nunique"),
            N_SUBESTACIONES_CEN_RUT_REGION=("SUBESTACION_NOMBRE", "nunique")
            if "SUBESTACION_NOMBRE" in cen_rut_region_barra.columns
            else ("BARRA", "nunique"),
            N_FILAS_CEN_CONSOLIDADAS_RUT_REGION=("BARRA", "size"),
        )
        .reset_index()
    )

    counts_raw = (
        cen_usado
        .groupby(["RUT_CLEAN", "REGION_KEY"])
        .size()
        .reset_index(name="N_FILAS_CEN_ORIGINALES_RUT_REGION")
    )

    cen_rut_region = (
        cen_rut_region
        .merge(counts_barra, on=["RUT_CLEAN", "REGION_KEY"], how="left")
        .merge(counts_raw, on=["RUT_CLEAN", "REGION_KEY"], how="left")
    )

    # -------------------------------------------------------------------------
    # 3) Base de establecimientos enriquecida sin multiplicar filas.
    # -------------------------------------------------------------------------
    est_counts = (
        est
        .groupby(["RUT_CLEAN", "REGION_KEY"])
        .agg(
            N_ESTABLECIMIENTOS_RUT_REGION=("CODIGO_VU", "count"),
            CODIGOS_VU_RUT_REGION=("CODIGO_VU", lambda s: unique_join(s.astype(str), sep=" | ")),
            ESTABLECIMIENTOS_RUT_REGION=("NOMBRE_ESTABLECIMIENTO", unique_join),
        )
        .reset_index()
    )

    est_enriched = est.merge(est_counts, on=["RUT_CLEAN", "REGION_KEY"], how="left")

    base_enriquecida = est_enriched.merge(
        cen_rut_region,
        on=["RUT_CLEAN", "REGION_KEY"],
        how="left",
        indicator=True,
    )

    base_enriquecida["CLIENTE_LIBRE_CEN_REGION"] = base_enriquecida["_merge"].eq("both")
    # Alias para mantener compatibilidad con scripts anteriores.
    base_enriquecida["CLIENTE_LIBRE_CEN"] = base_enriquecida["CLIENTE_LIBRE_CEN_REGION"]
    base_enriquecida = base_enriquecida.drop(columns=["_merge"])

    base_enriquecida["CASO_GRANULARIDAD_CEN_REGION"] = base_enriquecida.apply(
        build_granularity_case, axis=1
    )

    base_enriquecida["REQUIERE_REVISION_GEO"] = (
        base_enriquecida["CLIENTE_LIBRE_CEN_REGION"]
        & ~base_enriquecida["CASO_GRANULARIDAD_CEN_REGION"].eq("1_EST_1_BARRA_MISMA_REGION")
    )

    # -------------------------------------------------------------------------
    # 4) Tabla puente establecimiento x barra candidata y distancia referencial.
    # -------------------------------------------------------------------------
    puente = est.merge(
        cen_rut_region_barra,
        on=["RUT_CLEAN", "REGION_KEY"],
        how="inner",
    )

    if {"LATITUD", "LONGITUD"}.issubset(puente.columns) and {
        "SUBESTACION_LATITUD_WGS84",
        "SUBESTACION_LONGITUD_WGS84",
    }.issubset(puente.columns):
        puente["DISTANCIA_KM_EST_SUBESTACION"] = [
            haversine_km(lat_est, lon_est, lat_se, lon_se)
            for lat_est, lon_est, lat_se, lon_se in zip(
                puente["LATITUD"],
                puente["LONGITUD"],
                puente["SUBESTACION_LATITUD_WGS84"],
                puente["SUBESTACION_LONGITUD_WGS84"],
            )
        ]
        puente["RANK_DISTANCIA_SUBESTACION_MISMO_RUT_REGION"] = (
            puente.groupby("CODIGO_VU")["DISTANCIA_KM_EST_SUBESTACION"]
            .rank(method="first", ascending=True)
        )
        puente["ES_CANDIDATA_MAS_CERCANA_MISMO_RUT_REGION"] = (
            puente["RANK_DISTANCIA_SUBESTACION_MISMO_RUT_REGION"].eq(1)
        )
    else:
        puente["DISTANCIA_KM_EST_SUBESTACION"] = np.nan
        puente["RANK_DISTANCIA_SUBESTACION_MISMO_RUT_REGION"] = np.nan
        puente["ES_CANDIDATA_MAS_CERCANA_MISMO_RUT_REGION"] = False

    # Agregar a base_enriquecida una barra candidata más cercana solo como referencia.
    nearest_cols = [
        "CODIGO_VU", "BARRA", "SUBESTACION_NOMBRE", "SUBESTACION_NEMOTECNICO",
        "SUBESTACION_REGION_CEN", "SUBESTACION_COMUNA", "SUBESTACION_PROVINCIA",
        "SUBESTACION_LATITUD_WGS84", "SUBESTACION_LONGITUD_WGS84",
        "DISTANCIA_KM_EST_SUBESTACION", "CONSUMO_TOTAL_MWH_2025_CEN_BARRA",
    ]
    nearest_cols = [col for col in nearest_cols if col in puente.columns]

    nearest = puente[puente["ES_CANDIDATA_MAS_CERCANA_MISMO_RUT_REGION"]].copy()
    nearest = nearest[nearest_cols].rename(
        columns={
            "BARRA": "BARRA_CEN_CANDIDATA_MAS_CERCANA",
            "SUBESTACION_NOMBRE": "SUBESTACION_CEN_CANDIDATA_MAS_CERCANA",
            "SUBESTACION_NEMOTECNICO": "NEMOTECNICO_SUBESTACION_CEN_CANDIDATA_MAS_CERCANA",
            "SUBESTACION_REGION_CEN": "REGION_SUBESTACION_CEN_CANDIDATA_MAS_CERCANA",
            "SUBESTACION_COMUNA": "COMUNA_SUBESTACION_CEN_CANDIDATA_MAS_CERCANA",
            "SUBESTACION_PROVINCIA": "PROVINCIA_SUBESTACION_CEN_CANDIDATA_MAS_CERCANA",
            "SUBESTACION_LATITUD_WGS84": "LATITUD_SUBESTACION_CEN_CANDIDATA_MAS_CERCANA",
            "SUBESTACION_LONGITUD_WGS84": "LONGITUD_SUBESTACION_CEN_CANDIDATA_MAS_CERCANA",
            "DISTANCIA_KM_EST_SUBESTACION": "DISTANCIA_KM_EST_SUBESTACION_CEN_CANDIDATA_MAS_CERCANA",
            "CONSUMO_TOTAL_MWH_2025_CEN_BARRA": "CONSUMO_TOTAL_MWH_2025_CEN_BARRA_CANDIDATA",
        }
    )

    if not nearest.empty:
        base_enriquecida = base_enriquecida.merge(nearest, on="CODIGO_VU", how="left")

    base_enriquecida["BARRA_CANDIDATA_METODO"] = np.where(
        ~base_enriquecida["CLIENTE_LIBRE_CEN_REGION"],
        "SIN_MATCH_CEN_RUT_REGION",
        np.where(
            base_enriquecida["N_BARRAS_CEN_RUT_REGION"].fillna(0).eq(1),
            "UNICA_BARRA_RUT_REGION",
            "BARRA_MAS_CERCANA_REFERENCIAL_NO_CONFIRMADA",
        ),
    )

    # -------------------------------------------------------------------------
    # 5) Base final filtrada: ACTECOS/SII + Cliente Libre CEN por RUT + región.
    # -------------------------------------------------------------------------
    base_final = base_enriquecida[
        base_enriquecida["MATCH_ACTECOS_SII_BOOL"]
        & base_enriquecida["CLIENTE_LIBRE_CEN_REGION"]
    ].copy()

    # -------------------------------------------------------------------------
    # 6) Guardar salidas.
    # -------------------------------------------------------------------------
    output_base_enriquecida = out_dir / "Base_Establecimientos_ACTECOS_SII_clientes_libres_CEN_RUT_REGION_2025.csv"
    output_base_final = out_dir / "Base_Final_CupraTherm_ACTECOS_SII_Clientes_Libres_CEN_RUT_REGION_2025.csv"
    output_cen_rut_region = out_dir / "CEN_Clientes_Libres_consolidado_RUT_REGION_2025.csv"
    output_cen_rut_region_barra = out_dir / "CEN_Clientes_Libres_consolidado_RUT_REGION_BARRA_2025.csv"
    output_puente = out_dir / "Puente_Establecimientos_CEN_RUT_REGION_BARRA_2025_para_revision.csv"
    output_casos = out_dir / "Resumen_casos_granularidad_CEN_RUT_REGION_2025.csv"
    output_report = out_dir / "reporte_cruce_clientes_libres_CEN_RUT_REGION_2025.txt"

    base_enriquecida.to_csv(output_base_enriquecida, index=False, encoding="utf-8-sig")
    base_final.to_csv(output_base_final, index=False, encoding="utf-8-sig")
    cen_rut_region.to_csv(output_cen_rut_region, index=False, encoding="utf-8-sig")
    cen_rut_region_barra.to_csv(output_cen_rut_region_barra, index=False, encoding="utf-8-sig")
    puente.to_csv(output_puente, index=False, encoding="utf-8-sig")

    casos = (
        base_enriquecida
        .groupby("CASO_GRANULARIDAD_CEN_REGION")
        .agg(
            FILAS=("CODIGO_VU", "count"),
            RUTS_UNICOS=("RUT_CLEAN", "nunique"),
            DEMANDA_CALOR_MWH=("DEMANDA_CALOR_MWH", "sum")
            if "DEMANDA_CALOR_MWH" in base_enriquecida.columns
            else ("CODIGO_VU", "count"),
        )
        .reset_index()
    )
    casos.to_csv(output_casos, index=False, encoding="utf-8-sig")

    # Diagnóstico opcional para un RUT específico.
    diag_text = ""
    if args.diagnostico_rut:
        rut_diag = normalize_rut(args.diagnostico_rut)
        diag_est = base_enriquecida[base_enriquecida["RUT_CLEAN"].eq(rut_diag)].copy()
        diag_cen = cen_rut_region_barra[cen_rut_region_barra["RUT_CLEAN"].eq(rut_diag)].copy()
        diag_est_path = out_dir / f"diagnostico_establecimientos_RUT_{rut_diag}_CEN_RUT_REGION.csv"
        diag_cen_path = out_dir / f"diagnostico_barras_RUT_{rut_diag}_CEN_RUT_REGION_BARRA.csv"
        diag_est.to_csv(diag_est_path, index=False, encoding="utf-8-sig")
        diag_cen.to_csv(diag_cen_path, index=False, encoding="utf-8-sig")
        diag_text = (
            f"\nDiagnóstico RUT {rut_diag}:\n"
            f"- Establecimientos encontrados: {len(diag_est):,}\n"
            f"- Regiones de establecimientos: {unique_join(diag_est['REGION']) if 'REGION' in diag_est.columns else ''}\n"
            f"- Barras CEN RUT+región: {len(diag_cen):,}\n"
            f"- Archivo establecimientos: {diag_est_path}\n"
            f"- Archivo barras: {diag_cen_path}\n"
        )

    consumo_base_final_repetido = float(
        base_final["CONSUMO_TOTAL_MWH_2025_CEN_RUT_REGION"].sum()
    ) if "CONSUMO_TOTAL_MWH_2025_CEN_RUT_REGION" in base_final.columns else 0.0

    # Para evitar duplicación por múltiples establecimientos, el consumo agregado debe
    # calcularse en el consolidado CEN RUT+región o deduplicando la base final.
    pares_final = base_final[["RUT_CLEAN", "REGION_KEY"]].drop_duplicates()
    consumo_final_deduplicado = (
        pares_final
        .merge(
            cen_rut_region[["RUT_CLEAN", "REGION_KEY", "CONSUMO_TOTAL_MWH_2025_CEN_RUT_REGION"]],
            on=["RUT_CLEAN", "REGION_KEY"],
            how="left",
        )["CONSUMO_TOTAL_MWH_2025_CEN_RUT_REGION"]
        .sum()
    )

    report = f"""REPORTE CRUCE CLIENTES LIBRES CEN GEOREFERENCIADOS POR RUT + REGION

Entradas:
- Clientes Libres CEN georreferenciados: {cen_path}
- Establecimientos ACTECOS/SII: {establecimientos_path}

Dimensiones:
- Filas establecimientos: {len(est):,}
- RUT únicos establecimientos: {est["RUT_CLEAN"].nunique():,}
- Filas CEN georreferenciadas entrada: {len(cen):,}
- Filas CEN usadas con RUT y región válida: {len(cen_usado):,}
- RUT únicos CEN usados: {cen_usado["RUT_CLEAN"].nunique():,}
- Grupos CEN RUT + región: {len(cen_rut_region):,}
- Grupos CEN RUT + región + barra: {len(cen_rut_region_barra):,}

Resultado del match RUT + región:
- Filas establecimientos con CLIENTE_LIBRE_CEN_REGION=True: {int(base_enriquecida["CLIENTE_LIBRE_CEN_REGION"].sum()):,}
- RUT únicos con match RUT + región: {base_enriquecida.loc[base_enriquecida["CLIENTE_LIBRE_CEN_REGION"], "RUT_CLEAN"].nunique():,}
- Filas finales con MATCH_ACTECOS_SII=True y CLIENTE_LIBRE_CEN_REGION=True: {len(base_final):,}
- RUT únicos base final: {base_final["RUT_CLEAN"].nunique():,}

Consumo eléctrico CEN:
- Suma directa en base final, con posible repetición por establecimiento, MWh 2025: {consumo_base_final_repetido:,.3f}
- Suma deduplicada por RUT + región para la base final, MWh 2025: {consumo_final_deduplicado:,.3f}

Nota metodológica:
- La base final mantiene granularidad de establecimiento. Si un mismo RUT+región tiene varios establecimientos,
  el consumo regional CEN queda repetido en cada fila de establecimiento.
- Para análisis agregados de consumo eléctrico, usar CEN_Clientes_Libres_consolidado_RUT_REGION_2025.csv
  o deduplicar por RUT_CLEAN + REGION_KEY.
- La columna BARRA_CEN_CANDIDATA_MAS_CERCANA es referencial. No confirma conexión eléctrica del establecimiento;
  solo ordena candidatas por distancia geográfica dentro del mismo RUT + región.

Casos de granularidad:
{casos.to_string(index=False)}

Distribución de estados de match de subestación en CEN entrada:
{cen["SUBESTACION_MATCH_STATUS"].value_counts(dropna=False).to_string() if "SUBESTACION_MATCH_STATUS" in cen.columns else "No disponible"}
{diag_text}
Archivos generados:
- {output_base_enriquecida}
- {output_base_final}
- {output_cen_rut_region}
- {output_cen_rut_region_barra}
- {output_puente}
- {output_casos}
- {output_report}
"""

    output_report.write_text(report, encoding="utf-8")

    print(report)


if __name__ == "__main__":
    main()
