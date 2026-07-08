#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enriquece la base de Clientes Libres del Coordinador Eléctrico Nacional con
información de subestaciones, nemotécnico y georreferenciación.

Entrada principal:
- data/raw/Coordinador Electrico Nacional/Clientes Libres/2025-12-CLIENTES-Libres-Inf-33-TDLC.xlsx
- data/raw/Coordinador Electrico Nacional/Subestaciones/reporte_subestaciones.xlsx

Salidas:
- Clientes_Libres_CEN_2025_georreferenciados_subestaciones.csv/.xlsx
- Catalogo_BARRA_CEN_match_subestaciones.csv
- BARRAS_CEN_requieren_revision_manual.csv
- reporte_match_subestaciones_CEN_2025.txt

Notas metodológicas:
- El match se realiza a nivel BARRA única, no fila a fila, para evitar trabajo repetido.
- La columna BARRA del CEN se parsea en nombre base y tensión, por ejemplo:
  A.BLANCAS_____013 -> nombre: A BLANCAS, tension: 13 kV.
- Se compara contra la columna Nombre de reporte_subestaciones.xlsx usando normalización,
  heurísticas de abreviaturas, niveles de tensión y fuzzy matching.
- Los matches de baja confianza quedan marcados para revisión manual.
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any, Optional

import pandas as pd

try:
    from rapidfuzz import fuzz, process
except ImportError:  # fallback simple si rapidfuzz no está instalado
    import difflib

    class _FuzzFallback:
        @staticmethod
        def token_set_ratio(a: str, b: str) -> float:
            return 100.0 * difflib.SequenceMatcher(None, a, b).ratio()

    fuzz = _FuzzFallback()
    process = None

try:
    from pyproj import Transformer
except ImportError:
    Transformer = None


MESES_2025 = [
    "ene-25", "feb-25", "mar-25", "abr-25", "may-25", "jun-25",
    "jul-25", "ago-25", "sept-25", "oct-25", "nov-25", "dic-25",
]

COL_TENSION_PATIOS = "5.1 Identificar patios por nivel de tensión.  (Artículo 19 Anexo Técnico 03/2025)"
COL_TENSION_BARRAS = "5.2 Barras por nivel de tensión y su respectiva capacidad térmica, en función de la T° ambiente y T° conductor (Tabla de Relación Corriente – Temperatura) (Artículo 19 Anexo Técnico 03/2025)"
COL_HUSO = "Zona o Huso [Ej: 18H-19J etc.]"

# Alias útiles para abreviaturas frecuentes en la columna BARRA.
# Puedes agregar nuevos casos aquí si el archivo de revisión manual identifica errores sistemáticos.
BARRA_ALIASES = {
    "A BLANCAS": "AGUAS BLANCAS",
    "AG BLANCAS": "AGUAS BLANCAS",
    "A HOSPICIO": "ALTO HOSPICIO",
    "A JAHUEL": "ALTO JAHUEL",
    "A DE CORDOVA": "ALONSO DE CORDOVA",
    "S F MOSTAZAL": "SAN FRANCISCO MOSTAZAL",
    "STA ROSA": "SANTA ROSA",
    "SANTA ROSA": "SANTA ROSA",
    "LOMIRANDA": "LO MIRANDA",
    "ELPEUMO": "EL PEUMO",
    "LAMANGA": "LA MANGA",
    "LASARANAS": "LAS ARANAS",
    "LAVEGA": "LA VEGA",
    "LACRUZ": "LA CRUZ",
    "M DEVELASCO": "MANSO DE VELASCO",
    "M V CEN": "MINERA VALLE CENTRAL",
    "TVITOR": "TAP OFF VITOR",
    "TBARRILES": "TAP OFF BARRILES",
    "TOFF DOLORES": "TAP OFF DOLORES",
    "TLIBERTADORES": "TAP OFF LIBERTADORES",
    "PUERTOPATACHE": "PUERTO PATACHE",
    "PIDPID": "PID PID",
    "SANPEDRO": "SAN PEDRO",
}

STOPWORDS_SUBESTACIONES = {
    "S", "E", "SE", "SUBESTACION", "SUBEST", "CENTRAL", "DIESEL",
    "ELECTRICA", "ELECTRICO", "LTDA", "SPA", "SA", "S A",
}


def strip_accents(value: Any) -> str:
    """Convierte a string y elimina tildes/diacríticos."""
    text = "" if pd.isna(value) else str(value)
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def normalize_text(value: Any, remove_stopwords: bool = True) -> str:
    """Normaliza texto para comparación robusta."""
    text = strip_accents(value).upper().replace("Ñ", "N")
    text = text.replace("S/E", " ").replace("S / E", " ")
    text = re.sub(r"[\._\-\/\(\),;:\[\]\{\}\n\r\t]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    if remove_stopwords:
        tokens = [tok for tok in text.split() if tok not in STOPWORDS_SUBESTACIONES]
        text = " ".join(tokens)

    return text


def parse_barra(barra: Any) -> tuple[str, Optional[int], str, str]:
    """
    Extrae nombre base y tensión desde la columna BARRA.
    Devuelve: nombre_parseado, tension_kv, nombre_normalizado, nombre_normalizado_alias.
    """
    raw = "" if pd.isna(barra) else str(barra).strip().replace("\xa0", " ")
    match = re.search(r"[_\s]*(\d{3})\s*$", raw)

    tension = None
    name = raw
    if match:
        tension = int(match.group(1))
        name = raw[: match.start()]

    name = re.sub(r"_+", " ", name)
    name = name.replace(".", " ")
    name = re.sub(r"\s+", " ", name).strip()

    normalized = normalize_text(name)
    normalized_alias = BARRA_ALIASES.get(normalized, normalized)
    normalized_alias = normalize_text(normalized_alias)
    return name, tension, normalized, normalized_alias


def parse_chilean_number(value: Any) -> Optional[float]:
    """Parsea números con formato chileno o internacional."""
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "no aplica", "movil", "móvil"}:
        return None

    # Si hay etiquetas tipo "V1: 704102,64\nV2: ...", toma el primer número plausible.
    match = re.search(r"[-+]?\d[\d\.,]*", text)
    if not match:
        return None
    text = match.group(0)

    # Formato chileno: 7.537.595,80 -> 7537595.80
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        return float(text)
    except ValueError:
        return None


def extract_utm_zone(huso: Any) -> Optional[int]:
    """Extrae zona UTM desde valores tipo 18H, 19J, 19K."""
    text = strip_accents(huso).upper()
    match = re.search(r"(18|19)", text)
    if not match:
        return None
    return int(match.group(1))


def utm_to_lonlat(easting: Any, northing: Any, huso: Any) -> tuple[Optional[float], Optional[float]]:
    """Convierte UTM sur a lon/lat WGS84 si pyproj está disponible."""
    if Transformer is None:
        return None, None

    east = parse_chilean_number(easting)
    north = parse_chilean_number(northing)
    zone = extract_utm_zone(huso)

    # Filtro básico para evitar convertir coordenadas que parecen lat/lon o textos no UTM.
    if east is None or north is None or zone is None:
        return None, None
    if not (100_000 <= east <= 900_000 and 3_500_000 <= north <= 8_200_000):
        return None, None

    epsg = 32700 + zone  # Chile continental: hemisferio sur, EPSG 32718/32719.
    transformer = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(east, north)
    return round(lon, 7), round(lat, 7)


def extract_tension_levels(row: pd.Series) -> list[float]:
    """Extrae niveles de tensión declarados en el reporte de subestaciones."""
    levels: list[float] = []

    # Buscar valores seguidos de kV en texto largo.
    for col in [COL_TENSION_PATIOS, COL_TENSION_BARRAS]:
        if col not in row:
            continue
        text = strip_accents(row.get(col, "")).upper()
        for match in re.finditer(r"(\d{1,3}(?:[\.,]\d+)?)\s*K\s*V", text):
            try:
                levels.append(float(match.group(1).replace(",", ".")))
            except ValueError:
                pass

    # En la columna 5.1 los niveles suelen venir como "220 110 13".
    if COL_TENSION_PATIOS in row:
        text = strip_accents(row.get(COL_TENSION_PATIOS, "")).upper()
        if len(text) < 150:
            for match in re.finditer(r"(?<!\d)(\d{1,3}(?:[\.,]\d+)?)(?!\d)", text):
                try:
                    levels.append(float(match.group(1).replace(",", ".")))
                except ValueError:
                    pass

    out: list[float] = []
    for value in levels:
        if 0.1 <= value <= 800 and value not in out:
            out.append(value)
    return out


def has_tension_match(barra_tension: Optional[int], levels: list[float]) -> Optional[bool]:
    """Evalúa si la tensión de BARRA calza con los niveles de la subestación."""
    if barra_tension is None or not levels:
        return None
    for level in levels:
        # 13 kV debe calzar con 13.2 o 13.8 kV; 015 con 15 kV, etc.
        if abs(level - barra_tension) <= 1.0 or int(level) == barra_tension or round(level) == barra_tension:
            return True
    return False


def candidate_penalty(nombre_original: str, barra_alias_norm: str) -> int:
    """Penaliza candidatos menos probables en empates."""
    nombre = normalize_text(nombre_original, remove_stopwords=False)
    penalty = 0

    barra_es_tap_off = "TAP OFF" in barra_alias_norm
    if "TAP OFF" in nombre and not barra_es_tap_off:
        penalty += 2
    if "TAP OFF" not in nombre and barra_es_tap_off:
        penalty += 1

    # Si hay empate, preferir la S/E base sobre versiones CENTRAL/DIESEL.
    if "CENTRAL" in nombre:
        penalty += 1
    if "DIESEL" in nombre:
        penalty += 1

    return penalty


def score_candidate(barra_norm: str, barra_alias_norm: str, barra_tension: Optional[int], sub_row: pd.Series) -> dict[str, Any]:
    """Calcula score de matching para una BARRA y una subestación candidata."""
    cand_norm = sub_row["_NOMBRE_NORM"]

    base_original = fuzz.token_set_ratio(barra_norm, cand_norm)
    base_alias = fuzz.token_set_ratio(barra_alias_norm, cand_norm)
    base = max(base_original, base_alias)
    score = float(base)

    barra_tokens = barra_alias_norm.split()
    cand_tokens = set(cand_norm.split())
    long_tokens = [tok for tok in barra_tokens if len(tok) >= 3]
    short_tokens = [tok for tok in barra_tokens if 1 <= len(tok) < 3]

    if long_tokens and all(tok in cand_tokens for tok in long_tokens):
        score += 15

    if short_tokens:
        short_ok = True
        for tok in short_tokens:
            if not any(ct.startswith(tok) for ct in cand_tokens):
                short_ok = False
                break
        if short_ok:
            score += 10

    tension_match = has_tension_match(barra_tension, sub_row["_NIVELES_TENSION"])
    if tension_match is True:
        score += 20
    elif tension_match is False:
        score -= 15

    if barra_alias_norm == cand_norm:
        score += 30

    penalty = candidate_penalty(str(sub_row["Nombre"]), barra_alias_norm)
    return {
        "score": round(score, 3),
        "base_score": round(float(base), 3),
        "tension_match": tension_match,
        "penalty": penalty,
    }


def classify_match(score: Optional[float], gap: Optional[float]) -> str:
    """Clasifica confianza del match automático."""
    if score is None:
        return "SIN_MATCH"
    if score >= 115 and (gap is None or gap >= 3):
        return "MATCH_ALTA_CONFIANZA"
    if score >= 100:
        return "MATCH_REVISAR"
    return "SIN_MATCH_ALTA_CONFIANZA"


def detect_header_row(path: Path, sheet_name: str, required_columns: set[str]) -> int:
    """Detecta fila de encabezado buscando columnas obligatorias."""
    preview = pd.read_excel(path, sheet_name=sheet_name, header=None, nrows=30)
    for idx, row in preview.iterrows():
        values = {str(x).strip() for x in row.dropna().tolist()}
        if required_columns.issubset(values):
            return int(idx)
    raise ValueError(f"No se pudo detectar encabezado en {path.name}, hoja {sheet_name}.")


def load_clientes_libres(cen_xlsx: Path) -> pd.DataFrame:
    """Carga hoja Clientes Libres del archivo CEN."""
    df = pd.read_excel(cen_xlsx, sheet_name="Clientes Libres", header=3, dtype=str)
    df = df.dropna(how="all").copy()
    if "BARRA" not in df.columns:
        raise ValueError("No se encontró la columna BARRA en la hoja Clientes Libres.")
    return df


def load_subestaciones(sub_xlsx: Path) -> pd.DataFrame:
    """Carga reporte de subestaciones detectando encabezado."""
    xls = pd.ExcelFile(sub_xlsx)
    sheet = "Informacion Subestaciones" if "Informacion Subestaciones" in xls.sheet_names else xls.sheet_names[0]
    header_row = detect_header_row(sub_xlsx, sheet, {"ID", "Nombre", "Nemotecnico"})
    df = pd.read_excel(sub_xlsx, sheet_name=sheet, header=header_row)
    df = df.dropna(subset=["Nombre"]).copy()

    required = ["ID", "Nombre", "Nemotecnico", "Coordenada Este", "Coordenada Norte", COL_HUSO]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas en reporte_subestaciones.xlsx: {missing}")

    df["_NOMBRE_NORM"] = df["Nombre"].map(normalize_text)
    df["_NIVELES_TENSION"] = df.apply(extract_tension_levels, axis=1)
    return df


def load_manual_overrides(path: Optional[Path]) -> pd.DataFrame:
    """Carga overrides manuales opcionales."""
    if not path or not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    if "BARRA" not in df.columns:
        raise ValueError("El archivo de overrides manuales debe contener la columna BARRA.")
    return df


def build_match_catalog(cen: pd.DataFrame, sub: pd.DataFrame, manual: pd.DataFrame) -> pd.DataFrame:
    """Construye un catálogo único BARRA -> subestación."""
    manual_by_barra = {}
    if not manual.empty:
        for _, row in manual.iterrows():
            barra = str(row.get("BARRA", "")).strip()
            if not barra:
                continue
            manual_by_barra[barra] = row

    records: list[dict[str, Any]] = []
    sub_by_id = {str(row["ID"]): row for _, row in sub.iterrows()}

    # Precomputos para acelerar el fuzzy matching.
    sub_reset = sub.reset_index(drop=True).copy()
    sub_choices = sub_reset["_NOMBRE_NORM"].fillna("").tolist()

    for barra in sorted(cen["BARRA"].dropna().astype(str).unique()):
        barra_name, barra_tension, barra_norm, barra_alias_norm = parse_barra(barra)
        manual_row = manual_by_barra.get(barra)

        if manual_row is not None:
            selected = None
            if "SUBESTACION_ID_MANUAL" in manual_row and pd.notna(manual_row["SUBESTACION_ID_MANUAL"]):
                selected = sub_by_id.get(str(manual_row["SUBESTACION_ID_MANUAL"]).strip())
            elif "SUBESTACION_NOMBRE_MANUAL" in manual_row and pd.notna(manual_row["SUBESTACION_NOMBRE_MANUAL"]):
                target = normalize_text(manual_row["SUBESTACION_NOMBRE_MANUAL"])
                mask = sub["_NOMBRE_NORM"].eq(target)
                if mask.any():
                    selected = sub.loc[mask].iloc[0]
            if selected is not None:
                records.append(make_catalog_record(
                    barra, barra_name, barra_tension, barra_norm, barra_alias_norm,
                    selected, score=999, base_score=999, gap=None, tension_match=None,
                    status="MATCH_MANUAL", top_candidates="MATCH MANUAL"
                ))
                continue

        # Selección inicial de candidatos. En vez de comparar contra todas las subestaciones
        # con lógica Python pura, rapidfuzz.process.extract hace el prefiltrado en C.
        candidate_idx: set[int] = set()
        if process is not None:
            for query in {barra_norm, barra_alias_norm}:
                for _, _, idx in process.extract(query, sub_choices, scorer=fuzz.token_set_ratio, limit=60):
                    candidate_idx.add(int(idx))
        else:
            candidate_idx.update(range(len(sub_reset)))

        # Agregar candidatos que contengan tokens largos relevantes del nombre de la barra.
        for token in [tok for tok in barra_alias_norm.split() if len(tok) >= 4]:
            mask = sub_reset["_NOMBRE_NORM"].str.contains(rf"\b{re.escape(token)}\b", regex=True, na=False)
            candidate_idx.update(mask[mask].index.tolist())

        candidates: list[tuple[float, int, int, pd.Series, dict[str, Any]]] = []
        for idx in candidate_idx:
            sub_row = sub_reset.iloc[idx]
            score_info = score_candidate(barra_norm, barra_alias_norm, barra_tension, sub_row)
            score = score_info["score"]
            if score >= 70:
                # Orden: mayor score, menor penalización, nombre más corto.
                candidates.append((score, -int(score_info["penalty"]), -len(str(sub_row["Nombre"])), sub_row, score_info))

        candidates.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
        top = candidates[:5]

        if not top:
            records.append({
                "BARRA": barra,
                "BARRA_NOMBRE_PARSEADO": barra_name,
                "BARRA_TENSION_KV": barra_tension,
                "BARRA_NORMALIZADA": barra_norm,
                "BARRA_NORMALIZADA_ALIAS": barra_alias_norm,
                "SUBESTACION_MATCH_STATUS": "SIN_CANDIDATOS",
            })
            continue

        best_score, _, _, best_row, best_info = top[0]
        second_score = top[1][0] if len(top) > 1 else None
        gap = round(best_score - second_score, 3) if second_score is not None else None
        status = classify_match(best_score, gap)
        top_candidates = " || ".join(
            f"{i+1}) {row['Nombre']} [ID={row['ID']}; score={score}; tension={info['tension_match']}]"
            for i, (score, _, _, row, info) in enumerate(top)
        )

        records.append(make_catalog_record(
            barra, barra_name, barra_tension, barra_norm, barra_alias_norm,
            best_row, score=best_score, base_score=best_info["base_score"], gap=gap,
            tension_match=best_info["tension_match"], status=status,
            top_candidates=top_candidates
        ))

    return pd.DataFrame(records)


def make_catalog_record(
    barra: str,
    barra_name: str,
    barra_tension: Optional[int],
    barra_norm: str,
    barra_alias_norm: str,
    selected: pd.Series,
    score: Optional[float],
    base_score: Optional[float],
    gap: Optional[float],
    tension_match: Optional[bool],
    status: str,
    top_candidates: str,
) -> dict[str, Any]:
    lon, lat = utm_to_lonlat(
        selected.get("Coordenada Este"),
        selected.get("Coordenada Norte"),
        selected.get(COL_HUSO),
    )
    return {
        "BARRA": barra,
        "BARRA_NOMBRE_PARSEADO": barra_name,
        "BARRA_TENSION_KV": barra_tension,
        "BARRA_NORMALIZADA": barra_norm,
        "BARRA_NORMALIZADA_ALIAS": barra_alias_norm,
        "SUBESTACION_MATCH_STATUS": status,
        "SUBESTACION_MATCH_SCORE": score,
        "SUBESTACION_MATCH_BASE_SCORE": base_score,
        "SUBESTACION_MATCH_GAP_TOP2": gap,
        "SUBESTACION_TENSION_MATCH": tension_match,
        "SUBESTACION_ID": selected.get("ID"),
        "SUBESTACION_NOMBRE": selected.get("Nombre"),
        "SUBESTACION_NEMOTECNICO": selected.get("Nemotecnico"),
        "SUBESTACION_REGION": selected.get("Región"),
        "SUBESTACION_PROVINCIA": selected.get("Provincia"),
        "SUBESTACION_COMUNA": selected.get("Comuna"),
        "SUBESTACION_COORD_ESTE": selected.get("Coordenada Este"),
        "SUBESTACION_COORD_NORTE": selected.get("Coordenada Norte"),
        "SUBESTACION_HUSO": selected.get(COL_HUSO),
        "SUBESTACION_LONGITUD_WGS84": lon,
        "SUBESTACION_LATITUD_WGS84": lat,
        "SUBESTACION_NIVELES_TENSION_REPORTE": "; ".join(map(str, selected.get("_NIVELES_TENSION", []))),
        "SUBESTACION_TOP_CANDIDATOS": top_candidates,
    }


def create_report(cen: pd.DataFrame, catalog: pd.DataFrame, enriched: pd.DataFrame) -> str:
    status_counts = catalog["SUBESTACION_MATCH_STATUS"].value_counts(dropna=False).to_string()
    high_statuses = {"MATCH_ALTA_CONFIANZA", "MATCH_MANUAL"}
    catalog_high = catalog["SUBESTACION_MATCH_STATUS"].isin(high_statuses).sum()
    rows_high = enriched["SUBESTACION_MATCH_STATUS"].isin(high_statuses).sum()

    report = f"""
REPORTE DE MATCH CEN CLIENTES LIBRES - SUBESTACIONES
====================================================

Archivo CEN enriquecido a nivel de filas de Clientes Libres.

Resumen:
- Filas Clientes Libres CEN: {len(cen):,}
- Barras únicas CEN: {catalog['BARRA'].nunique():,}
- Barras con match alta confianza/manual: {catalog_high:,}
- Filas CEN con match alta confianza/manual: {rows_high:,}

Conteo por estado de match, a nivel BARRA única:
{status_counts}

Criterio recomendado:
- Usar directamente las filas con SUBESTACION_MATCH_STATUS = MATCH_ALTA_CONFIANZA o MATCH_MANUAL.
- Revisar manualmente las filas con MATCH_REVISAR, SIN_MATCH_ALTA_CONFIANZA o SIN_CANDIDATOS.
- Para revisión manual, usar el archivo BARRAS_CEN_requieren_revision_manual.csv y completar SUBESTACION_ID_MANUAL.

Columnas geográficas:
- SUBESTACION_COORD_ESTE, SUBESTACION_COORD_NORTE y SUBESTACION_HUSO corresponden a las coordenadas originales del reporte.
- SUBESTACION_LONGITUD_WGS84 y SUBESTACION_LATITUD_WGS84 se generan solo si pyproj está instalado y las coordenadas parecen UTM válidas.
""".strip()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Cruza Clientes Libres CEN con reporte de subestaciones.")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[1], help="Raíz del proyecto CupraTherm.")
    parser.add_argument("--cen-xlsx", type=Path, default=None, help="Ruta al Excel de Clientes Libres CEN.")
    parser.add_argument("--subestaciones-xlsx", type=Path, default=None, help="Ruta al reporte_subestaciones.xlsx.")
    parser.add_argument("--manual-overrides", type=Path, default=None, help="CSV opcional de overrides manuales.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Carpeta de salida.")
    args = parser.parse_args()

    root = args.project_root.resolve()
    cen_xlsx = args.cen_xlsx or root / "data/raw/Coordinador Electrico Nacional/Clientes Libres/2025-12-CLIENTES-Libres-Inf-33-TDLC.xlsx"
    sub_xlsx = args.subestaciones_xlsx or root / "data/raw/Coordinador Electrico Nacional/Subestaciones/reporte_subestaciones.xlsx"
    output_dir = args.output_dir or root / "data/processed/coordinador_electrico"
    manual_path = args.manual_overrides or output_dir / "manual_match_barras_subestaciones.csv"

    if not cen_xlsx.exists():
        raise FileNotFoundError(f"No existe el archivo CEN: {cen_xlsx}")
    if not sub_xlsx.exists():
        raise FileNotFoundError(f"No existe el reporte de subestaciones: {sub_xlsx}")

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Leyendo Clientes Libres CEN: {cen_xlsx}")
    cen = load_clientes_libres(cen_xlsx)
    print(f"Leyendo reporte de subestaciones: {sub_xlsx}")
    sub = load_subestaciones(sub_xlsx)
    manual = load_manual_overrides(manual_path)

    print("Construyendo catálogo BARRA -> Subestación...")
    catalog = build_match_catalog(cen, sub, manual)

    enriched = cen.merge(catalog, on="BARRA", how="left")

    # Consumo anual por fila CEN, útil para priorizar revisiones.
    for col in MESES_2025:
        if col in enriched.columns:
            enriched[col] = pd.to_numeric(enriched[col], errors="coerce")
    existing_months = [col for col in MESES_2025 if col in enriched.columns]
    if existing_months:
        enriched["CONSUMO_TOTAL_MWH_2025_CEN"] = enriched[existing_months].sum(axis=1, min_count=1)

    # Exportar salidas.
    enriched_csv = output_dir / "Clientes_Libres_CEN_2025_georreferenciados_subestaciones.csv"
    enriched_xlsx = output_dir / "Clientes_Libres_CEN_2025_georreferenciados_subestaciones.xlsx"
    catalog_csv = output_dir / "Catalogo_BARRA_CEN_match_subestaciones.csv"
    review_csv = output_dir / "BARRAS_CEN_requieren_revision_manual.csv"
    report_txt = output_dir / "reporte_match_subestaciones_CEN_2025.txt"

    enriched.to_csv(enriched_csv, index=False, encoding="utf-8-sig")
    catalog.to_csv(catalog_csv, index=False, encoding="utf-8-sig")

    review_status = {"MATCH_REVISAR", "SIN_MATCH_ALTA_CONFIANZA", "SIN_CANDIDATOS"}
    review = catalog[catalog["SUBESTACION_MATCH_STATUS"].isin(review_status)].copy()
    if not review.empty:
        review.insert(1, "SUBESTACION_ID_MANUAL", "")
        review.insert(2, "SUBESTACION_NOMBRE_MANUAL", "")
        review.insert(3, "COMENTARIO_REVISION_MANUAL", "")
    review.to_csv(review_csv, index=False, encoding="utf-8-sig")

    # Excel opcional. Requiere openpyxl instalado en el entorno de pandas.
    try:
        with pd.ExcelWriter(enriched_xlsx, engine="openpyxl") as writer:
            enriched.to_excel(writer, sheet_name="Clientes Libres enriquecido", index=False)
            catalog.to_excel(writer, sheet_name="Catalogo BARRA match", index=False)
            review.to_excel(writer, sheet_name="Requiere revision", index=False)
    except Exception as exc:
        print(f"Aviso: no se pudo exportar Excel ({exc}). Se mantienen salidas CSV.", file=sys.stderr)

    report = create_report(cen, catalog, enriched)
    report_txt.write_text(report, encoding="utf-8")

    print("\nProceso finalizado.")
    print(report)
    print("\nArchivos generados:")
    print(f"- {enriched_csv}")
    print(f"- {catalog_csv}")
    print(f"- {review_csv}")
    print(f"- {report_txt}")
    if enriched_xlsx.exists():
        print(f"- {enriched_xlsx}")


if __name__ == "__main__":
    main()
