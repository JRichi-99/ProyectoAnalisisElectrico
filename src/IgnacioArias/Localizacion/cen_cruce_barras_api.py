#!/usr/bin/env python3
"""Cruce de BARRA de Clientes Libres CEN con mnemotécnicos Infotécnica y coordenadas vía API SIP."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

try:
    from rapidfuzz import fuzz, process

    HAVE_RAPIDFUZZ = True
except ImportError:
    import difflib

    HAVE_RAPIDFUZZ = False

DEFAULT_BASE_URL = "https://sipub.api.coordinador.cl"
DEFAULT_PREFIXES = ["/api/v2/recursos", "/api/recursos/v2"]

DEFAULT_INPUT = "data/raw/2025-12-CLIENTES-Libres-Inf-33-TDLC.xlsx"
DEFAULT_OUTPUT = "data/processed/clientes_libres_con_mnemotecnico_y_geo.csv"
DEFAULT_MAPPING_OUTPUT = "data/processed/mapping_barras_cen.csv"


def normalize_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("Ñ", "N")
    text = re.sub(r"_+", " ", text)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_barra_cliente(barra: Any) -> dict[str, Any]:
    raw = "" if pd.isna(barra) else str(barra).strip()
    raw_upper = raw.upper()
    kv = None
    match = re.search(r"_+(\d{2,3})\s*$", raw_upper)
    if match:
        try:
            kv = int(match.group(1))
        except ValueError:
            kv = None
    base = re.sub(r"_+\d{2,3}\s*$", "", raw_upper)
    return {
        "BARRA_ORIGINAL": raw,
        "BARRA_BASE_NORMALIZADA": normalize_text(base),
        "BARRA_NORMALIZADA_COMPLETA": normalize_text(raw_upper),
        "BARRA_KV": kv,
    }


def extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("results", "content", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict):
            for nested_value in value.values():
                if isinstance(nested_value, list):
                    return [x for x in nested_value if isinstance(x, dict)]
    return []


def has_next_page(payload: Any, records: list[dict[str, Any]], limit: int, offset: int) -> bool:
    if isinstance(payload, dict):
        if payload.get("next"):
            return True
        count = payload.get("count") or payload.get("totalElements")
        if isinstance(count, int):
            return offset + limit < count
        total_pages = payload.get("totalPages")
        page = payload.get("page")
        if isinstance(total_pages, int) and isinstance(page, int):
            return page + 1 < total_pages
    return len(records) == limit


def request_with_retry(url: str, params: dict[str, Any], max_retries: int = 4) -> requests.Response:
    response = None
    for attempt in range(max_retries + 1):
        response = requests.get(url, params=params, timeout=60)
        if response.status_code == 429:
            wait = min(60, 5 * (2**attempt))
            print(f"[WARN] Rate limit 429. Esperando {wait} s...")
            time.sleep(wait)
            continue
        if response.status_code in (500, 502, 503, 504):
            wait = min(60, 3 * (2**attempt))
            print(f"[WARN] Error servidor {response.status_code}. Reintentando en {wait} s...")
            time.sleep(wait)
            continue
        return response
    return response  # type: ignore[return-value]


def fetch_all_resource(
    resource: str,
    api_key: str,
    base_url: str = DEFAULT_BASE_URL,
    prefixes: list[str] | None = None,
    limit: int = 1000,
    extra_params: dict[str, Any] | None = None,
) -> pd.DataFrame:
    resource_clean = resource.strip("/")
    prefixes = prefixes or DEFAULT_PREFIXES
    last_error = None

    for prefix in prefixes:
        prefix_clean = "/" + prefix.strip("/")
        endpoint = f"{base_url.rstrip('/')}{prefix_clean}/{resource_clean}/"
        print(f"[INFO] Probando endpoint: {endpoint}")

        records_all: list[dict[str, Any]] = []
        offset = 0
        first_page_ok = False

        while True:
            params: dict[str, Any] = {
                "user_key": api_key,
                "limit": limit,
                "offset": offset,
            }
            if extra_params:
                params.update(extra_params)

            response = request_with_retry(endpoint, params=params)

            if response.status_code == 404 and offset == 0:
                last_error = f"404 en {endpoint}"
                break
            if response.status_code >= 400:
                raise RuntimeError(
                    f"HTTP {response.status_code} en {endpoint}: {response.text[:300]}"
                )

            payload = response.json()
            records = extract_records(payload)
            first_page_ok = True
            records_all.extend(records)
            print(
                f"[INFO] {resource_clean}: offset={offset}, "
                f"registros={len(records)}, acumulado={len(records_all)}"
            )

            if not has_next_page(payload, records, limit, offset):
                break
            offset += limit
            time.sleep(0.2)

        if first_page_ok:
            return pd.DataFrame(records_all)

    raise RuntimeError(f"No se pudo descargar el recurso {resource_clean}. Último error: {last_error}")


def safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def infer_lat_lon(a: float | None, b: float | None) -> tuple[float | None, float | None]:
    if a is None or b is None:
        return None, None
    if -56 <= a <= -17 and -90 <= b <= -60:
        return a, b
    if -56 <= b <= -17 and -90 <= a <= -60:
        return b, a
    return a, b


def parse_coordinates(value: Any) -> tuple[float | None, float | None]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None, None

    obj: Any = value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None, None
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            obj = text

    if isinstance(obj, dict):
        keys = {k.lower(): k for k in obj.keys()}
        lat_key = keys.get("lat") or keys.get("latitude") or keys.get("latitud")
        lon_key = keys.get("lon") or keys.get("lng") or keys.get("longitude") or keys.get("longitud")
        if lat_key and lon_key:
            return safe_float(obj.get(lat_key)), safe_float(obj.get(lon_key))

    if isinstance(obj, list):
        if len(obj) >= 2 and not isinstance(obj[0], (list, dict)):
            return infer_lat_lon(safe_float(obj[0]), safe_float(obj[1]))
        if len(obj) >= 1 and isinstance(obj[0], dict):
            return parse_coordinates(obj[0])
        if len(obj) >= 1 and isinstance(obj[0], list) and len(obj[0]) >= 2:
            return infer_lat_lon(safe_float(obj[0][0]), safe_float(obj[0][1]))

    nums = re.findall(r"-?\d+(?:\.\d+)?", str(obj))
    if len(nums) >= 2:
        return infer_lat_lon(float(nums[0]), float(nums[1]))
    return None, None


def build_lookup_tables(
    llaves: pd.DataFrame, barras: pd.DataFrame, subestaciones: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ll = llaves.copy()
    if not ll.empty:
        ll["llave_nombre_natural_norm"] = ll.get("llave_nombre_natural", pd.Series(dtype=str)).map(
            normalize_text
        )
        ll["llave_nombre_base_norm"] = ll["llave_nombre_natural_norm"].map(
            lambda x: re.sub(r"\s+\d{2,3}$", "", x).strip()
        )

    ba = barras.copy()
    if not ba.empty:
        ba["nombre_barra_norm"] = ba.get("nombre", pd.Series(dtype=str)).map(normalize_text)
        ba["mnemotecnico_barra_norm"] = ba.get("mnemotecnico", pd.Series(dtype=str)).map(normalize_text)

    se = subestaciones.copy()
    if not se.empty:
        se = se.rename(
            columns={
                "mnemotecnico": "mnemotecnico_subestacion",
                "nombre": "nombre_subestacion",
                "region": "region_subestacion",
                "coordenadas": "coordenadas_subestacion",
            }
        )
        coords = se.get("coordenadas_subestacion", pd.Series([None] * len(se))).map(parse_coordinates)
        se["latitud"] = coords.map(lambda x: x[0])
        se["longitud"] = coords.map(lambda x: x[1])

    return ll, ba, se


def fuzzy_match(query: str, choices: list[str]) -> tuple[str | None, float]:
    if not query or not choices:
        return None, 0.0
    if HAVE_RAPIDFUZZ:
        result = process.extractOne(query, choices, scorer=fuzz.token_sort_ratio)
        if result is None:
            return None, 0.0
        return result[0], float(result[1])
    matches = difflib.get_close_matches(query, choices, n=1, cutoff=0.0)
    if not matches:
        return None, 0.0
    score = difflib.SequenceMatcher(None, query, matches[0]).ratio() * 100
    return matches[0], score


def create_mapping(
    input_barras: pd.Series,
    llaves: pd.DataFrame,
    barras_info: pd.DataFrame,
    subestaciones: pd.DataFrame,
    min_score: float,
) -> pd.DataFrame:
    parsed = pd.DataFrame(
        [parse_barra_cliente(x) for x in sorted(input_barras.dropna().astype(str).unique())]
    )

    llaves_by_full: dict[str, pd.Series] = {}
    llaves_by_base: dict[str, pd.Series] = {}
    if not llaves.empty:
        for _, row in llaves.iterrows():
            full = row.get("llave_nombre_natural_norm", "")
            base = row.get("llave_nombre_base_norm", "")
            if full and full not in llaves_by_full:
                llaves_by_full[full] = row
            if base and base not in llaves_by_base:
                llaves_by_base[base] = row
        choices_base = list(llaves_by_base.keys())
    else:
        choices_base = []

    barras_by_mnemo: dict[str, pd.Series] = {}
    barras_by_name: dict[str, pd.Series] = {}
    if not barras_info.empty:
        for _, row in barras_info.iterrows():
            mnemo = normalize_text(row.get("mnemotecnico", ""))
            name = row.get("nombre_barra_norm", "")
            if mnemo:
                barras_by_mnemo[mnemo] = row
            if name and name not in barras_by_name:
                barras_by_name[name] = row
        choices_barra_name = list(barras_by_name.keys())
    else:
        choices_barra_name = []

    output_rows = []
    for _, p in parsed.iterrows():
        full_query = p["BARRA_NORMALIZADA_COMPLETA"]
        base_query = p["BARRA_BASE_NORMALIZADA"]

        match_method = "sin_match"
        score = 0.0
        llave_row = None
        barra_row = None
        mnemotecnico_barra = None
        llave_nombre = None

        if full_query in llaves_by_full:
            llave_row = llaves_by_full[full_query]
            match_method = "exacto_llave_nombre_natural"
            score = 100.0
        elif base_query in llaves_by_base:
            llave_row = llaves_by_base[base_query]
            match_method = "exacto_llave_base"
            score = 100.0
        else:
            best_key, best_score = fuzzy_match(base_query, choices_base)
            if best_key and best_score >= min_score:
                llave_row = llaves_by_base[best_key]
                match_method = "fuzzy_llave_base"
                score = best_score

        if llave_row is not None:
            mnemotecnico_barra = llave_row.get("mnemotecnico_barra")
            llave_nombre = llave_row.get("llave_nombre_natural")
            barra_row = barras_by_mnemo.get(normalize_text(mnemotecnico_barra))

        if barra_row is None:
            best_key, best_score = fuzzy_match(base_query, choices_barra_name)
            if best_key and best_score >= min_score:
                barra_row = barras_by_name[best_key]
                mnemotecnico_barra = barra_row.get("mnemotecnico")
                if match_method == "sin_match":
                    match_method = "fuzzy_nombre_barra_infotecnica"
                else:
                    match_method += "+fuzzy_info"
                score = max(score, best_score)

        row_out = p.to_dict()
        row_out.update(
            {
                "MNEMOTECNICO_BARRA_CEN": mnemotecnico_barra,
                "NOMBRE_LLAVE_OPREAL": llave_nombre,
                "MATCH_METHOD": match_method,
                "MATCH_SCORE": round(score, 2),
            }
        )

        if barra_row is not None:
            row_out.update(
                {
                    "ID_BARRA_INFOTECNICA": barra_row.get("id_infotecnica"),
                    "NOMBRE_BARRA_INFOTECNICA": barra_row.get("nombre"),
                    "DESCRIPCION_BARRA_INFOTECNICA": barra_row.get("descripcion"),
                    "CODIGO_BARRA_INFOTECNICA": barra_row.get("codigo"),
                    "NUMERO_BARRA_INFOTECNICA": barra_row.get("numero"),
                    "PROPIETARIO_BARRA": barra_row.get("propietario"),
                    "MNEMOTECNICO_SUBESTACION": barra_row.get("subestacion"),
                }
            )
        else:
            row_out.update(
                {
                    "ID_BARRA_INFOTECNICA": None,
                    "NOMBRE_BARRA_INFOTECNICA": None,
                    "DESCRIPCION_BARRA_INFOTECNICA": None,
                    "CODIGO_BARRA_INFOTECNICA": None,
                    "NUMERO_BARRA_INFOTECNICA": None,
                    "PROPIETARIO_BARRA": None,
                    "MNEMOTECNICO_SUBESTACION": None,
                }
            )
        output_rows.append(row_out)

    mapping = pd.DataFrame(output_rows)

    if not subestaciones.empty and "MNEMOTECNICO_SUBESTACION" in mapping.columns:
        se_cols = [
            c
            for c in [
                "mnemotecnico_subestacion",
                "nombre_subestacion",
                "descripcion",
                "codigo",
                "numero",
                "propietario",
                "barra_set",
                "paño_set",
                "region_subestacion",
                "coordenadas_subestacion",
                "latitud",
                "longitud",
            ]
            if c in subestaciones.columns
        ]
        se_small = subestaciones[se_cols].drop_duplicates("mnemotecnico_subestacion")
        mapping = mapping.merge(
            se_small,
            left_on="MNEMOTECNICO_SUBESTACION",
            right_on="mnemotecnico_subestacion",
            how="left",
        )

    return mapping


def resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else (ROOT / path).resolve()


def read_input_table(path: Path, sheet: str, header_row: int) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet, header=header_row, dtype=str)
    return pd.read_csv(path, dtype=str)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cruza BARRA de Clientes Libres con mnemotécnicos y coordenadas del CEN/SIP."
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Archivo Excel o CSV de entrada.")
    parser.add_argument("--sheet", default="Clientes Libres", help="Hoja del Excel.")
    parser.add_argument("--header-row", type=int, default=3, help="Fila de encabezado en Excel (0-indexed).")
    parser.add_argument("--barra-col", default="BARRA", help="Columna con la barra operacional.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="CSV enriquecido de salida.")
    parser.add_argument("--mapping-output", default=DEFAULT_MAPPING_OUTPUT, help="CSV tabla puente de barras.")
    parser.add_argument(
        "--api-key",
        default=os.getenv("CEN_API_KEY"),
        help="API key SIP (recomendado: variable CEN_API_KEY en .env).",
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("CEN_API_BASE", DEFAULT_BASE_URL),
        help="Base URL de la API SIP.",
    )
    parser.add_argument("--limit", type=int, default=1000, help="Tamaño de página para API.")
    parser.add_argument("--min-score", type=float, default=88.0, help="Score mínimo para fuzzy matching.")
    return parser.parse_args()


def print_summary(mapping: pd.DataFrame, output_path: Path, mapping_path: Path) -> None:
    total = len(mapping)
    matched = int(mapping["MNEMOTECNICO_BARRA_CEN"].notna().sum())
    unmatched = total - matched
    pct = (matched / total * 100) if total else 0.0

    print("\n" + "=" * 60)
    print("RESUMEN CRUCE BARRAS CEN (API SIP)")
    print("=" * 60)
    print(f"Barras únicas totales:        {total:,}")
    print(f"Barras con match:             {matched:,}")
    print(f"Porcentaje de match:          {pct:.2f}%")
    print(f"Barras sin match:             {unmatched:,}")
    print(f"Salida base enriquecida:      {output_path}")
    print(f"Salida tabla puente:          {mapping_path}")
    print("=" * 60)

    sin_match = mapping.loc[mapping["MNEMOTECNICO_BARRA_CEN"].isna(), "BARRA_ORIGINAL"].head(20).tolist()
    if sin_match:
        print("\nPrimeras 20 barras sin match:")
        for barra in sin_match:
            print(f"  - {barra}")
        print("\nSugerencia: revisar manualmente o ajustar --min-score si los nombres vienen muy abreviados.")


def main() -> int:
    args = parse_args()

    if not args.api_key or args.api_key == "TU_API_KEY_REAL":
        raise SystemExit(
            "ERROR: Debes definir CEN_API_KEY en .env o como variable de entorno.\n"
            f"Copia .env.example a .env y completa tu API key del Coordinador."
        )

    input_path = resolve_path(args.input)
    output_path = resolve_path(args.output)
    mapping_path = resolve_path(args.mapping_output)

    if not input_path.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {input_path}")

    print(f"[INFO] Leyendo base de clientes libres: {input_path}")
    df = read_input_table(input_path, args.sheet, args.header_row)
    df = df.drop(columns=[c for c in df.columns if str(c).startswith("Unnamed")], errors="ignore")

    if args.barra_col not in df.columns:
        raise KeyError(
            f"No existe la columna '{args.barra_col}'. "
            f"Columnas disponibles: {list(df.columns)}"
        )

    print(f"[INFO] Filas entrada: {len(df):,}")
    print(f"[INFO] Barras únicas no nulas: {df[args.barra_col].dropna().nunique():,}")

    print("[INFO] Descargando tabla de llaves OPReal -> mnemotécnico barra...")
    llaves = fetch_all_resource("demanda_programada_llaves", args.api_key, args.base_url, limit=args.limit)

    print("[INFO] Descargando barras Infotécnica...")
    barras = fetch_all_resource("infotecnica/barras", args.api_key, args.base_url, limit=args.limit)

    print("[INFO] Descargando subestaciones Infotécnica...")
    subestaciones = fetch_all_resource(
        "infotecnica/subestaciones", args.api_key, args.base_url, limit=args.limit
    )

    llaves, barras, subestaciones = build_lookup_tables(llaves, barras, subestaciones)

    print("[INFO] Construyendo tabla puente BARRA -> MNEMOTECNICO_BARRA_CEN -> coordenadas...")
    mapping = create_mapping(df[args.barra_col], llaves, barras, subestaciones, args.min_score)

    df_out = df.merge(mapping, left_on=args.barra_col, right_on="BARRA_ORIGINAL", how="left")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    mapping_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(output_path, index=False, encoding="utf-8-sig")
    mapping.to_csv(mapping_path, index=False, encoding="utf-8-sig")

    print_summary(mapping, output_path, mapping_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
