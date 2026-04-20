#!/usr/bin/env python3
"""
Processeur de données INPI/BCE pour l'outil de benchmark sectoriel.

Télécharge le jeu de données officiel depuis data.gouv.fr, en extrait les
ratios utiles à l'outil (parmi les 95 colonnes disponibles), ne conserve
que l'année la plus récente par (NAF, tranche CA), et produit un JSON
compact.

Le CSV source est au format "large" :
    classe_naf ; classe_ca ; exercice ; <ratio>_q10 ; <ratio>_q25 ; ...

Usage :
    python3 process_inpi.py [--output benchmark_inpi.json] [--pretty]
"""

import argparse
import csv
import io
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen, Request

DATA_URL = "https://www.data.gouv.fr/api/1/datasets/r/1222d433-267f-45ea-88bc-43b372d882c7"

# ---------------------------------------------------------------------------
# Mapping direct préfixe_colonne CSV → ratio interne de l'outil
# ---------------------------------------------------------------------------
# Une ligne du CSV = une cohorte (NAF, tranche, année)
# Chaque ratio occupe 5 colonnes (q10, q25, q50, q75, q90)
RATIO_COLUMNS = {
    "margeBrute": "part_ca_marge_brute",       # % du CA — proxy marge brute
    "tauxEBE": "part_ca_ebe",                  # EBE / CA
    "rentaExpl": "part_ca_ebit",               # EBIT / CA ≈ rentabilité exploitation
    "netCA": "part_ca_resultat_net",           # Résultat net / CA
    "bfrJours": "poids_bfr_exploitation_sur_ca_jours",  # BFR en jours de CA
    "dso": "credit_clients_jours",             # Délai règlement clients
    "dpo": "credit_fournisseurs_jours",        # Délai règlement fournisseurs
    # Ratios non disponibles dans ce dataset :
    # - tauxVA (taux de valeur ajoutée)
    # - persoVA (charges de personnel / VA)
    # - persoCA (charges de personnel / CA)
    # Ils peuvent être ajoutés manuellement via l'interface de l'outil
    # pour certains secteurs clés (via Banque de France par exemple).
}

PERCENTILES = ["q10", "q25", "q50", "q75", "q90"]


def download_csv(url: str) -> str:
    """Télécharge le CSV et retourne son contenu en texte UTF-8."""
    print(f"→ Téléchargement depuis {url}", file=sys.stderr)
    req = Request(url, headers={"User-Agent": "benchmark-fec/1.0"})
    with urlopen(req, timeout=180) as resp:
        data = resp.read()
    text = data.decode("utf-8", errors="replace")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if text.startswith("\ufeff"):
        text = text[1:]
    print(f"  {len(data):,} octets reçus ({text.count(chr(10)):,} lignes)",
          file=sys.stderr)
    return text


def detect_separator(first_line: str) -> str:
    """Détecte le séparateur CSV (tab, pipe, point-virgule, virgule)."""
    for sep in [";", "\t", "|"]:
        if sep in first_line:
            return sep
    return ","


def parse_num(v):
    """Parse un nombre (gère virgule et point). Retourne None si invalide."""
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "null", "na"):
        return None
    try:
        return float(s.replace(",", "."))
    except (ValueError, TypeError):
        return None


def process_dataset(csv_text: str, verbose: bool = True):
    """Traite le CSV et retourne la structure compactée."""
    if not csv_text:
        return {"_meta": {}, "naf": {}}

    first_line = csv_text.split("\n", 1)[0]
    sep = detect_separator(first_line)
    if verbose:
        print(f"→ Séparateur détecté : {sep!r}", file=sys.stderr)

    reader = csv.DictReader(io.StringIO(csv_text, newline=""), delimiter=sep)
    fields = reader.fieldnames or []
    fields = [f.lstrip("\ufeff") if isinstance(f, str) else f for f in fields]
    # Retire le BOM aussi du fieldnames original pour que row.get() fonctionne
    clean_fields = {}
    for orig in reader.fieldnames or []:
        clean = orig.lstrip("\ufeff") if isinstance(orig, str) else orig
        clean_fields[clean] = orig

    def find_key(candidates):
        for c in candidates:
            if c in clean_fields:
                return clean_fields[c]
        for clean_name, orig_name in clean_fields.items():
            if clean_name.lower() == c.lower():
                return orig_name
        return None

    naf_col = find_key(["classe_naf", "code_naf", "code_activite", "naf"])
    tranche_col = find_key(["classe_ca", "tranche_ca",
                            "tranche_chiffre_affaires", "tranche"])
    annee_col = find_key(["exercice", "annee_exercice", "annee", "year"])

    if not (naf_col and tranche_col and annee_col):
        raise RuntimeError(
            f"Colonnes structurantes manquantes. "
            f"naf={naf_col!r}, tranche={tranche_col!r}, exercice={annee_col!r}."
        )
    if verbose:
        print(f"→ Colonnes structurantes : naf={naf_col!r}, "
              f"tranche={tranche_col!r}, exercice={annee_col!r}",
              file=sys.stderr)

    # Vérifie la présence des colonnes de ratios attendues
    available_ratios = {}
    missing = []
    for internal_id, prefix in RATIO_COLUMNS.items():
        cols = {p: f"{prefix}_{p}" for p in PERCENTILES}
        # Cherche dans clean_fields (sans BOM) et récupère le nom original
        resolved = {}
        all_found = True
        for p, col_name in cols.items():
            if col_name in clean_fields:
                resolved[p] = clean_fields[col_name]
            else:
                all_found = False
                break
        if all_found:
            available_ratios[internal_id] = resolved
        else:
            missing.append((internal_id, prefix))

    if verbose:
        print(f"→ Ratios mappés : {len(available_ratios)}/{len(RATIO_COLUMNS)}",
              file=sys.stderr)
        for internal_id in available_ratios:
            print(f"    ✓ {internal_id} ← {RATIO_COLUMNS[internal_id]}_*",
                  file=sys.stderr)
        for internal_id, prefix in missing:
            print(f"    ✗ {internal_id} ← {prefix}_* (colonnes manquantes)",
                  file=sys.stderr)

    if not available_ratios:
        raise RuntimeError("Aucun ratio mappé. Vérifiez la structure du CSV.")

    # (naf, tranche) -> {"year": int, "ratios": {internal_id: {q10..q90}}}
    bucket = {}
    row_count = 0

    for row in reader:
        row_count += 1
        naf = (row.get(naf_col) or "").strip()
        tranche = (row.get(tranche_col) or "tous").strip()
        year_raw = (row.get(annee_col) or "0").strip()
        if not naf:
            continue
        try:
            year = int(float(year_raw))
        except (ValueError, TypeError):
            year = 0

        ratios_values = {}
        for internal_id, cols in available_ratios.items():
            values = {p: parse_num(row.get(cols[p])) for p in PERCENTILES}
            if values["q50"] is not None:
                ratios_values[internal_id] = values

        if not ratios_values:
            continue

        key = (naf, tranche)
        if key not in bucket or bucket[key]["year"] < year:
            bucket[key] = {"year": year, "ratios": ratios_values}

    if verbose:
        print(f"→ {row_count:,} lignes CSV traitées", file=sys.stderr)
        print(f"→ {len(bucket):,} combinaisons (NAF, tranche) retenues "
              f"(année la plus récente)", file=sys.stderr)

    # Regroupement final par NAF > tranche > ratio
    by_naf = defaultdict(lambda: {"label": "", "tranches": {}})
    for (naf, tranche), item in bucket.items():
        by_naf[naf]["label"] = naf
        by_naf[naf]["tranches"][tranche] = item["ratios"]

    result_naf = {
        naf: {"label": data["label"], "tranches": dict(data["tranches"])}
        for naf, data in sorted(by_naf.items())
    }

    mapping_info = {k: RATIO_COLUMNS[k] for k in available_ratios}

    return {
        "_meta": {
            "source": "INPI/BCE via data.gouv.fr",
            "source_url": DATA_URL,
            "generated_at": datetime.now(timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%SZ"),
            "naf_count": len(result_naf),
            "total_cohortes": len(bucket),
            "rows_processed": row_count,
            "ratio_mapping": mapping_info,
            "ratios_not_in_dataset": ["tauxVA", "persoVA", "persoCA"],
        },
        "naf": result_naf,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", "-o", default="benchmark_inpi.json",
                    help="Fichier JSON de sortie (défaut: benchmark_inpi.json)")
    ap.add_argument("--input", "-i", default=None,
                    help="Fichier CSV local (si absent, télécharge depuis data.gouv.fr)")
    ap.add_argument("--pretty", action="store_true",
                    help="JSON indenté (plus gros fichier)")
    ap.add_argument("--quiet", action="store_true",
                    help="Supprime les logs détaillés")
    args = ap.parse_args()

    if args.input:
        print(f"→ Lecture de {args.input}", file=sys.stderr)
        csv_text = Path(args.input).read_text(encoding="utf-8")
    else:
        csv_text = download_csv(DATA_URL)

    print("→ Traitement...", file=sys.stderr)
    result = process_dataset(csv_text, verbose=not args.quiet)

    output_path = Path(args.output)
    with output_path.open("w", encoding="utf-8") as f:
        if args.pretty:
            json.dump(result, f, ensure_ascii=False, indent=2)
        else:
            json.dump(result, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = output_path.stat().st_size / 1024
    print(f"✓ {output_path} généré ({size_kb:,.0f} Ko, "
          f"{result['_meta']['naf_count']:,} codes NAF, "
          f"{result['_meta'].get('total_cohortes', 0):,} cohortes)",
          file=sys.stderr)


if __name__ == "__main__":
    main()
