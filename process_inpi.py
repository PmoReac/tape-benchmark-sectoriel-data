#!/usr/bin/env python3
"""
Processeur de données INPI/BCE pour l'outil de benchmark sectoriel.

Télécharge le jeu de données officiel depuis data.gouv.fr, le filtre aux
ratios utiles à l'outil, ne conserve que l'année la plus récente par
(NAF, tranche CA, ratio), et produit un JSON compact.

Usage :
    python3 process_inpi.py [--output benchmark_inpi.json]

Le JSON produit a cette structure :
{
  "_meta": {
    "source": "INPI/BCE via data.gouv.fr",
    "generated_at": "2026-04-18T14:00:00Z",
    "naf_count": 732,
    "unmapped_ratios": ["rotation_stocks_jours", ...]
  },
  "naf": {
    "5610A": {
      "label": "Restauration traditionnelle",
      "tranches": {
        "0-250k": { "margeBrute": {"q10": 55, "q25": 62, ...}, ... },
        "250k-500k": { ... }
      }
    }
  }
}
"""

import argparse
import csv
import io
import json
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen, Request

DATA_URL = "https://www.data.gouv.fr/api/1/datasets/r/1222d433-267f-45ea-88bc-43b372d882c7"

# ---------------------------------------------------------------------------
# Mapping des noms de ratios INPI/BCE vers les identifiants internes de l'outil
# ---------------------------------------------------------------------------
# Chaque clé cible (ex. "margeBrute") a une liste de patterns candidats.
# Le matching est insensible à la casse, aux accents et aux séparateurs.
RATIO_MAP = {
    "margeBrute": [
        "taux_marge_commerciale", "taux de marge commerciale",
        "marge commerciale",
    ],
    "tauxVA": [
        "taux_valeur_ajoutee", "taux de valeur ajoutee",
        "tx_va", "valeur ajoutee",
    ],
    "tauxEBE": [
        "taux_marge_brute_exploitation", "taux_ebe", "tx_ebe",
        "taux ebe", "marge brute d exploitation",
    ],
    "rentaExpl": [
        "rentabilite_economique", "rentabilite exploitation",
        "rentabilite d exploitation",
    ],
    "netCA": [
        "rentabilite_nette", "taux de rentabilite nette",
        "resultat_net_ca", "resultat net sur ca",
    ],
    "dso": [
        "credit_client", "credit client", "delai_client",
        "delai clients", "dso",
    ],
    "dpo": [
        "credit_fournisseur", "credit fournisseur", "delai_fournisseur",
        "delai fournisseurs", "dpo",
    ],
    "bfrJours": [
        "besoin_fonds_roulement", "bfr",
        "besoin en fonds de roulement",
    ],
    "persoVA": [
        "poids_charges_personnel_va",
        "charges de personnel sur va",
    ],
    "persoCA": [
        "poids_charges_personnel",
        "charges de personnel sur ca",
    ],
}


def normalize(s: str) -> str:
    """Normalise une chaîne : minuscules, sans accent, séparateurs unifiés."""
    if s is None:
        return ""
    s = str(s).lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[_\s\-']+", " ", s).strip()
    return s


def build_matcher():
    """Construit un matcher : nom_ratio_inpi -> id_interne."""
    index = {}
    for target, patterns in RATIO_MAP.items():
        for p in patterns:
            index[normalize(p)] = target
    return index


def match_ratio(name: str, matcher: dict):
    """Tente de retrouver l'id interne à partir d'un nom de ratio INPI."""
    n = normalize(name)
    if not n:
        return None
    if n in matcher:
        return matcher[n]
    # Fallback : match par inclusion
    for pattern, target in matcher.items():
        if pattern in n or n in pattern:
            return target
    return None


def download_csv(url: str) -> str:
    """Télécharge le CSV et retourne son contenu en texte UTF-8."""
    print(f"→ Téléchargement depuis {url}", file=sys.stderr)
    req = Request(url, headers={"User-Agent": "benchmark-fec/1.0"})
    with urlopen(req, timeout=120) as resp:
        data = resp.read()
    text = data.decode("utf-8", errors="replace")
    print(f"  {len(data):,} octets reçus ({len(text.splitlines()):,} lignes)",
          file=sys.stderr)
    return text


def detect_separator(first_line: str) -> str:
    """Détecte le séparateur CSV (tab, pipe, point-virgule, virgule)."""
    for sep in ["\t", "|", ";"]:
        if sep in first_line:
            return sep
    return ","


def parse_rows(csv_text: str):
    """Parse le CSV et yield des dictionnaires ligne par ligne."""
    lines = csv_text.splitlines()
    if not lines:
        return
    sep = detect_separator(lines[0])
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=sep)
    for row in reader:
        yield row


def process_dataset(csv_text: str):
    """Traite le CSV et retourne la structure compactée."""
    matcher = build_matcher()

    # (naf, tranche, ratio) -> (year, values, label_naf)
    bucket = {}
    unmapped_counter = defaultdict(int)
    naf_labels = {}
    row_count = 0

    for row in parse_rows(csv_text):
        row_count += 1
        # Les noms de colonnes peuvent varier selon l'export
        naf = (row.get("code_activite") or row.get("code_naf")
               or row.get("naf") or "").strip()
        ratio_name = (row.get("nom_ratio") or row.get("ratio")
                      or row.get("indicateur")
                      or row.get("libelle_ratio") or "").strip()
        tranche = (row.get("tranche_chiffre_affaires")
                   or row.get("tranche_ca")
                   or row.get("tranche") or "tous").strip()
        year_raw = (row.get("annee_exercice") or row.get("annee")
                    or row.get("exercice") or "0").strip()
        naf_label = (row.get("libelle_activite")
                     or row.get("libelle_naf") or "").strip()

        if not naf or not ratio_name:
            continue

        try:
            year = int(float(year_raw))
        except (ValueError, TypeError):
            year = 0

        mapped = match_ratio(ratio_name, matcher)
        if not mapped:
            unmapped_counter[ratio_name] += 1
            continue

        def parse_num(v):
            if v is None or v == "":
                return None
            try:
                return float(str(v).replace(",", "."))
            except (ValueError, TypeError):
                return None

        values = {
            "q10": parse_num(row.get("q10")),
            "q25": parse_num(row.get("q25")),
            "q50": parse_num(row.get("q50")),
            "q75": parse_num(row.get("q75")),
            "q90": parse_num(row.get("q90")),
        }
        # Skip si pas de médiane
        if values["q50"] is None:
            continue

        key = (naf, tranche, mapped)
        if key not in bucket or bucket[key]["year"] < year:
            bucket[key] = {"year": year, "values": values}

        if naf_label and naf not in naf_labels:
            naf_labels[naf] = naf_label

    print(f"  {row_count:,} lignes traitées", file=sys.stderr)
    print(f"  {len(bucket):,} combinaisons (NAF, tranche, ratio) retenues",
          file=sys.stderr)
    if unmapped_counter:
        total_unmapped = sum(unmapped_counter.values())
        print(f"  {total_unmapped:,} lignes ignorées (ratios non mappés) : "
              f"{len(unmapped_counter)} libellés distincts", file=sys.stderr)

    # Regroupement final par NAF > tranche > ratio
    by_naf = defaultdict(lambda: {"label": "", "tranches": defaultdict(dict)})
    for (naf, tranche, ratio), item in bucket.items():
        by_naf[naf]["tranches"][tranche][ratio] = item["values"]
        if naf in naf_labels:
            by_naf[naf]["label"] = naf_labels[naf]

    # Conversion defaultdict -> dict pour la sérialisation JSON
    result_naf = {
        naf: {
            "label": data["label"] or naf,
            "tranches": dict(data["tranches"]),
        }
        for naf, data in sorted(by_naf.items())
    }

    return {
        "_meta": {
            "source": "INPI/BCE via data.gouv.fr",
            "source_url": DATA_URL,
            "generated_at": datetime.now(timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%SZ"),
            "naf_count": len(result_naf),
            "mappings": {k: v for k, v in
                         sorted(RATIO_MAP.items())},
            "top_unmapped_ratios": sorted(
                unmapped_counter.items(),
                key=lambda x: -x[1]
            )[:20],
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
    args = ap.parse_args()

    if args.input:
        print(f"→ Lecture de {args.input}", file=sys.stderr)
        csv_text = Path(args.input).read_text(encoding="utf-8")
    else:
        csv_text = download_csv(DATA_URL)

    print("→ Traitement...", file=sys.stderr)
    result = process_dataset(csv_text)

    output_path = Path(args.output)
    with output_path.open("w", encoding="utf-8") as f:
        if args.pretty:
            json.dump(result, f, ensure_ascii=False, indent=2)
        else:
            json.dump(result, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = output_path.stat().st_size / 1024
    print(f"✓ {output_path} généré ({size_kb:,.0f} Ko, "
          f"{result['_meta']['naf_count']:,} codes NAF)", file=sys.stderr)

    if result["_meta"]["top_unmapped_ratios"]:
        print("\nRatios INPI non mappés (top 10) :", file=sys.stderr)
        for name, count in result["_meta"]["top_unmapped_ratios"][:10]:
            print(f"  {count:>6,}  {name}", file=sys.stderr)
        print("\nPour mapper un ratio manquant, édite le dict RATIO_MAP "
              "en haut du script.", file=sys.stderr)


if __name__ == "__main__":
    main()
