# save as scripts/pdp_local_etl.py and run:  python scripts/pdp_local_etl.py
import os, re, zipfile
from pathlib import Path
import pandas as pd

# ---- CONFIG ----
ROOT = Path(__file__).resolve().parents[1]
print(ROOT)         # repo/
ZIP_DIR = Path.home() / "Documents" / "pesticides-health-risks" / "data" / "zipped"
                
EXTRACT_TMP = ROOT / "data" / "tmp_unzipped"       # repo/data/tmp_unzipped
OUT_DIR = ROOT / "data" / "parquet"                # repo/data/parquet
CHUNKSIZE = 1_000_000                               # rows per chunk for Results

EXTRACT_TMP.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

def find_year_from_filename(name: str) -> int | None:
    m = re.search(r"(19|20)\d{2}", name)
    return int(m.group(0)) if m else None

def extract_zip(zpath: Path, outdir: Path) -> list[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zpath, "r") as zf:
        paths = []
        for member in zf.namelist():
            if member.endswith("/"):  # skip dirs
                continue
            dest = outdir / Path(member).name
            with zf.open(member) as src, open(dest, "wb") as dst:
                dst.write(src.read())
            paths.append(dest)
        return paths

def locate_txts(files: list[Path]) -> tuple[Path, Path]:
    samples = results = None
    for p in files:
        n = p.name.lower()
        if n.endswith(".txt"):
            if "sample" in n:
                samples = p
            elif "result" in n:
                results = p
    if not samples or not results:
        raise FileNotFoundError("Samples/Results .txt not found in bundle.")
    return samples, results

def standardize_samples(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    keep = [c for c in df.columns if c in {
        "SAMPLE_PK", "COMMOD", "STATE", "ORIGIN", "COUNTRY", "CLAIM",
        "VARIETY", "SAMPDATE", "GROWST", "PACKST", "DISTST"
    }]
    df = df[keep].copy()
    for c in df.columns:
        if c != "SAMPDATE":
            df[c] = df[c].astype("string")
    return df

def standardize_results_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    # keep common columns only (older years may differ)
    cols = [c for c in df.columns if c in {
        "SAMPLE_PK","PESTCODE","PESTICIDE","CONCEN","LOD","UNIT",
        "MEAN","EPATOL","TOLUNIT","TESTCLASS"
    }]
    df = df[cols].copy()

    for c in ("CONCEN","LOD","EPATOL"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("SAMPLE_PK","PESTCODE","PESTICIDE","UNIT","MEAN","TOLUNIT","TESTCLASS"):
        if c in df.columns:
            df[c] = df[c].astype("string")

    if "CONCEN" in df.columns:
        df["is_non_detect"] = df["CONCEN"].isna() | (df["CONCEN"] <= 0)
        df["is_detect"] = ~df["is_non_detect"]
    if {"CONCEN","EPATOL"} <= set(df.columns):
        df["above_tol"] = df["is_detect"] & df["EPATOL"].notna() & (df["CONCEN"] > df["EPATOL"])
    else:
        df["above_tol"] = pd.NA
    return df

def write_partition(df: pd.DataFrame, year: int, part_idx: int):
    part_dir = OUT_DIR / f"year={year}"
    part_dir.mkdir(parents=True, exist_ok=True)
    out_path = part_dir / f"part-{part_idx:06d}.parquet"
    df.to_parquet(out_path, index=False)  # requires pyarrow or fastparquet
    print(f"  wrote {out_path}")

def process_zip(zfile: Path):
    year = find_year_from_filename(zfile.name)
    if year is None:
        print(f"[skip] no year in filename: {zfile.name}")
        return
    print(f"\n[year {year}] processing {zfile.name}")

    # extract
    year_dir = EXTRACT_TMP / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    files = extract_zip(zfile, year_dir)

    # locate files
    samples_path, results_path = locate_txts(files)

    # read samples (small) and standardize
    samples = pd.read_csv(samples_path, sep="|", dtype=str, low_memory=False)
    samples = standardize_samples(samples)
    samples["YEAR"] = year

    # stream results in chunks
    part = 0
    for chunk in pd.read_csv(results_path, sep="|", dtype=str, low_memory=False, chunksize=CHUNKSIZE):
        chunk = standardize_results_chunk(chunk)
        merged = chunk.merge(samples, on="SAMPLE_PK", how="left")
        merged["YEAR"] = year
        write_partition(merged, year, part)
        part += 1

    print(f"[year {year}] done with {part} part(s).")

def main():
    zips = sorted([p for p in ZIP_DIR.glob("*.zip")] + [p for p in ZIP_DIR.glob("*.ZIP")])
    if not zips:
        raise SystemExit(f"No ZIPs found in {ZIP_DIR}")
    for z in zips:
        process_zip(z)

if __name__ == "__main__":
    main()