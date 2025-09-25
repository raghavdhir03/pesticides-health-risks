# load_pdp_all_years.py
import zipfile, pandas as pd
from pathlib import Path

data_dir = Path("data/zipped")
out_dir = Path("data/parquet")
out_dir.mkdir(parents=True, exist_ok=True)

# Column headers from PDP Data Dictionary 2023
SAMPLES_COLS = [
    "SAMPLE_PK", "STATE", "YEAR", "MONTH", "DAY", "SITE", "COMMOD",
    "SOURCE_ID", "VARIETY", "ORIGIN", "COUNTRY", "DISTTYPE",
    "COMMTYPE", "CLAIM", "QUANTITY", "GROWST", "PACKST", "DISTST"
]

RESULTS_COLS = [
    "SAMPLE_PK", "COMMOD", "COMMTYPE", "LAB", "PESTCODE", "TESTCLASS",
    "CONCEN", "LOD", "CONUNIT", "CONFMETHOD", "CONFMETHOD2", "ANNOTATE",
    "QUANTITATE", "MEAN", "EXTRACT", "DETERMIN"
]

def process_year(zip_path: Path, year: int):
    samples_df, results_df = None, None
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            lname = name.lower()
            if lname.endswith("samples.txt"):
                with zf.open(name) as f:
                    samples_df = pd.read_csv(
                        f, sep="|", dtype=str, header=None,
                        names=SAMPLES_COLS, low_memory=False
                    )
            elif lname.endswith("results.txt"):
                with zf.open(name) as f:
                    results_df = pd.read_csv(
                        f, sep="|", dtype=str, header=None,
                        names=RESULTS_COLS, low_memory=False
                    )

    if samples_df is not None and results_df is not None:
        merged_df = results_df.merge(samples_df, on="SAMPLE_PK", how="left")
        # Drop duplicate columns (keep results side)
        merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
        out_path = out_dir / f"pdp_{year}_merged.parquet"
        merged_df.to_parquet(out_path, index=False)
        print(f"[{year}] Merged saved: {merged_df.shape} -> {out_path}")
    else:
        print(f"[{year}] WARNING: Missing Samples or Results in {zip_path.name}")

if __name__ == "__main__":
    for year in range(1992, 2024):
        zip_path = data_dir / f"{year}PDPDatabase.zip"
        if not zip_path.exists():
            print(f"[{year}] No ZIP found at {zip_path}")
            continue
        print(f"Processing {year}...")
        process_year(zip_path, year)
