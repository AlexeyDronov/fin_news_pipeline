from pathlib import Path
import re
from typing import Literal
import pandas as pd
from flashtext import KeywordProcessor

def normalise_company_name(name: str) -> str:
    if not isinstance(name, str):
        return name

    # strip leading "DBA" alias prefix
    name = re.sub(r'^DBA\s+', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*\([^)]*\)', ' ', name)

    # strip bare .com
    name = re.sub(r'\.com\b', '', name)

    suffixes = [
        r'Class [A-Z]', r'Series [A-Z]', r'Ordinary Shares?', r'Common Shares?',
        r'Common Stock', r'Capital Stock', r'Incorporated', r'Corporation',
        r'Companies',
        r'Company', r'Holdings?', r'Holdlings', r'Limited', r'Group',
        r'Corp', r'Inc', r'Ltd', r'Plc', r'LLC', r'L\.L\.C', r'REIT',
        r'N\.V', r'NV', r'Co', r'of Beneficial Interest',
        r'Public Limited Company', r'Public', r'New'
    ]

    pattern = r'(?:[\s,-]+)(' + '|'.join(suffixes) + r')\.?\s*$'

    old_name = None
    while old_name != name:
        old_name = name
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)

    name = re.sub(r'^The\s+', '', name) # strip leading article "The"

    # Clean up dangling conjunctions, punctuation, whitespace
    name = re.sub(r'\s+(and|&)\s*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[,.-]\s*$', '', name)
    name = re.sub(r'\s+', ' ', name).strip()

    return name

def process_or_load_tickers(file_name: Literal["sp500.csv", "all.csv"] | None = None) -> pd.DataFrame:
    path_to_dir = Path(__file__).parent.resolve()
    ticker_file = file_name or "all.csv"
    storage_file = ticker_file.split(".")[0] + "_clean.parquet"
    
    if (path_to_dir / storage_file).exists():
        df = pd.read_parquet(path_to_dir / storage_file)
        return df

    df = pd.read_csv(Path(__file__).parent.resolve() / ticker_file)
    df["normalised_name"] = df["name"].apply(normalise_company_name)
    df.drop(["name", "price", "volume"], axis=1, inplace=True)
    
    df.to_parquet(path_to_dir / storage_file, engine="pyarrow", index=False)
    
    return df

def build_lookup(df: pd.DataFrame) -> tuple[dict, "KeywordProcessor"]:
    primary = (
        df.sort_values("marketCap", ascending=False)
          .drop_duplicates(subset="normalised_name")
    )
    name_to_ticker = {
        row.normalised_name: row.symbol
        for row in primary.itertuples() 
    }
    kp = KeywordProcessor(case_sensitive=False)
    for name, ticker in name_to_ticker.items():
        kp.add_keyword(name, ticker)

    return name_to_ticker, kp