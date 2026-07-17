
                                              
import os, zipfile, pathlib
from typing import Union, Iterable, Optional

import polars as pl
import pandas as pd

ENGINE_DEFAULT = os.getenv("ENGINE", "polars").lower()                        
PathLike = Union[str, pathlib.Path]

def _is_zip(path: PathLike) -> bool:
    return str(path).lower().endswith(".zip")

def read_csv_smart(path: PathLike, *, engine: Optional[str] = None, **kwargs):
    
    use = (engine or ENGINE_DEFAULT)
    if _is_zip(path):
        try:
            df_pd = pd.read_csv(path, dtype=str, low_memory=False, **kwargs)
        except Exception:
            with zipfile.ZipFile(path, "r") as zf:
                name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
                if name is None:
                    raise FileNotFoundError(f"No CSV found inside zip: {path}")
                with zf.open(name) as f:
                    df_pd = pd.read_csv(f, dtype=str, low_memory=False, **kwargs)
        df_pl = pl.from_pandas(df_pd, include_index=False)
        return to_lower(df_pl)
    else:
        if use == "polars":
            df_pl = pl.read_csv(path, try_parse_dates=True, ignore_errors=True, **kwargs)
            return to_lower(df_pl)
        else:
            df_pd = pd.read_csv(path, dtype=str, low_memory=False, **kwargs)
            return to_lower(df_pd)

def read_parquet_smart(path: PathLike, *, engine: Optional[str] = None):
    use = (engine or ENGINE_DEFAULT)
    if use == "polars":
        return to_lower(pl.read_parquet(path))
    else:
        return to_lower(pd.read_parquet(path))

def concat_smart(dfs: Iterable):
    it = iter(dfs)
    first = next(it)
    rest = list(it)
    if isinstance(first, pl.DataFrame):
        return pl.concat([first, *rest], how="vertical_relaxed")
    else:
        return pd.concat([first, *rest], axis=0, ignore_index=True)

def to_lower(df):
    if isinstance(df, pl.DataFrame):
        return df.rename({c: str(c).lower() for c in df.columns})
    else:
        df.columns = [str(c).lower() for c in df.columns]
        return df

def to_polars(df):
    return df if isinstance(df, pl.DataFrame) else pl.from_pandas(df, include_index=False)

def write_parquet(df, path: PathLike):
    path = str(path)
    if isinstance(df, pl.DataFrame):
        df.write_parquet(path)
    else:
        df.to_parquet(path, index=False)

def ensure_dir(path: PathLike):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
