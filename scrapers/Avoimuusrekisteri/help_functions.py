import csv
import argparse
import asyncio
import polars as pl
import requests
from tqdm import tqdm

def fetch_targets():
    url = "https://public.api.avoimuusrekisteri.fi/open-data-target/targets"
    response = requests.get(url)
    return response.json()
target_columns = ["organization", "department", "unit", "title", "name"]

df = pl.DataFrame(fetch_targets())
df = df.with_columns([pl.col("fi").struct.field(k).alias(k) for k in target_columns]).drop(["createdAt", "hash", "termId", "fiId", "svId", "enId", "fi", "sv", "en"])
print(df.filter(pl.col("name") == "Riikka Purra"))
