import os.path
import polars as pl
from tqdm import tqdm


def vaski_parser():
    vaski_fname = os.path.join("data", "raw", "VaskiData.tsv")

    for vaski_data in tqdm(
        pl.read_csv(vaski_fname, separator="\t", has_header=True).iter_slices(
            n_rows=10_000
        ),
        total=31,
    ):  # Total achieved by experiment
        vaski_data = vaski_data.with_columns(
            pl.col("XmlData").str.extract(r"VASKI_JULKVP_(\w+)").alias("doctype")
        )
        vaski_data = vaski_data.filter(pl.col("doctype").str.ends_with("_fi"))

        for (doctype,), sub_vaski in vaski_data.group_by("doctype"):
            f_path = os.path.join("data", "raw", "vaski", f"{doctype}.tsv")
            sub_vaski = sub_vaski.drop("doctype")
            with open(f_path, mode="a" if os.path.exists(f_path) else "w") as f:
                sub_vaski.write_csv(f, separator="\t", include_header=f.mode == "w")


if __name__ == "__main__":
    vaski_parser()
