import os.path
import pandas as pd
import numpy as np
from lxml import etree
from tqdm import tqdm


def vaski_parser():
    pd.options.mode.copy_on_write = True  # Silence a warning about pandas. See https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy

    vaski_fname = os.path.join("data", "raw", "VaskiData.tsv")

    for vaski_data in tqdm(pd.read_csv(vaski_fname, sep="\t", chunksize=10000), total=31):  # Total achieved by experiment
        vaski_data['Xml'] = vaski_data["XmlData"].apply(
            lambda x: etree.fromstring(x)
        )
        vaski_data = vaski_data[  # Only keep finnish files
            vaski_data['Xml'].apply(
                lambda x: x[0][0].text.endswith('_fi')
            )
        ]
        vaski_data['doctype'] = vaski_data['Xml'].apply(
            lambda x: x[0][0].text[13:]  # All doctypes start with VASKI_JULKVP_
        )

        for doctype in vaski_data['doctype'].unique():
            sub_vaski = vaski_data[vaski_data['doctype'] == doctype].drop(
                columns=["doctype", 'Xml']
            )
            f_path = os.path.join("data", "raw", "vaski", f"{doctype}.tsv")            
            if not os.path.exists(f_path):  # If no file, write header
                sub_vaski.to_csv(f_path, sep="\t", header=True, index=False)
            else:  # If file, no header and append
                sub_vaski.to_csv(f_path, sep="\t", header=False, index=False, mode='a')

if __name__ == "__main__":
    vaski_parser()


