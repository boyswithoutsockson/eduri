import os.path
import pandas as pd
import numpy as np
from lxml import etree


def vaski_parser():
    with open(os.path.join("data", "raw", "VaskiData.tsv"), "r") as f:
        vaski_data = pd.read_csv(f, sep="\t") #, nrows=10000)

    vaski_dict = {}
    for index, vaski in vaski_data.iterrows():
        xml = vaski.XmlData
        root = etree.fromstring(xml)
        name = root[0][0].text[13:]

        if name.endswith("_fi"):
            try: 
                vaski_dict[name] = pd.concat([vaski_dict[name], vaski.to_frame().T])

            except KeyError as e:
                vaski_dict[name] = vaski.to_frame().T
                vaski_dict[name].columns = ["Id", "XmlData", "Status", "Created", "Eduskuntatunnus", "AttachmentGroupId", "Imported"]
            
        elif name.endswith(("_sv", "_en")):
            continue

        else:
            try: 
                vaski_dict[name] = pd.concat([vaski_dict[name], vaski.to_frame().T])

            except KeyError as e:
                vaski_dict[name] = vaski.to_frame().T
                vaski_dict[name].columns = ["Id", "XmlData", "Status", "Created", "Eduskuntatunnus", "AttachmentGroupId", "Imported"]

    for datatype in vaski_dict:
        os.makedirs(os.path.join("data", "raw", "vaski"), exist_ok=True)
        vaski_dict[datatype].to_csv(os.path.join("data", "raw", "vaski", f"{datatype}.tsv"), sep="\t", header=True, index=False)

if __name__ == "__main__":
    vaski_parser()


