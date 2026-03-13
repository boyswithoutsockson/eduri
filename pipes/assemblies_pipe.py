import os
import pandas as pd
import xmltodict

from db import get_connection

assemblies_csv_path = os.path.join("data", "preprocessed", "assemblies.csv")


def preprocess_data():

    with open(os.path.join("data", "raw", "MemberOfParliament.tsv"), "r") as f:
        MoP = pd.read_csv(f, sep="\t")

    xml_dicts = MoP.XmlDataFi.apply(xmltodict.parse)
    committees = []

    for henkilo in xml_dicts:

        cur_committees = henkilo["Henkilo"]["NykyisetToimielinjasenyydet"]["Toimielin"]

        if isinstance(cur_committees, dict):
            cur_committees = [cur_committees]

        try:
            prev_committees = henkilo["Henkilo"]["AiemmatToimielinjasenyydet"][
                "Toimielin"
            ]
        except TypeError:
            prev_committees = []

        if isinstance(prev_committees, dict):
            prev_committees = [prev_committees]

        for committee in cur_committees + prev_committees:
            if committee["Nimi"]:
                if committee["@OnkoValiokunta"] == "true":
                    committees.append(committee["Nimi"])

    # Due to lack of proper API for assemblies and their respective abbreviations,
    # These common committees that have records associated with them are hard coded
    # to the procedure.
    assemblies = [
        {"code": "HaV", "name": "Hallintovaliokunta"},
        {"code": "LaV", "name": "Lakivaliokunta"},
        {"code": "LiV", "name": "Liikenne- ja viestintävaliokunta"},
        {"code": "MmV", "name": "Maa- ja metsätalousvaliokunta"},
        {"code": "PeV", "name": "Perustuslakivaliokunta"},
        {"code": "PmN", "name": "Puhemiesneuvosto"},
        {"code": "EK", "name": "Eduskunnan täysistunto"},
        {"code": "PuV", "name": "Puolustusvaliokunta"},
        {"code": "SiV", "name": "Sivistysvaliokunta"},
        {"code": "StV", "name": "Sosiaali- ja terveysvaliokunta"},
        {"code": "SuV", "name": "Suuri valiokunta"},
        {"code": "TaV", "name": "Talousvaliokunta"},
        {"code": "TrV", "name": "Tarkastusvaliokunta"},
        {"code": "TuV", "name": "Tulevaisuusvaliokunta"},
        {"code": "TyV", "name": "Työelämä- ja tasa-arvovaliokunta"},
        {"code": "UaV", "name": "Ulkoasiainvaliokunta"},
        {"code": "VaV", "name": "Valtiovarainvaliokunta"},
        {"code": "YmV", "name": "Ympäristövaliokunta"},
        {"code": "SuVtJ", "name": "Suuren valiokunnan jaosto"},
        {"code": "TiV", "name": "Tiedusteluvalvontavaliokunta"},
    ]
    df_assemblies = pd.DataFrame(assemblies)
    other_committees = [
        c for c in set(committees) if c not in df_assemblies.name.values
    ]
    other_committees = pd.DataFrame(
        {"name": other_committees, "code": [None] * len(other_committees)}
    )
    df_assemblies = pd.concat([df_assemblies, other_committees])
    df_assemblies.to_csv(assemblies_csv_path, index=False)


def import_data():
    conn = get_connection()
    cursor = conn.cursor()

    with open(assemblies_csv_path) as f:
        cursor.copy_expert(
            "COPY assemblies(code, name) FROM stdin DELIMITERS ',' CSV HEADER QUOTE '\"';",
            f,
        )

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--preprocess-data", help="preprocess the data", action="store_true"
    )
    parser.add_argument(
        "--import-data", help="import preprocessed data", action="store_true"
    )
    args = parser.parse_args()
    if args.preprocess_data:
        preprocess_data()
    if args.import_data:
        import_data()
    if not args.preprocess_data and not args.import_data:
        preprocess_data()
        import_data()
