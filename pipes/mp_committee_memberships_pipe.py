import os.path
import pandas as pd
import xmltodict
import argparse

from db import get_connection, bulk_insert

csv_path = "data/preprocessed/mp_committee_memberships.csv"


def preprocess_data():
    earliest_retirement_date = "2010-01-01"

    roles = {
        "jäsen": "member",
        "varajäsen": "associate",
        "lisäjäsen": "additional",
        "puheenjohtaja": "chair",
        "varapuheenjohtaja": "first vice",
        "ensimmäinen varapuheenjohtaja": "first vice",
        "toinen varapuheenjohtaja": "second vice",
    }

    with open(os.path.join("data", "raw", "MemberOfParliament.tsv"), "r") as f:
        MoP = pd.read_csv(f, sep="\t")

    xml_dicts = MoP.XmlDataFi.apply(xmltodict.parse)
    membership_df = pd.DataFrame(
        columns=["person_id", "committee_name", "start_date", "end_date", "role"]
    )
    for henkilo in xml_dicts:
        person_id = int(henkilo["Henkilo"]["HenkiloNro"])
        if henkilo["Henkilo"]["KansanedustajuusPaattynytPvm"]:
            retirement_date = "-".join(
                list(
                    reversed(
                        (henkilo["Henkilo"]["KansanedustajuusPaattynytPvm"]).split(".")
                    )
                )
            )
            if retirement_date < earliest_retirement_date:
                continue

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
                    committee_name = committee["Nimi"]
                    try:
                        memberships = (
                            committee["Jasenyys"]
                            if isinstance(committee["Jasenyys"], list)
                            else [committee["Jasenyys"]]
                        )
                    except KeyError:
                        continue

                    for membership in memberships:
                        if membership["AlkuPvm"]:
                            start_date = "-".join(
                                list(reversed((membership["AlkuPvm"]).split(".")))
                            )
                            if len(start_date) < 10:
                                start_date = f"{start_date[:4]}-01-01"
                            if membership["LoppuPvm"]:
                                end_date = "-".join(
                                    list(reversed((membership["LoppuPvm"]).split(".")))
                                )
                                if len(end_date) < 10:
                                    end_date = f"{end_date[:4]}-12-31"
                            role = roles[membership["Rooli"].lower()]
                            membership_df.loc[len(membership_df)] = [
                                person_id,
                                committee_name,
                                start_date,
                                end_date,
                                role,
                            ]

    membership_df.to_csv(csv_path, index=False)


def import_data():
    conn = get_connection()
    cursor = conn.cursor()

    with open(csv_path) as f:
        bulk_insert(
            cursor,
            "mp_committee_memberships",
            ["person_id", "committee_name", "start_date", "end_date", "role"],
            f,
            has_header=True,
        )

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
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
