import glob
import datetime

import xml.etree.ElementTree as ET
import pandas as pd


log_fp = "log_file.txt"


def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df


def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process, lines=True)
    return df


def extract_from_xml(file_to_process):
    tree = ET.parse(file_to_process)
    root = tree.getroot()

    tag_name = "person"
    persons = []

    for person in root.findall(f'.//{tag_name}'):
        persons.append(
            {"name": person.find("name").text,
             "height": float(person.find("height").text),
             "weight": float(person.find("weight").text)}
        )

    return pd.DataFrame(persons)


def extract():
    file_list = glob.glob("./source*")
    data = []

    for f in file_list:
        if f.endswith(".csv"):
            df = extract_from_csv(f)
            data.append(df)

        elif f.endswith(".json"):
            df = extract_from_json(f)
            data.append(df)

        elif f.endswith(".xml"):
            df = extract_from_xml(f)
            data.append(df)

    df = pd.concat(data, ignore_index=True)
    return df


def transform(df):
    df["height"] = round(df["height"] * 0.0254, 2)
    df["weight"] = round(df["weight"] * 0.453592, 2)
    return df


def load_data(df, target_file):
    df.to_csv(target_file)


def log_progress(message):
    dt = datetime.datetime.now()
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    timestamp = dt.strftime(timestamp_format)
    with open(log_fp, "a") as f:
        f.write(f"{timestamp}: {message}\n")


if __name__ == "__main__":
    output_fp = "transformed_data.csv"
    log_progress("ETL Job started")
    log_progress("Extract phase started") 
    df = extract()
    log_progress("Extract phase ended") 
    log_progress("Transform phase started") 
    df = transform(df)
    log_progress("Transform phase ended") 
    log_progress("Load phase started")
    load_data(df, output_fp)
    log_progress("Load phase ended")
    log_progress("ETL Job ended") 