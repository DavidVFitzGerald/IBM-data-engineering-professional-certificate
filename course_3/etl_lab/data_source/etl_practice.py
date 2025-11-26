import glob
import zipfile
import datetime

import requests
import xml.etree.ElementTree as ET
import pandas as pd


log_fp = "log_file.txt"


def download_data(url, target_file):
    response = requests.get(url)
    response.raise_for_status() # Raise an exception for bad status codes

    with open(target_file, "wb") as f:
        f.write(response.content)


def unzip(zip_filepath):
    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        zip_ref.extractall()


def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df


def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process, lines=True)
    return df


def extract_from_xml(file_to_process):
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    data = []

    for element in root:
        data.append(
            {"car_model": element.find("car_model").text,
             "year_of_manufacture": float(element.find("year_of_manufacture").text),
             "price": float(element.find("price").text),
             "fuel": element.find("fuel").text}
        )

    return pd.DataFrame(data)


def extract():
    file_list = glob.glob("./used_car_prices*")
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
    df["price"] = round(df["price"], 2)
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
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip"
    output_fp = "transformed_data.csv"
    filename = url.split("/")[-1]

    log_progress("ETL Job started")
    log_progress("Download phase started") 
    download_data(url, filename)
    log_progress("Download phase ended") 
    log_progress("Unzip phase started")
    unzip(filename)
    log_progress("Unzip phase ended") 
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
