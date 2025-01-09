import os
import requests
import zipfile
import pandas as pd


def ensure_directory_exists(directory):
    """Ensure the directory exists. Create it if not."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Directory '{directory}' created.")
        return False
    else:
        print(f"Directory '{directory}' already exists.")
        return True


def download_file_if_not_exists(file_path, url):
    """Download a file if it does not exist."""
    if not os.path.exists(file_path):
        response = requests.get(
            url,
            stream=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
            },
        )
        response.raise_for_status()
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        print(f"Downloaded '{file_path}'.")
    else:
        print(f"File '{file_path}' already exists.")


def unzip(zip_path, extract_to):
    """Unzip a file and delete the .zip file afterward."""
    if zipfile.is_zipfile(zip_path):
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"Extracted '{zip_path}' to '{extract_to}'.")
    else:
        print(f"'{zip_path}' is not a valid zip file.")


def download(
    dir_zip="zip",
    dir_out="datasets",
    name="municipios",
    download_url="https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/UFs/BA/BA_Municipios_2022.zip",
):
    """
    Downloads and extracts a dataset.

    Parameters:
        dir_zip (str): Directory for the downloaded zip file.
        dir_out (str): Directory for the extracted dataset.
        name (str): Name of the dataset folder.
        download_url (str): URL to download the dataset zip file.
    """
    dir_final = os.path.join(dir_out, name)
    zip_file_path = os.path.join(dir_zip, os.path.basename(download_url))

    # Ensure necessary directories exist
    for directory in [dir_zip, dir_out, dir_final]:
        ensure_directory_exists(directory)

    try:  # zip
        # Download the file if it doesn't exist
        download_file_if_not_exists(zip_file_path, download_url)

        # Unzip the file to the final directory
        unzip(zip_file_path, dir_final)
    except:  # csv
        data = pd.read_csv(download_url)

        data.to_csv(os.path.join(dir_final, "{}.csv".format(name)), index=False)
