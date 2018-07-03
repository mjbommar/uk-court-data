# Imports
import multiprocessing
import os
import pandas
import requests
import sys

# Conants
BASE_HTTP_URL = "http://www.bailii.org"
CASE_OUTPUT_PATH = "data/cases/"
REQUEST_HEADERS = {
    'User-Agent': 'https://github.com/mjbommar/uk-court-data',
}


def download_case(case_url, output_path=None, skip_existing=True):
    """
    Download a case by URL.
    :param case_url:
    :param output_path:
    :param skip_existing:
    :return:
    """
    # setup output file and ensure path exists
    output_file_name = os.path.join(output_path if output_path else CASE_OUTPUT_PATH, case_url.lstrip("/"))
    if os.path.exists(output_file_name) and skip_existing:
        return True

    output_file_directory = os.path.dirname(output_file_name)
    if not os.path.exists(output_file_directory):
        os.makedirs(output_file_directory)

    # build full URL and retrieve contents to disk
    full_case_url = "{0}/{1}".format(BASE_HTTP_URL, case_url.lstrip("/"))
    r = requests.get(full_case_url, headers=REQUEST_HEADERS)
    print(output_file_name)
    with open(output_file_name, "wb") as output_file:
        output_file.write(r.content)
    return True


def download_all_cases(decision_file_path, output_path=None, skip_existing=True, workers=1):
    """
    Download all cases listed in the given decision file.
    :param decision_file_path:
    :param output_path:
    :param skip_existing:
    :param workers:
    :return:
    """
    # Read decision file
    decision_df = pandas.read_csv(decision_file_path, low_memory=False, compression="gzip")

    # Iterate through camera ID list
    if workers == 1:
        for row_id, row in decision_df.iterrows():
            download_case(row["url"], output_path, skip_existing)
    elif workers > 1:
        pool_arguments = [(row["url"], output_path, skip_existing) for row_id, row in decision_df.iterrows()]
        with multiprocessing.Pool(workers) as pool:
            results = pool.starmap(download_case, pool_arguments)
    else:
        raise ValueError("workers must be >= 1")


if __name__ == "__main__":
    download_all_cases("data/decisions.csv.gz", workers=int(sys.argv[1]))
