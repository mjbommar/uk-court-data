# Imports
import multiprocessing
import os
import pandas
import sys
import tika.parser

from download_case_data import CASE_OUTPUT_PATH
CASE_TEXT_PATH = "data/text/"

def extract_case_judgement_text(case_file_path):
    """
    Extract text from case file.
    :param case_file_path:
    :return:
    """
    # Download path
    case_file_name = case_file_path
    text_file_name = case_file_path.replace("data/cases/", "data/text/")

    if os.path.exists(text_file_name):
        return True
    elif not os.path.exists(case_file_name):
        return False
    else:
        try:
            os.makedirs(os.path.dirname(text_file_name))
        except FileExistsError as e:
            pass
                
    # get case text
    case_text = None
    case_file_buffer = open(case_file_path, "rb").read()
    tika_response = tika.parser.from_buffer(case_file_buffer)
    if "content" in tika_response:
        if len(tika_response["content"]) > 0:
            content = tika_response["content"]
            p0 = content.find("HTML VERSION OF JUDGMENT")
            p1 = content.find("BAILII:\nCopyright Policy")
            case_text = content[p0:p1].strip()
        else:
            return False
    else:
        return False

    # output to file
    with open(text_file_name, "w") as output_file:
        output_file.write(case_text)    


def extract_all_cases(decision_file_path, output_path=None, skip_existing=True, workers=1):
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
            case_file_name = os.path.join(CASE_OUTPUT_PATH, row["url"].lstrip("/"))
            print(case_file_name)
            extract_case_judgement_text(case_file_name)
    elif workers > 1:
        pool_arguments = [(os.path.join(CASE_OUTPUT_PATH, row["url"].lstrip("/")), ) for row_id, row in decision_df.iterrows()]
        with multiprocessing.Pool(workers) as pool:
            results = pool.starmap(extract_case_judgement_text, pool_arguments)
    else:
        raise ValueError("workers must be >= 1")


if __name__ == "__main__":
    extract_all_cases("data/decisions.csv.gz", workers=int(sys.argv[1]))
    

