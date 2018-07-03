# Imports
import os
import pandas
import tika.parser

from download_case_data import CASE_OUTPUT_PATH
CASE_TEXT_PATH = "data/text/"

def extract_case_judgement_text(case_file_path):
    """
    Extract text from case file.
    :param case_file_path:
    :return:
    """
    tika_response = tika.parser.from_file(case_file_path)
    if "content" in tika_response:
        if len(tika_response["content"]) > 0:
            content = tika_response["content"]
            p0 = content.find("HTML VERSION OF JUDGMENT")
            p1 = content.find("BAILII:\nCopyright Policy")
            return content[p0:p1].strip()



if __name__ == "__main__":
    # load decisions
    decision_df = pandas.read_csv("data/decisions.csv.gz", low_memory=False, compression="gzip")
    for row_id, row in decision_df.iterrows():
        # skip existing or missing
        case_file_name = os.path.join(CASE_OUTPUT_PATH, row["url"].lstrip("/"))
        text_file_name = os.path.join(CASE_TEXT_PATH, row["url"].lstrip("/"))

        if os.path.exists(text_file_name):
            continue
        elif not os.path.exists(case_file_name):
            continue
        else:
            try:
                os.makedirs(os.path.dirname(text_file_name))
            except FileExistsError as e:
                pass
                
        # get case text
        case_text = extract_case_judgement_text(case_file_name)
    
        # output to file
        with open(text_file_name, "w") as output_file:
            output_file.write(case_text)
            print(row["url"])

