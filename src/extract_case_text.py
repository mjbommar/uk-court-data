# Imports
import os
import pandas
import tika.parser


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
    case_df = pandas.read_csv("../data/decisions.csv.gz", low_memory=False, compression="gzip")
    case_file_name = os.path.join("data", case_df.iloc[0]["url"])
    text = extract_case_judgement_text(case_file_name)
    print(text[0:200])
    print(text[-200:])
