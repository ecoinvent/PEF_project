
import pandas as pd
from util import file_utils
import os

file_path = r"D:\ecoinvent_scripts\output\merged"


def create_list_of_files():
    ext = "xlsx"
    files = file_utils.create_file_list(file_path, ext)
    return files


def generate_dataframe():
    files = create_list_of_files()
    temp_df = None
    df = None
    for file in files:
        print(f"processing {file}...")
        df = pd.read_excel(file)
        df = pd.concat([temp_df, df])
        temp_df = df
    df.drop_duplicates(subset=["exchange name", "compartment", "subcompartment"], keep='first', inplace=True)
    write_to_excel(df)


def write_to_excel(df):
    print("writing to excel....")
    file = os.path.join(file_path, "removed_dup.xlsx")
    df.to_excel(file, index=False)


generate_dataframe()
# class AddMissingProcessesToIndex:
    
#     def __init__(self):
#         pass

#     def __read_index_PEF(self, sheet):
#         print("read index PEF file...")
#         index_pef_df = pd.read_excel(files_path.INDEXES_PEF_ALLOCATION, sheet_name=sheet)
#         return index_pef_df 

#     def fetch_list_of_files(self, src_dir):
#         ext = "csv"
#         self.files = create_file_list(src_dir, ext)

#     def __read_processes(self):
#         pass