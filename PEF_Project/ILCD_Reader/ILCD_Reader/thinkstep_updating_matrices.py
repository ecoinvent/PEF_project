import pandas as pd
import pickle
from util import file_utils
import files_path
import sys


class ThinkstepUpdatingMatrices:
    
    def __init__(self):
        pass

    def __read_indices(self, sheet):
        print("read index PEF file...")
        index_pef_df = pd.read_excel(files_path.INDEXES_PEF_ALLOCATION, sheet_name=sheet)
        df.dropna(subset=["amount"], inplace=True)
        return index_pef_df

    def __concat_the_dataframe(self):
        pass

    def __load_matrix_A(self):
        pass

    def __load_matrix_B(self):
        pass

    def __update_matrix_A(self):
        pass

    def __update_matrix_B(self):
        pass


