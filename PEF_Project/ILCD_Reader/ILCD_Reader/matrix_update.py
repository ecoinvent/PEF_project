import pandas as pd
import pickle
from util import file_utils
import files_path
import sys
# Correct amount and unit of elementary exchanges in matrix B.
# updating the values in matrix B


class MatrixUpdate:
    def __init__(self, matrix):
        self.updated_resources_df = None
        self.index_correction_df = None
        self.final_merge_df = None
        self.csc_Matrix_data = None
        self.csc_updated_matrix = None
        self.matrix = matrix

    def _ReadFiles(self):
        self._check_files_exist()
        self._read_updated_water_resource_template()
        self._read_index_PEF()
        self._load_Matrix_pickle()

    @staticmethod
    def _check_files_exist():
        try:
            with open(
                files_path.UPDATE_AMOUNTS_FOR_WATER_RESOURCES, "r"
            ) as _, open(
                files_path.INDEXES_PEF_ALLOCATION, "r"
            ) as _, open(
                files_path.INDEXES_WATERBALANCE_CORRECTION, "r"
            ) as _, open(
                files_path.DATASETS_TO_EXCLUDE_FROM_CONVERSION, "r"
            ) as _, open(
                files_path.WATER_AMOUNT_AND_UNIT_CORRECTION, "r"
            ):
                pass
        except FileNotFoundError:
            print("Please make sure you supplied the correct filepaths and filenames")
            sys.exit(1)

    def _read_updated_water_resource_template(self):
        self.updated_resources_df = pd.read_excel(
            files_path.UPDATE_AMOUNTS_FOR_WATER_RESOURCES
        )
        if self.matrix == 'A':
            self.multiply_column_by_negative_one()

    def multiply_column_by_negative_one(self):
        self.updated_resources_df['new exchange amount'] = self.updated_resources_df['new exchange amount'].apply(lambda x: x * -1)

    def _read_index_PEF(self):
        self.index_pef_df = pd.read_excel(
            files_path.INDEXES_PEF_ALLOCATION, sheet_name=None
        )

    def _read_index_waterbalance_correction_template(self):
        self.index_correction_df = pd.read_excel(
            files_path.INDEXES_WATERBALANCE_CORRECTION, sheet_name=None
        )

    def _read_datasets_to_exclude_template(self):
        excluded_dataset_df = pd.read_excel(
            files_path.DATASETS_TO_EXCLUDE_FROM_CONVERSION
        )
        return excluded_dataset_df

    def _read_water_amount_template(self, drop_column=None):
        water_amount_df = pd.read_excel(files_path.WATER_AMOUNT_AND_UNIT_CORRECTION)
        if drop_column == "amount":
            water_amount_df.dropna(subset=["amount"], inplace=True)
        elif drop_column == "exclude some datasets from conversion":
            water_amount_df.dropna(
                subset=["exclude some datasets from conversion"], inplace=True
            )
        return water_amount_df

    def _load_Matrix_pickle(self):
        if self.matrix == "A":
            self.csc_Matrix_data = file_utils.load_pkl_file(files_path.MATRIX_A_PICKLE)
        elif self.matrix == "Z":
            self.csc_Matrix_data = file_utils.load_pkl_file(files_path.MATRIX_Z_PICKLE)
            print('original ', self.csc_Matrix_data.shape)

    def _write2Pickle(self):
        with open(f"D:\\ecoinvent_scripts\\{self.matrix}.pkl", "wb") as file:
            pickle.dump(
                self.csc_Matrix_data, file, protocol=pickle.HIGHEST_PROTOCOL
            )
