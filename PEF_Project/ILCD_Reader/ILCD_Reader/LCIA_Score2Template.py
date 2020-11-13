import os
import pandas as pd
import numpy as np
from LCIA_score_calculation import calculate_scalings, calculate_g, calculate_h
from util.file_utils import load_pkl_file
import pickle
import files_path


class Score2Template:

    matrix_A = "A.pkl"
    matrix_B = "B.pkl"
    matrix_C = "C.pkl"
    matrix_Z = "Z.pkl"
    indexes = "indexes.pkl"

    def __init__(self, source_dir, destination_dir):
        """[initialize instance variables with source_directory and destination directory]

        Args:
            source_dir ([type]): [file path to source directory]
            destination_dir ([type]): [file path to destination directory]
        """
        self.source_dir = source_dir
        self.destination_dir = destination_dir
        self.matrix_A_data, self.matrix_B_data, self.matrix_C_data = None, None, None
        self.matrix_Z_data, self.indexes_data = None, None

    @classmethod
    def from_input(cls):
        """[Alterntive constructor take input from user to initialize instance variables with source_directory
            and destination directory]

        Returns:
            [str]: [file path to source directory]
            [str]: [file path to destination directory]
        """
        return cls(
            input(
                "Please Enter Path of Source folder that contain matrices and indexes: "
            ),
            input("Please Enter Path of Destination folder: "),
        )

    def process_pickles(self):
        """[Read pickle files that contains matrices A,B,C and Z]
        """
        self.matrix_A_data = load_pkl_file(self.source_dir, self.__class__.matrix_A)
        self.matrix_B_data = load_pkl_file(self.source_dir, self.__class__.matrix_B)
        self.matrix_C_data = load_pkl_file(self.source_dir, self.__class__.matrix_C)
        self.matrix_Z_data = load_pkl_file(self.source_dir, self.__class__.matrix_Z)
        self.indexes_data = load_pkl_file(self.source_dir, self.__class__.indexes)
        self.generate_scores(
            self.matrix_A_data,
            self.matrix_B_data,
            self.matrix_C_data,
            self.matrix_Z_data,
            self.indexes_data,
        )

    def generate_scores(
        self, matrix_A_data, matrix_B_data, matrix_C_data, matrix_Z_data, indexes_data
    ):
        """[Calculate the scaling, g and h vectors and creating directories to store the pickle files]

        Args:
            matrix_A_data ([type]): [description]
            matrix_B_data ([type]): [description]
            matrix_C_data ([type]): [description]
            matrix_Z_data ([type]): [description]
            indexes_data ([type]): [description]
        """
        dataset_to_iterate = []

        scaling_folder = os.path.join(self.destination_dir, "scaling_folder")
        lci_folder = os.path.join(self.destination_dir, "LCI_folder")
        lcia_folder = os.path.join(self.destination_dir, "LCIA_folder")

        directory_list = [scaling_folder, lci_folder, lcia_folder]
        self.check_path_exist(directory_list)
        calculate_scalings(indexes_data, matrix_A_data, matrix_Z_data, scaling_folder, dataset_to_iterate)
        calculate_g(
            scaling_folder, indexes_data, matrix_B_data, lci_folder, dataset_to_iterate
        )
        calculate_h(
            lci_folder, indexes_data, matrix_C_data, lcia_folder, dataset_to_iterate
        )

    def check_path_exist(self, directory_list):
        """[check if directories exist if not then create new ones (scaling_folder, lci_folder, lcia_folder)]

        Args:
            directory_list ([type]): [description]
        """
        for directory in directory_list:
            if not os.path.exists(directory):
                os.makedirs(directory)

    def build_dataframe(self):
        """[summary]
        """
        df1_columns = ["activityName", "geography", "product"]
        df2_columns = self.create_df_columns_names()
        dataset_list_df = self.read_dataset_list_excel()
        datalist_indexlist = dataset_list_df["index"].tolist()
        values = self.read_toggle_dictionary((datalist_indexlist))
        arr = self.read_h_vector_pickles(datalist_indexlist)
        df1 = pd.DataFrame(values, columns=df1_columns)
        df1["unitName"] = dataset_list_df["unitName"]
        df2 = pd.DataFrame(arr, columns=df2_columns)
        concatenated_df = pd.concat([df1, df2], axis=1)
        output_file = os.path.join(files_path.EXCEL_DESTINATION_DIRECTORY, "LCIAcalc.xlsx")
        concatenated_df.to_excel(output_file, index=False)

    def create_df_columns_names(self):
        """[creating column names by reading each row in LCIA dataframe and apply formatting on every row and then
            append it to column list]

        Returns:
            [list]: [list of formatted column names]
        """
        columns = []
        # dfs is a dictionary in indexes_data, the keys are(ie,ee,LCIA) and values are panda data frames
        for i, _ in enumerate(self.indexes_data.dfs["LCIA"].index):
            # Reading every row in LCIA df
            dataframe_row = self.indexes_data.dfs["LCIA"].iloc[i, [0, 1, 2, 3]]
            dataframe_row[3] = f"({dataframe_row[3]})"
            dataframe_row = "-".join(dataframe_row)
            columns.append(dataframe_row)
        return columns

    def read_dataset_list_excel(self):
        """[Reading dataset excel file]

        Returns:
            [pd.Dataframe]: [datafrme of dataset list excel file]
        """
        datalist_df = pd.read_excel(files_path.DATASET_LIST)
        return datalist_df

    def read_toggle_dictionary(self, datalist_indexes):
        """[summary]

        Args:
            datalist_indexes ([type]): [description]

        Returns:
            [type]: [description]
        """
        values = [self.indexes_data.toggle["ie"][x] for x in datalist_indexes]
        return values

    def read_h_vector_pickles(self, datalist_indexlist):
        """[Read all the pickle files that are generated in LCIA Folder]

        Args:
            datalist_indexlist ([type]): [description]

        Returns:
            [type]: [description]
        """
        arr = np.empty((0, 28), float)
        for i in datalist_indexlist:
            try:
                with open(
                    os.path.join(self.destination_dir, "LCIA_folder", f"{i}.pkl"), "rb"
                ) as file:
                    data = pickle.load(file)
                    arr = np.append(arr, data.T, axis=0)
            except IOError as error:
                print(error)
        return arr


def main():
    input = Score2Template.from_input()
    input.process_pickles()
    input.build_dataframe()


if __name__ == "__main__":
    main()
