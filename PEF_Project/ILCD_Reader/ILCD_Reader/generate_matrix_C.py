import pandas as pd
from scipy import sparse
import pickle
import sys


class GenerateMatrixC:
    def __init__(self, matrixC_excel, indexes_excel):
        """initialize instance variables with matrixC and indexes excel file locations

        Args:
            matrixC_excel (str): [file path to matrixC excel file]
            indexes_excel (str): [file path to index excel file]
        """
        self.matrixC_excel = matrixC_excel
        self.indexes_excel = indexes_excel
        self.matrix_C_df, self.indexes_ee_df, self.indexes_LCIA_df = None, None, None
        self.merged_C_ee_df, self.final_merged_df = None, None
        self.rows, self.cols = None, None

    @classmethod
    def user_input(cls):
        """Alterntive constructor initialize instance variables with matrixC and indexes excel locations

        Returns:
            [str]: [file path to matrixC excel file]
            [str]: [file path to index excel file]
        """
        return cls(
            input("Please Enter Path of Matrix-C Excel's file: "),
            input("Please Enter Path of indexes Excel's file: "),
        )

    def gen_matrixC(self):
        """Generate Matrix C (The interface function to user)
        """
        self.__read_Excels()
        self.get_df_index_length()
        self.__merge_dataframes()
        self.__create_matrix()
        self.__dump_to_pickle()

    def __read_Excels(self):
        """Read excel files and converted to panda dataframes
        """
        if all([self.matrixC_excel.endswith(".xlsx"), self.indexes_excel.endswith(".xlsx")]):
            self.matrix_C_df = pd.read_excel(self.matrixC_excel)
            self.indexes_ee_df = pd.read_excel(self.indexes_excel, "ee")
            self.indexes_LCIA_df = pd.read_excel(self.indexes_excel, "LCIA")
        else:
            print("Files selected are not an Excel Files")
            sys.exit(1)

    def __merge_dataframes(self):
        """Merge matrix_C_df with indexes_ee_df and the result of the merge would be merged with indexes_LCIA_df

        Args:
            matrix_C_df (pd.DataFrame): [panda dataframe of the matrixC excel file]
            indexes_ee_df (pd.DataFrame): [panda dataframe of the index excel file sheet name (ee)]
            indexes_LCIA_df (pd.DataFrame): [panda dataframe of the index excel file sheet name (LCIA)]
        """
        # self.matrix_C_df['category'] = self.matrix_C_df['category'].astype(str)
        print('matrix c', self.matrix_C_df.info())
        print('matrix EE', self.indexes_ee_df.info())
        print('matrix LCIA', self.indexes_LCIA_df.info())
        try:
            self.merged_C_ee_df = pd.merge(
                self.matrix_C_df,
                self.indexes_ee_df,
                left_on=["exchange name", "compartment", "subcompartment"],
                right_on=["name", "compartment", "subcompartment"],
                how="left",
            )
        except ValueError as err:
            print(err)
            sys.exit(1)
        try:
            self.final_merged_df = pd.merge(
                self.merged_C_ee_df,
                self.indexes_LCIA_df,
                left_on=["method", "category", "indicator"],
                right_on=["method", "category", "indicator"],
                how="left",
            )
        except ValueError as err:
            print(err)
            sys.exit(1)

    def get_df_index_length(self):
        """Getting the length of data frames index columns

        Args:
            indexes_LCIA_df (pd.DataFrame): [panda dataframe of the index excel file sheet name (LCIA)]
            indexes_ee_df (pd.DataFrame): [panda dataframe of the index excel file sheet name (ee)]
        """
        self.rows = len(self.indexes_LCIA_df.index)
        self.cols = len(self.indexes_ee_df.index)

    def __create_matrix(self):
        """creating sparse coo matrix and then converted to sparse csc matrix
        """
        row_index = self.final_merged_df["index_y"]
        col_index = self.final_merged_df["index_x"]
        data = self.final_merged_df["CF"]
        self.csc_matrix = sparse.coo_matrix(
            (data, (row_index, col_index)), shape=(self.rows, self.cols)
        ).tocsc()

    def __dump_to_pickle(self):
        """store the csc sparse matrix to pickle file
        """
        with open("D:\\ecoinvent_scripts\\C.pkl", "wb") as outfile:
            pickle.dump(self.csc_matrix, outfile, pickle.HIGHEST_PROTOCOL)


def main():
    gen_mat = GenerateMatrixC.user_input()
    gen_mat.gen_matrixC()


if __name__ == "__main__":
    main()
