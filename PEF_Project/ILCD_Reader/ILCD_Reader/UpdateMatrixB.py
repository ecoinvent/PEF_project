import pandas as pd
import numpy as np
import pickle
import file_utils


class UpdateMatrixB:
    def __init__(self, Matrix_FilePath, waterCorrection_TemplatePath, index_TemplatePath):
        self.Matrix_FilePath = Matrix_FilePath
        self.waterCorrection_TemplatePath = waterCorrection_TemplatePath
        self.index_TemplatePath = index_TemplatePath

    @classmethod
    def take_filePath_input(cls):
        return cls(
            input("Please Enter Path of Matrix B File: "),
            input("Please Enter Path of water correction template: "),
            input("Please Enter Path of index PEF template: "),
        )

    def __read_pickleMatrix(self):
        cscMatrix_data = file_utils.load_pkl_file(self.Matrix_FilePath)
        return cscMatrix_data

    def __read_water_correctionTempalte(self):
        """[read water amount and unit correction excel file]

        Returns:
            [panda.dataframe]: [description]
        """
        water_amount_df = pd.read_excel(self.waterCorrection_TemplatePath)
        water_amount_df.dropna(subset=["amount"], inplace=True)
        return water_amount_df

    def __read_index_Template(self):
        """[read index PEF excel file]

        Returns:
            [panda.dataframe]: [description]
        """
        index_df = pd.read_excel(self.index_TemplatePath, sheet_name="ee")
        filter = index_df["name"].str.contains("regionalized", regex=True)
        index_df.loc[filter, "name"] = (
            index_df.loc[filter, "name"].str.split(",").str[0:2].str.join(",")
        )
        return index_df

    def __merge_Dataframes(self):
        """[merge index dataframe with water amount correction dataframe]

        Returns:
            [panda.dataframe]: [description]
        """
        index_df = self.__read_index_Template()
        water_amount_df = self.__read_water_correctionTempalte()
        left_cols = ["name", "compartment", "subcompartment"]
        right_cols = ["flow", "compartment", "subcompartment"]
        merged_df = pd.merge(
            index_df,
            water_amount_df,
            left_on=left_cols,
            right_on=right_cols,
            how="inner",
        )
        return merged_df

    def __fetch_index_column(self):
        """[fetch index column from the merged dataframe]

        Returns:
            [list]: [list of indexes]
        """
        merged_df = self.__merge_Dataframes()
        list_of_indexes = merged_df["index"].tolist()
        return list_of_indexes

    def update_MatrixB(self):
        self.__multiply_matrixWithVector()

    def __multiply_matrixWithVector(self):
        """[element wise multiply csc matrix B with numpy array (vector of ones)]
        """
        list_of_indexes = self.__fetch_index_column()
        cscMatrix_data = self.__read_pickleMatrix()
        vector_of_ones = np.ones((cscMatrix_data.shape[0], 1))
        vector_of_ones[list_of_indexes, ] = 0.001
        cooMatrix_data = cscMatrix_data.multiply(vector_of_ones)
        updated_cscMatrix_data = cooMatrix_data.tocsc()
        self.__save2Pickle(updated_cscMatrix_data)

    def __save2Pickle(self, cscMatrix_data):
        with open(r"D:\ecoinvent_scripts\B.pkl", "wb") as file:
            pickle.dump(cscMatrix_data, file, protocol=pickle.HIGHEST_PROTOCOL)


def main():
    obj = UpdateMatrixB.take_filePath_input()
    obj.update_MatrixB()


if __name__ == "__main__":
    main()
