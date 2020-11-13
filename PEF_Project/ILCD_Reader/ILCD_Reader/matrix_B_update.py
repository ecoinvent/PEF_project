import pandas as pd
import pickle
import file_utils
import numpy as np
import files_path
# Correct amount and unit of elementary exchanges in matrix B.
# updating the values in matrix B


class MatrixBUpdate:

    def __init__(self):
        self.updated_resources_df = None
        self.index_correction_df = None
        self.final_merge_df = None
        self.csc_MatrixB_data = None
        self.csc_updated_matrix = None

    def ReplaceElements(self):
        self.__read_updated_water_resource_template()
        self.__read_index_PEF()
        self.__load_MatrixB_pickle()
        self.__merge_updated_resources_with_index()
        self.__Replace_Matrix_Elements()
        self.__pointWise_divide_by_1000()
        self.__pointWise_multiply_by_1000()
        self.__write2Pickle()

    def __read_updated_water_resource_template(self):
        self.updated_resources_df = pd.read_excel(r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\excel\updated amounts for correction_MatrixB.xlsx")

    def __read_index_PEF(self):
        self.index_pef_df = pd.read_excel(files_path.INDEXES_PEF_ALLOCATION, sheet_name=None)

    def __read_index_waterbalance_correction_template(self):
        self.index_correction_df = pd.read_excel(r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\06_matrixFlowChanges\06A_water\in\indexes_waterBalance_correction_20200326.xlsx", sheet_name=None)

    def __read_datasets_to_exclude_template(self):
        excluded_dataset_df = pd.read_excel(r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\06_matrixFlowChanges\06A_water\in\datasets to exclude from amount conversion for water, Resources.xlsx")
        return excluded_dataset_df

    def __read_water_amount_template(self, drop_column=None):
        water_amount_df = pd.read_excel(r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\06_matrixFlowChanges\06A_water\in\water amount and unit correction_20200326.xlsx")
        if drop_column == "amount":
            water_amount_df.dropna(subset=['amount'], inplace=True)
        elif drop_column == "exclude some datasets from conversion":
            water_amount_df.dropna(subset=['exclude some datasets from conversion'], inplace=True)
        return water_amount_df

    def __load_MatrixB_pickle(self):
        file_path = r"C:\Dropbox (ecoinvent)\ei-int\technical\external\PEF\PEF follow-up\execution\06_matrixFlowChanges\06A_water\in\pilotPhase_water_correction\B.pkl"
        self.csc_MatrixB_data = file_utils.load_pkl_file(file_path)

    def __merge_updated_resources_with_index(self):
        column_idx_df = pd.merge(self.updated_resources_df, self.index_pef_df['ie'], how='inner', left_on=['activityName', 'geography', 'reference product'], right_on=['activityName', 'geography', 'product'])
        row_idx_df = pd.merge(self.updated_resources_df, self.index_pef_df['ee'], how='inner', left_on=['exchange name', 'compartment', 'subcompartment'], right_on=['name', 'compartment', 'subcompartment'])
        self.merged_resources_and_index_df = pd.merge(row_idx_df, column_idx_df, how='inner', left_on=['activityName', 'geography', 'reference product', 'exchange name', 'compartment', 'subcompartment'], right_on=['activityName', 'geography', 'reference product', 'exchange name', 'compartment', 'subcompartment'])

    def __Replace_Matrix_Elements(self):
        rowIdx_array = self.merged_resources_and_index_df['index_x'].to_numpy()
        colIdx_array = self.merged_resources_and_index_df['index_y'].to_numpy()
        newvaluess_array = self.merged_resources_and_index_df['new exchange amount_x'].to_numpy()
        count = 0
        for row, col, newVal in zip(rowIdx_array, colIdx_array, newvaluess_array):
            self.csc_MatrixB_data[row, col] = newVal
            count = count + 1
        print("Values Updated in Matrix", count)

    def __merge_excluded_dataset_with_index(self):
        excluded_dataset_df = self.__read_datasets_to_exclude_template()
        merged_idx_excluded_df = pd.merge(excluded_dataset_df, self.index_pef_df['ie'], how='inner', left_on=['activityName', 'geography', 'reference product'], right_on=['activityName', 'geography', 'product'])
        # merged_idx_excluded_df.to_excel(r"D:\ecoinvent_scripts\merged_idx_excluded_df.xlsx")
        return merged_idx_excluded_df['index'].to_numpy()

    def __pointWise_divide_by_1000(self):
        # merge the water amount with index in order to get the index column
        numpyArray_index_row = self.__merge_water_amount_with_index('amount')
        # vector_toMulitpy = np.full((1, self.csc_MatrixB_data.shape[1]), 1).astype(float)
        # vector_toMulitpy[0, numpyArray_index_col] = 1
        vector_to_multiply = np.ones((self.csc_MatrixB_data.shape[0], 1))
        vector_to_multiply[numpyArray_index_row, ] = 0.001
        coo_matrix = self.csc_MatrixB_data.multiply(vector_to_multiply)
        self.csc_updated_matrixB = coo_matrix.tocsc()

    def __merge_water_amount_with_index(self, drop_column=None):
        self.__modify_name_column_in_index()
        water_amount_df = self.__read_water_amount_template(drop_column)
        print(water_amount_df.info())
        water_idx_merged = pd.merge(water_amount_df, self.index_pef_df['ee'], how='inner', left_on=['flow', 'compartment', 'subcompartment'], right_on=['name', 'compartment', 'subcompartment'])
        # water_idx_merged.to_excel(r"D:\ecoinvent_scripts\water_idx_merged.xlsx")
        return water_idx_merged['index'].to_numpy()

    def __modify_name_column_in_index(self):
        filter = self.index_pef_df["ee"]["name"].str.contains("regionalized", regex=True)
        self.index_pef_df["ee"].loc[filter, "name"] = (
            self.index_pef_df["ee"].loc[filter, "name"].str.split(",").str[0:2].str.join(",")
        )

    def __pointWise_multiply_by_1000(self):
        numpyArray_index_row = self.__merge_water_amount_with_index('exclude some datasets from conversion')
        matrix_to_multiply = np.ones((self.csc_updated_matrixB.shape[0], self.csc_updated_matrixB.shape[1]))
        numpyArray_index_col = self.__merge_excluded_dataset_with_index()

        for i in numpyArray_index_row:
            for j in numpyArray_index_col:
                matrix_to_multiply[i][j] = 1000
        coo_matrixB_data = self.csc_updated_matrixB.multiply(matrix_to_multiply)
        self.csc_updated_matrixB = coo_matrixB_data.tocsc()

    def __write2Pickle(self):
        with open(r"D:\ecoinvent_scripts\B.pkl", "wb") as file:
            pickle.dump(self.csc_updated_matrixB, file, protocol=pickle.HIGHEST_PROTOCOL)


obj = MatrixBUpdate()
obj.ReplaceElements()
