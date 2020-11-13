from util.file_utils import load_pkl_file
import scipy
import pandas as pd
from tqdm import tqdm
import pickle
import sys
import files_path


# Generate Matrix Exhanges


class MatrixExchanges:
    def convert_matrix_to_df(self, folder_path, matrix_name):
        pickled_matrix = load_pkl_file(folder_path, matrix_name)
        if type(pickled_matrix) == scipy.sparse.csc.csc_matrix:
            coo_matrix = pickled_matrix.tocoo()
            # intialise dictionary of lists with matrix data
            data = {
                "row_index": coo_matrix.row,
                "column_index": coo_matrix.col,
                "data": coo_matrix.data,
            }
            # df = pd.DataFrame(data)
            # df.to_excel('matrixb.xlsx')
            return pd.DataFrame(data)
        else:
            print("its not a matrix")

    @staticmethod
    def map_matrix_to_exhanges(matrix_name):
        if isinstance(matrix_name, str):
            indexes_meaning = {"A": "ie", "B": ("ee", "ie"), "C": ("LCIA", "ee"), "Z": "ie"}
            return indexes_meaning[matrix_name]

    @staticmethod
    def read_dataset_list_water():
        """[summary]
        """
        dataset_df = pd.read_excel(files_path.DATASET_LIST_FLOWCHANGES)
        return dataset_df

    def read_index_Template(self):
        df_index = pd.read_excel(files_path.INDEXES_PEF_ALLOCATION, sheet_name=None)
        return df_index

    def fetch_dataframes(self, index_data, matrix_name):
        # if isinstance(index_data, dict) and isinstance(matrix_name, str):
        if matrix_name == "A" or matrix_name == "Z":
            exchange = self.map_matrix_to_exhanges(matrix_name)
            data_frame = index_data[exchange]
            return (data_frame)
        else:
            first_exchange, second_exchange = self.map_matrix_to_exhanges(matrix_name)
            first_df = index_data[first_exchange]
            second_df = index_data[second_exchange]
            return (first_df, second_df)

    def rename_index_df_columns(self, index_df, col, exchange):
        # renaming columns of dataframes that are exist in index dfs dictionary
        columns_names = {}
        if exchange == "ie":
            if col == "row_index":
                columns_names = {
                    "activityName": "activityLink_activityName",
                    "geography": "activityLink_geography",
                    "product": "exchange name",
                    "unitName": "exchange unitName",
                }
            elif col == "column_index":
                columns_names = {
                    "product": "reference product",
                    "unitName": "reference product unitName",
                }
        if exchange == "ee":
            columns_names = {"name": "exchange name", "unitName": "exchange unitName"}
        if exchange == "LCIA":
            columns_names = {"unitName": "indicator unitName"}

        index_df.rename(columns=columns_names, inplace=True)
        return index_df

    def merge_index_df_with_matrices(self, df, matrix_name, index_data):
        ie_df_cols = df.columns[:2]
        index_df = self.fetch_dataframes(index_data, matrix_name)
        if matrix_name == "A" or matrix_name == "Z":
            exchange = "ie"
            for col in ie_df_cols:
                new_index_df = index_df.copy()
                renamed_index_df = self.rename_index_df_columns(new_index_df, col, exchange)
                df = pd.merge(
                    df, renamed_index_df, how="left", left_on=col, right_on="index"
                )
                df.drop("index", axis=1, inplace=True)
            return df
        else:
            exchanges = self.map_matrix_to_exhanges(matrix_name)
            for idf, col, exchange in zip(index_df, ie_df_cols, exchanges):
                renamed_index_df = self.rename_index_df_columns(idf, col, exchange)
                df = pd.merge(
                    df, renamed_index_df, how="left", left_on=col, right_on="index"
                )
                df.drop("index", axis=1, inplace=True)
            return df

    def concat_dataframes(self, folder_path, matrices_list, index_data):
        if isinstance(matrices_list, list):
            matrices_iter = iter(matrices_list)
            first_matrix = next(matrices_iter)
            first_matrix_df = self.convert_matrix_to_df(folder_path, first_matrix)
            merged_index_matriceZ = self.merge_index_df_with_matrices(
                first_matrix_df, first_matrix, index_data
            )
            second_matrix = next(matrices_iter)
            second_matrix_df = self.convert_matrix_to_df(folder_path, second_matrix)
            merged_index_matriceB = self.merge_index_df_with_matrices(
                second_matrix_df, second_matrix, index_data
            )
            concatented_df = pd.concat(
                [merged_index_matriceZ, merged_index_matriceB], sort=False
            )
            return concatented_df

    def input_matrix_names(self, folder_path, file_name):
        index_file_data = self.read_index_Template()
        matrices = input("Enter a list of matrices names seperated by comma: ").upper()
        try:
            if len(matrices) == 3:
                matrices_list = matrices.split(",")
                df = self.concat_dataframes(folder_path, matrices_list, index_file_data)
                return df
            elif len(matrices) == 1 and matrices in "ABCZ":
                data_frame = self.convert_matrix_to_df(folder_path, matrices)
                print("converted matrix", data_frame.shape)
                df = self.merge_index_df_with_matrices(data_frame, matrices, index_file_data)
                return df
            else:
                raise ValueError
        except ValueError:
            print("Matrices names are wrong. Please Enter either A,B,C,Z")
            sys.exit(1)

    def df_to_customized_excel(self, folder_path, file_name):
        df = self.input_matrix_names(folder_path, file_name)
        dataset_df = self.read_dataset_list_water()
        merged_df3 = pd.merge(
            df,
            dataset_df,
            on=["activityName", "geography", "reference product"],
            how="left",
        )

        merged_df3 = merged_df3[
            [
                "row_index",
                "column_index",
                "activityName",
                "geography",
                "reference product",
                "reference product unitName",
                "exchange name",
                "compartment",
                "subcompartment",
                "activityLink_activityName",
                "activityLink_geography",
                "data",
                "exchange unitName",
                "Dataset_type",
            ]
        ]

        # generate different sheets based on that datset
        dataset_type_list = merged_df3["Dataset_type"].dropna().unique().tolist()
        dataframes_dict = {
            dataset: merged_df3.loc[merged_df3["Dataset_type"] == dataset, :].copy()
            for dataset in dataset_type_list
        }
        Ts_other_df = dataframes_dict.pop("TS other")
        Ts_el_df = dataframes_dict.pop("TS el")
        Ts_other_df1 = Ts_other_df.iloc[:500000, :]
        Ts_other_df2 = Ts_other_df.iloc[500000:, :]
        Ts_el_df1 = Ts_el_df.iloc[:500000, :]
        Ts_el_df2 = Ts_el_df.iloc[500000:, :]

        Ts_other_dicitonary = {"Ts_other_firstHalf": Ts_other_df1, "Ts_other_secondHalf": Ts_other_df2}
        Ts_el_dicitonary = {"Ts_el_firstHalf": Ts_el_df1, "Ts_el_secondHalf": Ts_el_df2}
        dataframes_dict = {**dataframes_dict, **Ts_other_dicitonary, **Ts_el_dicitonary}
        df_shapes_list = []
        writer = pd.ExcelWriter(
            r"D:\ecoinvent_scripts\Matrix Exchanges.xlsx", engine="xlsxwriter"
        )
        print("Starting the writing process...")
        for key in tqdm(dataframes_dict):
            df_shapes_list.append(dataframes_dict[key].shape)
            dataframes_dict[key].drop("Dataset_type", axis=1, inplace=True)
            dataframes_dict[key].to_excel(writer, sheet_name=key, index=False)

        for sheet, shape in zip(writer.sheets, df_shapes_list):
            worksheet = writer.sheets[sheet]
            worksheet.autofilter(0, 0, shape[0] - 1, shape[1] - 2)
            worksheet.freeze_panes(1, 0)

        print()
        print("Saving Excel File in process...")
        writer.save()
        # """Applying filtering on dataframe to divide into different sheets"""

    def dataframe_to_excel(self, folder_path, file_name):
        df = self.input_matrix_names(folder_path, file_name)
        # df.sort_values(['indicator', 'exchange name', 'compartment', 'subcompartment'], inplace=True)

        with open("MergedMatrix.pkl", "wb") as outfile:
            pickle.dump(df, outfile, pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    folder_path = files_path.PICKLES_SOURCE_DIRECTORY
    file_name = "indexes.xlsx"
    user_input = input(
        "Choose option (1) Generating matrix exchanges excel or (2) Generating merged matrix with indexes "
    )
    print(user_input)
    obj = MatrixExchanges()
    if int(user_input) == 1:
        obj.df_to_customized_excel(folder_path, file_name)
    else:
        obj.dataframe_to_excel(folder_path, file_name)
