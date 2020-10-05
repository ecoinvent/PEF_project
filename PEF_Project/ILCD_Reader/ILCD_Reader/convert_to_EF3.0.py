import pandas as pd
import files_path
from util import file_utils
import numpy as np
from scipy import sparse


class ConvertToEF3:
    def __init__(self):
        self.flow_changes_df = None
        self.index_PEF_df = None
        self.merged_matrix = None
        self.matrix_B_array = None
        self.grouped_rows_df = None

    def start_converting(self):
        self._read_flowchanges_excel()
        self._read_index_PEF_excel()
        self._merge_flowchanges_and_index()
        self._read_matrix_B()
        self._build_df_of_matrixrows_to_addup()
        self._addup_matrix_rows()
        self._write_index_PEF_to_excel()

    def _read_flowchanges_excel(self):
        """[summary]
        """
        self.flow_changes_df = pd.read_excel(files_path.FLOW_CHANGES_SPEC)
        self._rename_flowchanges_columns()

    def _rename_flowchanges_columns(self):
        """[summary]
        """
        self.flow_changes_df.columns = self.flow_changes_df.iloc[0]
        self.flow_changes_df.drop(0, axis=0, inplace=True)
        self.flow_changes_df = self.flow_changes_df.loc[
            :, self.flow_changes_df.columns.notnull()
        ]
        self.flow_changes_df.columns = [
            "exchange name",
            "compartment",
            "subcompartment",
            "exchange unitName",
            "new_exchange name",
            "new_compartment",
            "new_subcompartment",
            "new_exchange unitName",
        ]

    def _read_index_PEF_excel(self):
        """[summary]
        """
        self.index_PEF_df = pd.read_excel(
            files_path.INDEXES_PEF_ALLOCATION, sheet_name=None
        )

    def _read_matrix_B(self):
        """[summary]
        """
        matrix_B = file_utils.load_pkl_file(files_path.PICKLES_SOURCE_DIRECTORY, "B")
        self.matrix_B_array = matrix_B.todense()
        print("shape", self.matrix_B_array.shape)

    def _merge_flowchanges_and_index(self):
        """[summary]
        """
        self.merged_matrix = pd.merge(
            self.flow_changes_df,
            self.index_PEF_df["ee"],
            how="left",
            left_on=["exchange name", "compartment", "subcompartment"],
            right_on=["name", "compartment", "subcompartment"],
        )

    def _build_df_of_matrixrows_to_addup(self):
        """[summary]
        """
        self.grouped_rows_df = (
            self.merged_matrix.groupby(
                ["new_exchange name", "new_compartment", "new_subcompartment", "new_exchange unitName"]
            )["index"]
            .apply(list)
            .reset_index(name="rows_indexes")
        )
        self.grouped_rows_df.sort_values(by=['new_exchange name'], ignore_index=True, inplace=True)
        self.grouped_rows_df.to_excel('grouped2.xlsx')

    def _addup_matrix_rows(self):
        """[summary]
        """
        list_to_delete = []
        extend = list_to_delete.extend
        self.new_array = np.zeros(shape=(len(self.grouped_rows_df), self.matrix_B_array.shape[1]))
        for i, indexes_list in enumerate(self.grouped_rows_df["rows_indexes"]):
            numpy_array = np.zeros(self.matrix_B_array.shape[1], dtype=np.int)
            if len(indexes_list) == 1:
                self.new_array[i] = self.matrix_B_array[next(iter(indexes_list))]
            elif len(indexes_list) > 1:
                extend(indexes_list[1:])
                for idx in indexes_list:
                    numpy_array = numpy_array + self.matrix_B_array[idx]
                self.new_array[i] = numpy_array
            else:
                continue
        self._save_csc_matrix_to_pickle()

    def _save_csc_matrix_to_pickle(self):
        """[summary]
        """
        print("new shape", self.new_array.shape)
        csc_matrix = sparse.csc_matrix(self.new_array)
        file_utils.dump_to_pickle("B", csc_matrix)

    def _write_index_PEF_to_excel(self):
        """[summary]
        """
        self.grouped_rows_df['rows_indexes'] = self.grouped_rows_df.index
        self.grouped_rows_df.rename(columns={
                                           'new_exchange name': 'name',
                                           'new_compartment': 'compartment',
                                           'new_subcompartment': 'subcompartment',
                                           "new_exchange unitName": "unitName",
                                           'rows_indexes': 'index'
                                           }, inplace=True
                                    )
        sheets = ["ie", "ee", "LCIA"]
        dataframes_dict = {
                          "ie": self.index_PEF_df["ie"],
                          "ee": self.grouped_rows_df,
                          "LCIA": self.index_PEF_df["LCIA"]
        }

        with pd.ExcelWriter("Index2.xlsx") as writer:
            for sheet in sheets:
                try:
                    dataframes_dict[sheet].to_excel(
                        writer, sheet_name=sheet, index=False
                    )
                except ValueError as error:
                    print(error)


obj = ConvertToEF3()
obj.start_converting()



  # if next(iter(indexes_list)) == 31252 or next(iter(indexes_list)) == 31253:
                #     print("**=============================================")
                #     print(i)
                #     print(next(iter(indexes_list)))
                #     print(np.nonzero(self.matrix_B_array[next(iter(indexes_list))]))
                #     print(np.nonzero(self.new_array[i]))

   # def _update_index_PEF_contents(self):
    #     """[summary]
    #     """
        # index_rows = self.merged_matrix["index"]
        # count = 0
        # for idx in index_rows:
        #     row = self.merged_matrix.loc[
        #         self.merged_matrix["index"] == idx,
        #         [
        #             "new_exchange name",
        #             "new_compartment",
        #             "new_subcompartment",
        #             "new_exchange unitName",
        #         ],
        #     ]
        #     exchange_name = row["new_exchange name"].item()
        #     compartment = row["new_compartment"].item()
        #     subcompartment = row["new_subcompartment"].item()
        #     unitName = row["new_exchange unitName"].item()
        # self.index_PEF_df["updated_ee"].loc[self.grouped_rows_df.index, self.grouped_rows_df.columns] = self.grouped_rows_df
        # self.index_PEF_df["updated_ee"]['rows_indexes'] = self.grouped_rows_df.index
        # self.index_PEF_df["ee"].loc[
        #     self.index_PEF_df["ee"]["index"] == idx,
        #     ["name", "compartment", "subcompartment", "unitName"],
        # ] = (exchange_name, compartment, subcompartment, unitName)
        #     count = count + 1
        # self._drop_duplicated_rows()
        # print("count", count)

    # def _drop_duplicated_rows(self):
    #     """[summary]
    #     """
    #     self.index_PEF_df["ee"].dropna(axis=0, how="any", inplace=True)
    #     self.index_PEF_df["ee"].sort_values("index", inplace=True)
    #     # print('dupp', self.index_PEF_df["ee"].duplicated(subset=["name", "compartment", "subcompartment", "unitName"], keep='first').sum())
    #     # df = self.index_PEF_df["ee"][self.index_PEF_df["ee"].duplicated(subset=["name", "compartment", "subcompartment", "unitName"])]
    #     # df.to_excel('dupp.xlsx')
    #     self.index_PEF_df["ee"].drop_duplicates(
    #         subset=["name", "compartment", "subcompartment", "unitName"],
    #         keep="first",
    #         inplace=True,
    #     )