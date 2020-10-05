import pandas as pd
from matrix_update import MatrixUpdate

# Correct amount and unit of elementary exchanges in matrix A or Z.
# updating the values in matrix A or Z


class MatrixAZUpdate(MatrixUpdate):
    def __init__(self, matrix):
        super().__init__(matrix)
        self.merged_updatedAmount_index_df = None

    def ReplaceElements(self):
        self._ReadFiles()
        self._merge_updated_resources_with_index()
        self._Replace_Matrix_Elements()
        self._write2Pickle()

    def _merge_updated_resources_with_index(self):
        """[summary]
        """
        self.merged_updatedAmount_index_df = pd.merge(
            self.updated_resources_df,
            self.index_pef_df["ie"],
            how="inner",
            left_on=[
                "exchange name",
                "activityLink_activityName",
                "activityLink_geography",
            ],
            right_on=["product", "activityName", "geography"],
        )

    def _Replace_Matrix_Elements(self):
        """[summary]
        """
        index_array = self.merged_updatedAmount_index_df["index"].to_numpy()
        newvalues_array = self.merged_updatedAmount_index_df[
            "new exchange amount"
        ].to_numpy()
        count = 0
        for idx, new_value in zip(index_array, newvalues_array):
            self.csc_Matrix_data[idx, idx] = new_value
            count = count + 1
        print("Values Updated in Matrix", count)


if __name__ == "__main__":
    flag = True
    while flag:
        matrix = input("Please Enter Matrix name. Either A or Z ").upper()
        if matrix == "A" or matrix == "Z":
            obj = MatrixAZUpdate(matrix)
            obj.ReplaceElements()
            flag = False
        else:
            print("Please Enter the correct matrix name, (A or Z)")
