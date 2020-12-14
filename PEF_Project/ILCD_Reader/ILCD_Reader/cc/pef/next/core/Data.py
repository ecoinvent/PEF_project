from pandas import DataFrame
from scipy.sparse import csc_matrix


class PefData:
    def __init__(self, A: csc_matrix, B: csc_matrix, C: csc_matrix, A_idx: DataFrame, B_idx: DataFrame, C_idx: DataFrame):
        self.A = A
        self.B = B
        self.C = C
        self.A_idx = A_idx
        self.B_idx = B_idx
        self.C_idx = C_idx
