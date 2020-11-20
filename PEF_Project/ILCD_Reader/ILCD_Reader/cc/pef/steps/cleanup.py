import numpy as np
import scipy.sparse as sp


def cleanup(conf, data):
    A, B, C, A_idx, deliverables = [data[k] for k in ("A", "B", "C", "A_idx", "deliverables")]

    # adding the missing rows to B matrix
    X = np.zeros((10, B.shape[1]))
    B = np.vstack((B.toarray(), X))

    # fixing a bug with nan value in B due to step 6B
    # There are 34 values that were meant to be deleted (blank in new amount column).
    for (row, col) in np.argwhere(np.isnan(B)):
        B[row, col] = 0
    data["B"] = sp.csc_matrix(B)
    return data
