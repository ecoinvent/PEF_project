import numpy as np
import scipy.sparse as sp

from pef.Data import PefData

glucose_product = 15386

glucose_flow = [
    527,
    529,
    531,
    532,
    535,
    540,
    541,
    542,
    543,
    544,
    545,
    546,
    547,
    548,
    549,
    551,
    553,
    554,
    559,
    560,
    561,
    562,
    566,
    567,
    571,
    572,
    573,
    574,
    576,
    578,
    579,
    583,
]
import pandas as pd

silicon_prod = 15250

silicon = pd.DataFrame([
    ("silicon, electronics grade", "silicon production, electronics grade", "RER", 0.025),
    ("silicon, electronics grade", "silicon production, electronics grade", "RoW", 0.025),
    ("silicon, metallurgical grade", "silicon production, metallurgical grade", "RER", 0.1875),
    ("silicon, metallurgical grade", "silicon production, metallurgical grade", "GLO", 0.5625),
    ("silicon, solar grade", "silicon production, solar grade, modified Siemens process", "RER", 0.02375),
    ("silicon, solar grade", "silicon production, solar grade, modified Siemens process", "GLO", 0.17625)
])


def cleanup(conf, data: PefData) -> PefData:
    # adding the missing rows to B matrix
    X = np.zeros((10, data.B.shape[1]))
    B = np.vstack((data.B.toarray(), X))
    # fixing a bug with nan value in B due to step 6B
    # There are 34 values that were meant to be deleted (blank in new amount column).
    for (row, col) in np.argwhere(np.isnan(B)):
        B[row, col] = 0
    data.B = sp.csc_matrix(B)

    ##fixing enzyme
    if data.B[1339, 15286] == 0.95:
        print("enzymes production, from arable")
        data.B[1339, 15286] = 0
    else:
        print("nope")

    ##fixing glucose
    for i in glucose_flow:
        data.B[i, glucose_product] = data.B[i, glucose_product] / 1000


    ##fixing silicon
    silicon_new_inventory = silicon.merge(data.A_idx, left_on=[0, 1, 2], right_on=["product", "activityName", "geography"])[["index", 3]]
    data.A[:, silicon_prod] = 0
    data.A[silicon_prod, silicon_prod] = 1
    for _, v in silicon_new_inventory.iterrows():
        data.A[v["index"], silicon_prod] = -v[3]
    return data

