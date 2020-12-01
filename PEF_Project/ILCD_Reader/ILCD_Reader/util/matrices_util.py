
import file_utils
from files_path import MATRIX_A_PICKLE


def generate_Z_matrix():
    A = file_utils.load_pkl_file(MATRIX_A_PICKLE)
    Z = -A
    Z.setdiag(0)
    file_utils.dump_to_pickle("Z", Z)


generate_Z_matrix()
