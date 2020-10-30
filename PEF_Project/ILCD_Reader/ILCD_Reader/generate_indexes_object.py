from matrix_index  import MatrixIndex
import pickle


def write_to_pickle(matrix_index_obj):
    with open("D:\\ecoinvent_scripts\\index.pkl", "wb") as outfile:
            pickle.dump(matrix_index_obj, outfile, pickle.HIGHEST_PROTOCOL)


obj = MatrixIndex()
obj.convert_to_indexes_pickle()

write_to_pickle(obj)