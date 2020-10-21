import os
import pickle
import sys
# from scipy import sparse


def create_file_list(folder_path, ext="xml"):
    """[scan the directory and build a list of files]

    Args:
        folder_path ([str]): [path of the source folder]

    Returns:
        [list]: [list of found files]
    """
    if os.path.isdir(folder_path):
        list_of_files = [
            file.path
            for file in os.scandir(folder_path)
            if os.path.isfile(os.path.join(folder_path, file))
            and file.path.split(".")[-1] == ext
        ]
        return list_of_files

    print("Error: The folder's path provided is not a directory")
    sys.exit()


def load_pkl_file(folder_path, file_name=None):
    """[read the contents of the pickle file]

    Args:
        folder_path ([str]): [description]
        file_name ([str], optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """
    if file_name is not None:
        if file_name[-4:] == ".pkl":
            file_path = os.path.join(folder_path, file_name)
        else:
            file_path = os.path.join(folder_path, f"{file_name}.pkl")
    else:
        file_path = folder_path
    try:
        with open(file_path, "rb") as file:
            pickled_data = pickle.load(file)
        return pickled_data
    except IOError as error:
        print(error)
        print(f"cant process {file_path} ")
        sys.exit()


def dump_to_pickle(matrix_name, csc_matrix):
    """[store the csc sparse matrix into pickle file]

    Args:
        matrix_name ([str]): [name of the matrix]
        csc_matrix ([type]): [csc matrix]
    """
    if isinstance(matrix_name, str) and type(csc_matrix) == sparse.csc.csc_matrix:
        with open(f"{matrix_name}.pkl", "wb") as outfile:
            pickle.dump(csc_matrix, outfile, pickle.HIGHEST_PROTOCOL)


def write_df_to_excel(outputdir, file_name, df):
    template = os.path.join(outputdir, file_name)
    print("Writing to Excel in process...")
    df.to_excel(template, index=False)


def save_xml_file(xml_file=None, target_dir=None, file_name=None):
    """[process of saving xml file to disk]

    Args:
        xml_file ([type], optional): [description]. Defaults to None.
        target_dir ([str], optional): [destination directory]. Defaults to None.
        file_name ([str], optional): [name of modified xml file]. Defaults to None.
    """
    if not xml_file:
        print("Error couldnt generate the xml file, please supply the xml file object")
        return

    if(all([xml_file, target_dir, file_name])):
        path = os.path.join(target_dir, file_name + "." + "xml")
        write_xml(xml_file, path)
        return
    if(all([xml_file, file_name])):
        target_dir = r"D:\ecoinvent_scripts"
        path = os.path.join(target_dir, file_name + "." + "xml")
        write_xml(xml_file, path)
        return
    if(all([xml_file, target_dir])):
        file_name = r"xml_result_file"
        path = os.path.join(target_dir, file_name + "." + "xml")
        write_xml(xml_file, path)
        return


def write_xml(xml_file, path):
    """[writing xml file to disk]

    Args:
        xml_file ([type]): [description]
        path ([str]): [file path]
    """
    xml_file.write(
                path,
                encoding="utf-8",
                standalone=False,
                xml_declaration=True,
                pretty_print=True,
            )
