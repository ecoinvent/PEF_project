from lxml import objectify
import pandas as pd
from tqdm import tqdm
from util.file_utils import create_file_list
import os
import files_path


class EditProcessFiles:

    common = "{http://lca.jrc.it/ILCD/Common}"
    process = "{http://lca.jrc.it/ILCD/Process}"

    def __init__(self, source_template, source_xml_dir, destination_xml_dir):
        self.source_template = source_template
        self.source_xml_dir = source_xml_dir
        self.destination_xml_dir = destination_xml_dir
        self.myfile = None

    def edit_files(self):
        self._open_xml_file()

    def read_excelTemplate(self):
        """[summary]
        """
        try:
            df = pd.read_excel(self.source_template, sheet_name=None)
            pandaSeries_row = df["metadata_toAll"].iloc[0, :]
        except FileNotFoundError:
            print("Error: Excel file wasn't found")
        else:
            for row_tuple in tqdm(df["metadata_datasetSpec"].itertuples()):
                self.__open_xml_file(row_tuple, pandaSeries_row)

    def _create_name_list(self):
        """[summary]
        """
        self.files = create_file_list(self.source_xml_dir)

    # def _open_xml_file(self, row_tuple, pandaSeries_row):
    def _open_xml_file(self, row_tuple, pandaSeries_row):
        """[summary]

        Args:
            row_tuple ([tuple]): [description]
            pandaSeries_row ([pandas.core.series]): [description]
        """
        self._create_name_list()
        for file in self.files:
            try:
                with open(file, "r", encoding="utf-8") as xml_f:
                    self.myfile = objectify.parse(xml_f)
                    root = self.myfile.getroot()
                    self._edit_xml_elements(root, row_tuple, pandaSeries_row)
            except IOError as error:
                print(f"Error: Couldnt process {file}, {error}")

    def _edit_xml_elements(self, root, row_tuple, pandaSeries_row):
        """[summary]

        Args:
            root ([type]): [description]
            row_tuple ([tuple]): [description]
            pandaSeries_row ([pandas.core.series]): [description]
        """
        print(root.tag)
        dataSet_information = root.processInformation.dataSetInformation
        lcia_method_allocation = root.modellingAndValidation.LCIMethodAndAllocation
        publication_ownership = root.administrativeInformation.publicationAndOwnership
        dataCutOff_Completeness = (
            root.modellingAndValidation.dataSourcesTreatmentAndRepresentativeness.dataCutOffAndCompletenessPrinciples
        )
        validation_review = root.modellingAndValidation.validation.review

        dataSet_information.name.baseName._setText(row_tuple.baseName)
        dataSet_information[f"{EditProcessFiles.common}generalComment"]._setText(
            row_tuple.generalComment
        )
        root.processInformation.time[f"{EditProcessFiles.common}referenceYear"]._setText(
            str(row_tuple.referenceYear)
        )
        root.processInformation.time[
            f"{EditProcessFiles.common}dataSetValidUntil"
        ]._setText(str(row_tuple.dataSetValidUntil))
        root.processInformation.technology.technologyDescriptionAndIncludedProcesses._setText(
            row_tuple.technologyDescriptionAndIncludedProcesses
        )

        lcia_method_allocation.typeOfDataSet._setText(row_tuple.typeOfDataSet)
        lcia_method_allocation.deviationsFromLCIMethodPrinciple._setText(pandaSeries_row[0])
        lcia_method_allocation.modellingConstants._setText(pandaSeries_row[1])
        dataCutOff_Completeness._setText(pandaSeries_row[2])
        dataCutOff_Completeness._setText(pandaSeries_row[3])
        validation_review[0][f"{EditProcessFiles.common}reviewDetails"]._setText(
            pandaSeries_row[4]
        )
        validation_review[1][f"{EditProcessFiles.common}reviewDetails"]._setText(
            pandaSeries_row[5]
        )
        root.administrativeInformation[f"{EditProcessFiles.common}commissionerAndGoal"][
            f"{EditProcessFiles.common}project"
        ]._setText(pandaSeries_row[6])

        root.administrativeInformation.dataEntryBy[
            f"{EditProcessFiles.common}timeStamp"
        ]._setText(str(pandaSeries_row[7]))

        publication_ownership[f"{EditProcessFiles.common}dateOfLastRevision"]._setText(
            str(pandaSeries_row[8])
        )

        publication_ownership[f"{EditProcessFiles.common}dataSetVersion"]._setText(
            str(pandaSeries_row[9])
        )

        publication_ownership[f"{EditProcessFiles.common}accessRestrictions"]._setText(
            pandaSeries_row[10]
        )
        publication_ownership[f"{EditProcessFiles.common}licenseType"]._setText(
            pandaSeries_row[11]
        )

        self.__write_xml(row_tuple.processFile)

    def _write_xml(self, file_name):
        """[summary]

        Args:
            file_name ([type]): [description]
        """
        self.myfile.write(
            os.path.join(self.destination_xml_dir, f"{file_name}.xml"),
            encoding="utf-8",
            standalone=False,
            xml_declaration=True,
            pretty_print=True,
        )


obj = EditProcessFiles(
    r"D:\ecoinvent_scripts\PEF_Project\ILCD_Reader\Data\input\excel\changes to ILCD.xlsx",
    files_path.PROCESS_FILES_SOURCE_DIR,
    files_path.PROCESS_FILES_Destination_DIR
)
obj.edit_files()
