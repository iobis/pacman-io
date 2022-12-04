import os
from pacmanio.template import Template
import numpy as np


class Archive:

    def __init__(self, path: str = None):

        self.template: Template = None

        if path is not None:
            self.path: str = os.path.expanduser(path)
            self.read(self.path)

    def read(self, path: str) -> None:

        if os.path.exists(path) and os.path.exists(path):
            files = os.listdir(path)
            excel_files = [file for file in files if os.path.isfile(os.path.join(path, file)) and file.endswith(".xlsx") and not file.startswith("~")]

            if len(excel_files) == 1:
                self.template = Template(os.path.join(path, excel_files[0]))
            elif len(excel_files) > 1:
                raise Exception("Found more than one Excel file")
            elif len(excel_files) == 0:
                raise Exception("No Excel file found")

    def generate_dwca(self) -> None:

        if self.template is None:
            raise Exception("No template")

        # TODO: generate EML

        # TODO: generate event core

        event = self.template.samples.df
        event.rename(columns={
            "Sample / plate ID": "eventID",
            "Plate deployment ID": "parentEventID",
            "Longitude": "decimalLongitude",
            "Latitude": "decimalLatitude",
            "Type": "type",
            "Date": "eventDate"
        }, inplace=True)
        event = event.loc[:, ["eventID", "parentEventID", "decimalLongitude", "decimalLatitude", "eventDate", "type"]]

        # TODO: generate occurrence extension

        occurrence = self.template.vouchers.df
        occurrence.rename(columns={
            "Sample / plate": "eventID",
            "Specimen voucher ID": "materialSampleID",
            "Identified by": "identifiedBy",
            "Phylum": "phylum",
            "Subphylum": "subphylum",
            "Class": "class",
            "Order": "order",
            "Family": "family",
            "Genus": "genus",
            "Species": "species",
            "Abundance": "organismQuantity"
        }, inplace=True)
        occurrence.replace({np.nan: None}, inplace=True)

        occurrence["occurrenceRemarks"] = occurrence[
            ["Morphology", "Growth pattern", "Habitat", "Texture", "Odor", "External color", "Notes"]
        ].apply(lambda values: "; ".join([value for value in values if value is not None]), axis=1)

        occurrence["scientificName"] = occurrence[
            ["phylum", "subphylum", "class", "order", "family", "genus", "species"]
        ].apply(lambda values: [value for value in values if value is not None][-1], axis=1)

        occurrence = occurrence.loc[:, ["eventID", "materialSampleID", "scientificName", "identifiedBy", "phylum", "subphylum", "class", "order", "family", "genus", "species", "organismQuantity", "occurrenceRemarks"]]

        # TODO: generate measurementorfact extension

        # TODO: generate archive
