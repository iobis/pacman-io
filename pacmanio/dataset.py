import os
from pacmanio.template import Template
import numpy as np
import pyworms
from dwcawriter import Archive, Table
import logging


logger = logging.getLogger("pacmanio")


class Dataset:

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

    def generate_dwca(self, match_worms=True) -> None:

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

        if match_worms:
            names = occurrence["scientificName"].values.tolist()
            logger.debug(f"Matching {len(names)} names")
            all_matches = [pyworms.aphiaRecordsByMatchNames(name) for name in names]
            assert (len(all_matches) == len(names))

            def select_match(matches):
                matches = matches[0]
                good_matches = [match for match in matches if match["match_type"] == "exact" or match["match_type"] == "exact_subgenus"]
                if len(good_matches) > 0:
                    return "urn:lsid:marinespecies.org:taxname:" + str(good_matches[0]["AphiaID"])

            matched_ids = list(map(select_match, all_matches))
            occurrence["scientificNameID"] = matched_ids

        # TODO: generate measurementorfact extension

        # TODO: generate archive

        archive = Archive()
        archive.eml_text = ""

        core_table = Table(spec="https://rs.gbif.org/core/dwc_event_2022-02-02.xml", data=event, id_index=0, only_mapped_columns=True)
        archive.core = core_table

        extension_table = Table(spec="https://rs.gbif.org/core/dwc_occurrence_2022-02-02.xml", data=occurrence, id_index=0)
        archive.extensions.append(extension_table)

        print(archive)
