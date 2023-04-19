import re
import pandas as pd
from dwcawriter import Archive, Table
from pacmanio.util import match_names
from pacmanio.plutof import PlutofReader
import logging


logger = logging.getLogger("pacmanio")


def generate_dwca(plutof_reader: PlutofReader, match_worms=True, remove_missing_names=False) -> Archive:

    # event core

    areas = plutof_reader.get_areas()
    areas_df = pd.DataFrame({
        "eventID": [area["name"] for area in areas],
        "parentEventID": ["FIJI" if area["name"] != "FIJI" else None for area in areas],
        "locality": [area["locality_text"] for area in areas],
        "country": [area["country"] for area in areas],
        "footprintWKT": [area["geom"] for area in areas]
    })
    areas_df[["decimalLongitude", "decimalLatitude"]] = areas_df["footprintWKT"].apply(lambda x: pd.Series(re.findall("-?\d+\.\d+", x) if x is not None else [None, None]))
    areas_df["coordinateUncertaintyInMeters"] = [10.0 if wkt is not None and wkt.startswith("POINT") else None for wkt in areas_df["footprintWKT"]]

    events = plutof_reader.get_events()
    events_df = pd.DataFrame({
        "eventID": [event["event_id"] for event in events],
        "parentEventID": [event["event_id"][0:5] for event in events],
        "country": [event["country"] for event in events],
        "eventDate": [event["timespan_begin"] for event in events]
    })

    samples = plutof_reader.get_samples()
    samples_df = pd.DataFrame({
        "eventID": [sample["name"] for sample in samples],
        "parentEventID": [sample["name"][0:14] for sample in samples],
        "eventDate": [sample["timespan_begin"] for sample in samples],
        "minimumDepthInMeters": 0.0,
        "maximumDepthInMeters": 30.0
    })

    event_df = pd.concat([areas_df, events_df, samples_df])

    # fix for duplicate events

    event_df.drop_duplicates(subset=["eventID"], inplace=True)

    # multimedia extension

    files = plutof_reader.get_files_for_samples(samples)
    multimedia_df = pd.DataFrame({
        "eventID": [file["content_object"]["name"] for file in files],
        "type": "StillImage",
        "format": [file["file"]["format"] for file in files],
        "identifier": [file["file"]["public_url"] for file in files],
        "references": [file["file"]["url"] for file in files],
        "license": [file["file"]["license"] for file in files],
    })

    # occurrence extension

    specimens = plutof_reader.get_specimens_for_samples(samples)
    plutof_reader.resolve(specimens, ["related_materialsample"])
    specimen_df = pd.DataFrame({
        "eventID": [specimen["related_materialsample"]["name"] if "related_materialsample" in specimen else None for specimen in specimens],
        "occurrenceID": ["pacman:specimen:" + specimen["specimen_id"] for specimen in specimens],
        "occurrenceStatus": "present",
        "basisOfRecord": "PreservedSpecimen",
        "materialSampleID": [specimen["specimen_id"] for specimen in specimens],
        "occurrenceRemarks": [specimen["remarks"].replace("\xa0", " ") for specimen in specimens],
        "scientificName": [specimen["taxon_name"] if "taxon_name" in specimen else None for specimen in specimens],
    })

    if match_worms:
        names = specimen_df["scientificName"].values.tolist()
        specimen_df["scientificNameID"] = match_names(names)

    if remove_missing_names:
        specimen_df = specimen_df[specimen_df["scientificNameID"].notnull()]

    # fix for missing eventID

    specimen_df = specimen_df[specimen_df["eventID"].notnull()]

    # measurementorfact extension

    logger.warning("Environmental samples assumed to contain _ENV in name")
    env_samples = [sample for sample in samples if "_ENV" in sample["name"]]
    measurements = plutof_reader.get_measurements_for_samples(env_samples)
    measurement_df = pd.DataFrame({
        "eventID": [measurement["sample"]["name"] for measurement in measurements],
        "measurementType": [measurement["measurement_name"] for measurement in measurements],
        "measurementValue": [measurement["value"] for measurement in measurements],
        "measurementTypeID": [measurement["measurement"] for measurement in measurements],
    })

    # archive

    archive = Archive()
    archive.eml_text = ""

    core_table = Table(spec="https://rs.gbif.org/core/dwc_event_2022-02-02.xml", data=event_df, id_index=0, only_mapped_columns=True)
    archive.core = core_table

    extension_table = Table(spec="https://rs.gbif.org/core/dwc_occurrence_2022-02-02.xml", data=specimen_df, id_index=0)
    archive.extensions.append(extension_table)

    extension_table_env = Table(spec="https://rs.gbif.org/extension/obis/extended_measurement_or_fact.xml", data=measurement_df, id_index=0)
    archive.extensions.append(extension_table_env)

    extension_table_mm = Table(spec="https://rs.gbif.org/extension/gbif/1.0/multimedia.xml", data=multimedia_df, id_index=0)
    archive.extensions.append(extension_table_mm)

    return archive
