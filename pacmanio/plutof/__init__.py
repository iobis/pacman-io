from dotenv.main import load_dotenv
import os
import requests
import logging
from typing import List
import json
import re
from dwcawriter import Archive, Table
import pandas as pd
from pacmanio.util import match_names
from urllib3 import encode_multipart_formdata


load_dotenv()
logger = logging.getLogger("pacmanio")


class PlutofReader:

    def __init__(self):
        self.project_id = 98281
        self.access_token = self.get_token()

    def get_token(self):
        url = "https://api.plutof.ut.ee/v1/public/auth/token/"
        logger.debug(url)
        res = requests.post(url, data={
            "username": os.getenv("PLUTOF_USER"),
            "password": os.getenv("PLUTOF_PASS"),
            "client_id": os.getenv("PLUTOF_CLIENT_ID"),
            "client_secret": os.getenv("PLUTOF_CLIENT_SECRET"),
            "grant_type": "password"
        })
        logger.debug(res.status_code)
        data = res.json()
        return data["access_token"]

    def fetch(self, url):

        logger.debug(url)
        res = items = requests.get(url, headers={
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": "PacMAN"
        })
        logger.debug(res.status_code)
        items = res.json()
        return items

    def paginate(self, url):

        page = 1
        items = []
        while True:
            page_url = url + str(page)
            logger.debug(page_url)
            res = requests.get(page_url, headers={
                "Authorization": f"Bearer {self.access_token}",
                "User-Agent": "PacMAN"
            })
            logger.debug(res.status_code)
            if res.status_code != 200:
                break
            else:
                results = res.json()
                if type(results) is not list:
                    results = results["results"]
                if len(results) == 0:
                    break
                items = items + results
                page = page + 1
        return items

    def set_image_access(self, file_id, view="PUBLIC"):
        url = "https://api.plutof.ut.ee/v1/access/"
        logger.debug(url)
        res = requests.post(
            url,
            data=json.dumps({
                "access_edit": "PRIVATE",
                "access_view": view,
                "hyperlinks": [
                    f"https://api.plutof.ut.ee/v1/filerepository/files/{file_id}/"
                ]
            }),
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "User-Agent": "PacMAN",
                "Content-Type": "application/json; charset=UTF-8"
            }
        )
        logger.debug(res.status_code)

    def upload_file(self, path, filename, sample_id):

        # filerepository

        size = str(os.path.getsize(path))
        fields = {
            "file": (filename, open(path, "rb").read(), "image/jpeg"),
            "resumableChunkNumber": "1",
            "resumableTotalChunks": "1",
            "resumableChunkSize": size,
            "resumableCurrentChunkSize": size,
            "resumableTotalSize": size,
            "resumableFilename": filename,
            "resumableRelativePath": filename,
            "resumableIdentifier": "1234",
            "resumableType": "image/jpeg"
        }
        body, header = encode_multipart_formdata(fields)
        res_content = requests.post("https://api.plutof.ut.ee/v1/filerepository/fileuploads/content/", body, headers={
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": "PacMAN",
            "Content-Type": header,
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "*/*",
            "Content-Length": size
        })

        # files

        upload_id = res_content.json()["upload_id"]
        res_files = requests.post(
            "https://api.plutof.ut.ee/v1/filerepository/files/",
            data=json.dumps({
                "identifier": filename,
                "original_name": filename,
                "format": "image/jpeg",
                "file_upload": f"https://api.plutof.ut.ee/v1/filerepository/fileuploads/{upload_id}/",
                "license": "https://api.plutof.ut.ee/v1/filerepository/licenses/2/",
                "type": 18
            }),
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "User-Agent": "PacMAN",
                "Content-Type": "application/json; charset=UTF-8"
            }
        )

        # items

        requests.post(
            "https://api.plutof.ut.ee/v1/filerepository/items/",
            data=json.dumps({
                "object_id": sample_id,
                "is_public": True,
                "content_type": "https://api.plutof.ut.ee/v1/contenttypes/209/",
                "file": res_files.json()["url"]
            }),
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "User-Agent": "PacMAN",
                "Content-Type": "application/json; charset=UTF-8"
            }
        )

    def get_project(self):

        url = f"https://api.plutof.ut.ee/v1/public/projects/{self.project_id}/"
        items = self.fetch(url)
        return items

    def get_samples(self):

        url = f"https://api.plutof.ut.ee/v1/taxonoccurrence/materialsample/materialsamples/?page_size=20&study={self.project_id}&page="
        items = self.paginate(url)
        return items

    def get_areas(self):

        url = f"https://api.plutof.ut.ee/v1/sample/samplingareas/search/?page_size=20&study={self.project_id}&page="
        items = self.paginate(url)
        return items

    def get_sample(self, sample_id):

        url = f"https://api.plutof.ut.ee/v1/taxonoccurrence/materialsample/materialsamples/{sample_id}/"
        sample = self.fetch(url)
        return sample

    def get_events(self):

        samples = self.get_samples()
        event_urls = list(set([sample["samplingevent"] for sample in samples]))
        items = list(map(self.fetch, event_urls))
        return items

    def get_specimens(self, material_sample: int = None):

        if material_sample is not None:
            url = f"https://api.plutof.ut.ee/v1/taxonoccurrence/specimen/specimens/?page_size=50&related_materialsample={material_sample}&page="
        else:
            url = f"https://api.plutof.ut.ee/v1/taxonoccurrence/specimen/specimens/search/?page_size=50&parent_taxon=true&study={self.project_id}&include_cb=true&page="
        items = self.paginate(url)
        return items

    def get_measurements(self, material_sample: int = None):

        url = f"https://api.plutof.ut.ee/v1/measurement/objectmeasurements/?content_type=209&object_id={material_sample}&page="
        items = self.paginate(url)
        return items

    def get_measurements_for_samples(self, samples: List):

        results = []
        for sample in samples:
            page = self.get_measurements(material_sample=sample["id"])
            for measurement in page:
                measurement["sample"] = sample
            results.extend(page)
        return results

    def get_files(self, material_sample: int, content_type=209):

        url = f"https://api.plutof.ut.ee/v1/filerepository/items/?content_type={content_type}&object_id={material_sample}&page_size=20&page="
        items = self.paginate(url)
        for item in items:
            file = self.fetch(item["file"])
            public_url = file["public_url"]
            if public_url is not None:
                file["public_url"] = f"https://files.plutof.ut.ee/{public_url}"
            item["file"] = file
        return items

    def get_files_for_samples(self, samples: List):

        sample_ids = list(set([sample["id"] for sample in samples]))
        results = []
        for sample_id in sample_ids:
            page = self.get_files(material_sample=sample_id)
            results.extend(page)
        return results

    def get_specimens_for_samples(self, samples: List):

        specimens = []
        for sample in samples:
            page = self.get_specimens(material_sample=sample["id"])
            for specimen in specimens:
                specimen["sample"] = sample
            specimens.extend(page)
        return specimens

    def delete(self, url):

        logger.debug(url)
        res = requests.delete(url, headers={
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": "PacMAN"
        })
        logger.debug(res.status_code)
        if res.status_code != 204:
            logger.error(res.text)

    def find_taxon_id(self, name):
        url = f"https://api.plutof.ut.ee/v1/taxonomy/taxonnodes/autocomplete/?name={name}&page_size=20&page=1&access=view&is_deleted=false&tree=1"
        res = self.fetch(url)
        names = res["results"]
        if len(names) > 0 and names[0]["taxon_name"] == name:
            return names[0]["id"]

    def create_specimen(self, specimen_name, materialsample_id, taxon_id, remarks=None):

        sample = self.get_sample(materialsample_id)
        search = re.search("samplingevents/([0-9]+)/", sample["samplingevent"])
        event_id = search.group(1)

        specimen_data = {
            "location_in_collection": "",
            "attrib_isolation_date": "",
            "attrib_emergence_date": "",
            "specimen_idprim_sub": "",
            "coll_det_remarks": "",
            "specimen_idprim": specimen_name,
            "collected_date": "",
            "exsiccata_no": "",
            "remarks": remarks,
            "on_clipboard": False,
            "in_gbif": False,
            "download_count": None,
            "active_transaction_id": None,
            "taxon_name": "",
            "collected_by": "",
            "in_elurikkus": False,
            "occurrence_source": "",
            "url": "",
            "created_at": "",
            "updated_at": "",
            "is_public": False,
            "related_materialsample": f"https://api.plutof.ut.ee/v1/taxonoccurrence/materialsample/materialsamples/{materialsample_id}/",
            "exsiccata": None,
            "substrate": None,
            "sub_repository": None,
            "deposited_in": None,
            "mainform": "https://api.plutof.ut.ee/v1/measurement/mainforms/168/",
            "project": f"https://api.plutof.ut.ee/v1/study/studies/{self.project_id}/",
            "samplingevent": f"https://api.plutof.ut.ee/v1/sample/samplingevents/{event_id}/",
            "created_by": None,
            "updated_by": None,
            "owner": None
        }

        specimen_url = "https://api.plutof.ut.ee/v1/taxonoccurrence/specimen/specimens/"
        logger.debug(specimen_url)
        specimen_res = requests.post(
            specimen_url,
            data=json.dumps(specimen_data),
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "User-Agent": "PacMAN",
                "Content-Type": "application/json; charset=UTF-8"
            }
        )
        logger.debug(specimen_res.status_code)
        specimen = specimen_res.json()
        specimen_id = specimen["id"]

        determination_data = {
            "object_id": f"{specimen_id}",
            "new_name": "",
            "remarks": "",
            "assessment": None,
            "determined_date": "",
            "basis": [],
            "is_current": False,
            "owner_name": "",
            "is_deleted": False,
            "can_edit": 3,
            "url": "",
            "created_at": "",
            "updated_at": "",
            "is_public": False,
            "content_type": "https://api.plutof.ut.ee/v1/contenttypes/147/",
            "taxon_node": f"https://api.plutof.ut.ee/v1/taxonomy/taxonnodes/{taxon_id}/",
            "typification": None,
            "created_by": None,
            "updated_by": None,
            "owner": None
        }

        determination_url = "https://api.plutof.ut.ee/v1/determination/determinations/"
        logger.debug(determination_url)
        determination_res = requests.post(
            determination_url,
            data=json.dumps(determination_data),
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "User-Agent": "PacMAN",
                "Content-Type": "application/json; charset=UTF-8"
            }
        )
        logger.debug(determination_res.status_code)

    def create_measurement(self, name, description, type):

        data = {
            "name": name,
            "description": description,
            "type": type,
            "name_translated": name,
            "description_translated": description,
            "language": "https://api.plutof.ut.ee/v1/geography/languages/123/"
        }

        url = "https://api.plutof.ut.ee/v1/measurement/measurements/"
        logger.debug(url)
        res = requests.post(
            url,
            data=json.dumps(data),
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "User-Agent": "PacMAN",
                "Content-Type": "application/json; charset=UTF-8"
            }
        )
        logger.debug(res.status_code)

    def clear_project(self):

        answer = input("Are you sure? [y/N] ")
        if answer.lower() not in ["y", "yes"]:
            return

        samples = self.get_samples()
        specimens = self.get_specimens_for_samples(samples)
        events = self.get_events()

        for specimen in specimens:
            specimen_id = specimen["id"]
            url = f"https://api.plutof.ut.ee/v1/taxonoccurrence/specimen/specimens/{specimen_id}/"
            self.delete(url)

        for sample in samples:
            sample_id = sample["id"]
            url = f"https://api.plutof.ut.ee/v1/taxonoccurrence/materialsample/materialsamples/{sample_id}/"
            self.delete(url)

        for event in events:
            event_id = event["id"]
            url = f"https://api.plutof.ut.ee/v1/sample/samplingevents/{event_id}/"
            self.delete(url)

    def generate_dwca(self, match_worms=True) -> Archive:

        # event core

        areas = self.get_areas()
        areas_df = pd.DataFrame({
            "eventID": [area["name"] for area in areas],
            "parentEventID": ["FIJI" if area["name"] != "FIJI" else None for area in areas],
            "locality": [area["locality_text"] for area in areas],
            "country": [area["country"] for area in areas],
            "footprintWKT": [area["geom"] for area in areas]
        })
        areas_df[["decimalLongitude", "decimalLatitude"]] = areas_df["footprintWKT"].apply(lambda x: pd.Series(re.findall("\d+\.\d+", x) if x is not None else [None, None]))

        events = self.get_events()
        events_df = pd.DataFrame({
            "eventID": [event["event_id"] for event in events],
            "parentEventID": [event["event_id"][0:5] for event in events],
            "country": [event["country"] for event in events],
            "eventDate": [event["timespan_begin"] for event in events]
        })

        samples = self.get_samples()
        samples_df = pd.DataFrame({
            "eventID": [sample["name"] for sample in samples],
            "parentEventID": [sample["name"][0:14] for sample in samples],
            "eventDate": [sample["timespan_begin"] for sample in samples]
        })

        event = pd.concat([areas_df, events_df, samples_df])

        # multimedia extension

        files = self.get_files_for_samples(samples)
        multimedia_df = pd.DataFrame({
            "eventID": [file["content_object"]["name"] for file in files],
            "type": "StillImage",
            "format": [file["file"]["format"] for file in files],
            "identifier": [file["file"]["public_url"] for file in files],
            "references": [file["file"]["url"] for file in files],
            "license": [file["file"]["license"] for file in files],
        })

        # occurrence extension

        specimens = self.get_specimens_for_samples(samples)
        specimen_df = pd.DataFrame({
            "eventID": [specimen["sample"]["name"] for specimen in specimens],
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

        # measurementorfact extension

        logger.warning("Environmental samples assumed to contain _ENV in name")
        env_samples = [sample for sample in samples if "_ENV" in sample["name"]]
        measurements = self.get_measurements_for_samples(env_samples)
        measurement_df = pd.DataFrame({
            "eventID": [measurement["sample"]["name"] for measurement in measurements],
            "measurementType": [measurement["measurement_name"] for measurement in measurements],
            "measurementValue": [measurement["value"] for measurement in measurements],
            "measurementTypeID": [measurement["measurement"] for measurement in measurements],
        })

        # archive

        archive = Archive()
        archive.eml_text = ""

        core_table = Table(spec="https://rs.gbif.org/core/dwc_event_2022-02-02.xml", data=event, id_index=0, only_mapped_columns=True)
        archive.core = core_table

        extension_table = Table(spec="https://rs.gbif.org/core/dwc_occurrence_2022-02-02.xml", data=specimen_df, id_index=0)
        archive.extensions.append(extension_table)

        extension_table_env = Table(spec="https://rs.gbif.org/extension/obis/extended_measurement_or_fact.xml", data=measurement_df, id_index=0)
        archive.extensions.append(extension_table_env)

        extension_table_mm = Table(spec="https://rs.gbif.org/extension/gbif/1.0/multimedia.xml", data=multimedia_df, id_index=0)
        archive.extensions.append(extension_table_mm)

        return archive
