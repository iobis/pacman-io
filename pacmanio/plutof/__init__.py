from dotenv.main import load_dotenv
import os
import requests
import logging
import json
import re
from urllib3 import encode_multipart_formdata


load_dotenv()
logger = logging.getLogger("pacmanio")


PACMAN_PROJECT_ID = 98281


class PlutofReader:

    def __init__(self, project_id: int = PACMAN_PROJECT_ID):
        self.project_id = project_id
        self.access_token = self.get_token()

    def resolve(self, items: list, fields: list):
        for field in fields:
            keys = set([item[field] for item in items])
            results_map = dict([(key, self.fetch(key)) for key in keys])
            for item in items:
                item[field] = results_map[item[field]]

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
                    if "results" in results:
                        results = results["results"]
                    else:
                        results = results["data"]
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

    def update_sample(self, sample):

        sample_id = sample["id"]
        url = f"https://api.plutof.ut.ee/v1/taxonoccurrence/materialsample/materialsamples/{sample_id}/"
        res = requests.put(
            url,
            data=json.dumps(sample),
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "User-Agent": "PacMAN",
                "Content-Type": "application/json; charset=UTF-8"
            }
        )
        logger.debug(res.status_code)
        return res.json()

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

    def get_measurements_for_samples(self, samples: list):

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
            download_link = file["download_links"]["link"]
            if download_link is not None:
                file["download_link"] = download_link
            item["file"] = file
        return items

    def get_files_for_samples(self, samples: list):

        sample_ids = list(set([sample["id"] for sample in samples]))
        results = []
        for sample_id in sample_ids:
            page = self.get_files(material_sample=sample_id)
            results.extend(page)
        return results

    def get_specimens_for_samples(self, samples: list):

        specimens = []
        for sample in samples:
            page = self.get_specimens(material_sample=sample["id"])
            specimens.extend(page)
        return specimens

    def get_dnas(self, material_sample: int = None):

        url = f"https://api.plutof.ut.ee/v1/dna-lab/dnas/?include=dna_extraction&material_sample={material_sample}&page[number]="
        items = self.paginate(url)
        return items

    def get_dnas_for_samples(self, samples: list):

        dnas = []
        for sample in samples:
            page = self.get_dnas(material_sample=sample["id"])
            dnas.extend(page)
        return dnas

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
