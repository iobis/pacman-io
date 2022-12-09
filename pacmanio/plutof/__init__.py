from dotenv.main import load_dotenv
import os
import requests
import logging
from typing import List


load_dotenv()
logger = logging.getLogger("pacmanio")


class PlutofReader:

    def __init__(self):

        self.project_id = 98281
        url = "https://api.plutof.ut.ee/v1/public/auth/token/"
        logger.info(url)
        res = requests.post(url, data={
            "username": os.getenv("PLUTOF_USER"),
            "password": os.getenv("PLUTOF_PASS"),
            "client_id": os.getenv("PLUTOF_CLIENT_ID"),
            "client_secret": os.getenv("PLUTOF_CLIENT_SECRET"),
            "grant_type": "password"
        })
        data = res.json()
        self.access_token = data["access_token"]

    def fetch(self, url):

        logger.info(url)
        items = requests.get(url, headers={
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": "PacMAN"
        }).json()
        return items

    def paginate(self, url):

        page = 1
        items = []
        while True:
            page_url = url + str(page)
            logger.info(page_url)
            res = requests.get(page_url, headers={
                "Authorization": f"Bearer {self.access_token}",
                "User-Agent": "PacMAN"
            })
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

    def get_project(self):

        url = f"https://api.plutof.ut.ee/v1/public/projects/{self.project_id}/"
        logger.info(url)
        items = self.fetch(url)
        return items

    def get_samples(self):

        url = f"https://api.plutof.ut.ee/v1/taxonoccurrence/materialsample/materialsamples/?page_size=20&study={self.project_id}&page="
        items = self.paginate(url)
        return items

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

    def get_specimens_for_samples(self, samples: List):

        sample_ids = list(set([sample["id"] for sample in samples]))
        specimens = []
        for sample_id in sample_ids:
            page = self.get_specimens(material_sample=sample_id)
            specimens.extend(page)
        return specimens
