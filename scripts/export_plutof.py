from pacmanio.plutof import PlutofReader
from pacmanio.dataset import Dataset
import logging
import os
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig
from eml.gbif_1_1 import AgentType, Eml, Dataset, IndividualName, IntellectualRights, KeywordSet, Para, Ulink, Abstract


logging.basicConfig()
logging.getLogger("pacmanio").setLevel(logging.DEBUG)


plutof = PlutofReader()
archive = plutof.generate_dwca()

abstract = "This dataset contains the first voucher specimens collected by the PacMAN project, which aims to set up an invasive alien species monitoring network and early alert system in the Pacific Small Island Developing States (PSIDS). The voucher specimens collected here were found on settlement plates deployed in and around Suva harbour, Fiji, in 2022. Future versions of this dataset will include results from metabarcoding of water samples and settlement plates, as well as settlement plate and voucher images."
title = "Data collected by the PacMAN project"

doc = Eml(
    #package_id="http://ipt.vliz.be/kmfri/resource?id=vegetation_gazi_bay_kenya_1987/v1.0",
    #system="http://gbif.org",
    lang="eng",
    dataset=Dataset(
        title=title,
        abstract=Abstract([Para([abstract])]),
        #alternate_identifier="http://ipt.vliz.be/kmfri/resource?id=vegetation_gazi_bay_kenya_1987/v1.0",
        creator=[
            AgentType(
                individual_name=[IndividualName("Joape", "Ginigini")],
                organization_name=["University of the South Pacific (USP)"],
                electronic_mail_address=["joape.ginigini@usp.ac.fj"]
            ),
            AgentType(
                individual_name=[IndividualName("Miriama", "Vuiyasawa")],
                organization_name=["University of the South Pacific (USP)"],
                electronic_mail_address=["adimiriama.vuiyasawa@usp.ac.fj"]
            ),
            AgentType(
                individual_name=[IndividualName("Gilianne", "Brodie")],
                organization_name=["University of the South Pacific (USP)"],
                electronic_mail_address=["gilianne.brodie@usp.ac.fj"]
            ),
            AgentType(
                individual_name=[IndividualName("Pieter", "Provoost")],
                organization_name=["IOC-UNESCO"],
                electronic_mail_address=["p.provoost@unesco.org"]
            ),
            AgentType(
                individual_name=[IndividualName("Saara", "Suominen")],
                organization_name=["IOC-UNESCO"],
                electronic_mail_address=["s.suominen@unesco.org"]
            ),
            AgentType(
                individual_name=[IndividualName("Ward", "Appeltans")],
                organization_name=["IOC-UNESCO"],
                electronic_mail_address=["w.appeltans@unesco.org"]
            ),
        ],
        keyword_set=KeywordSet(keyword=["PacMAN", "invasive species"]),
        intellectual_rights=IntellectualRights(
            para=Para(
                [
                    "This work is licensed under a ",
                    Ulink(
                        url="http://creativecommons.org/licenses/by/4.0/legalcode",
                        content=["Creative Commons Attribution (CC-BY) 4.0 License"]
                    ),
                    "."
                ]
            )
        ),
    )
)
config = SerializerConfig(pretty_print=True)
serializer = XmlSerializer(config=config)
eml = serializer.render(doc)

archive.eml_text = eml
archive.export(os.path.expanduser("~/Desktop/temp/pacman.zip"))
