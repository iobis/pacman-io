from pacmanio.dataset import Dataset
import logging
import eml.gbif_1_1 as eml
from xsdata.formats.dataclass.serializers import XmlSerializer
from xsdata.formats.dataclass.serializers.config import SerializerConfig
import os


logging.basicConfig()
logging.getLogger("pacmanio").setLevel(logging.DEBUG)


dataset = Dataset("~/Desktop/temp/miri/site_1")
archive = dataset.generate_dwca()

doc = eml.Eml(
    lang="eng",
    dataset=eml.Dataset(
        title="Test dataset",
        abstract=eml.Abstract([eml.Para(["Suspendisse imperdiet imperdiet leo, at eleifend nisi rutrum eget. Donec aliquam mollis risus, feugiat laoreet nulla facilisis vel. Fusce viverra magna ante, ut lobortis sapien convallis ut. Nulla facilisi. Cras at tellus leo. Suspendisse eget blandit tellus. Duis auctor turpis eros. Nullam convallis ligula eleifend volutpat aliquam. Donec cursus mattis viverra. Nunc ac lorem vel lectus malesuada bibendum. Vestibulum non dolor quis enim auctor consectetur in a augue. Maecenas sodales ullamcorper quam."])]),
        creator=eml.AgentType(individual_name=[eml.IndividualName("John", "Doe")]),
        keyword_set=eml.KeywordSet(keyword=["test keyword"]),
        intellectual_rights=eml.IntellectualRights(
            para=eml.Para(
                [
                    "This work is licensed under a ",
                    eml.Ulink(
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
eml_text = serializer.render(doc)

archive.eml_text = eml_text
archive.export(os.path.expanduser("~/Desktop/test.zip"))
