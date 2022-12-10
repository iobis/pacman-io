from pacmanio.plutof import PlutofReader
import logging


logging.basicConfig()
logging.getLogger("pacmanio").setLevel(logging.DEBUG)


plutof = PlutofReader()

#project = plutof.get_project()
#samples = plutof.get_samples()
#events = plutof.get_events()
#specimens = plutof.get_specimens_for_samples(samples)
#print(json.dumps(specimens, indent=2))

taxon_id = plutof.find_taxon_id("Mollusca")
plutof.create_specimen("PAC_98", 83929, taxon_id)
