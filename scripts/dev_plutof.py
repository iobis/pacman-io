from pacmanio.plutof import PlutofReader
import logging
import json


logging.basicConfig()
logging.getLogger("pacmanio").setLevel(logging.DEBUG)


plutof = PlutofReader()
project = plutof.get_project()
samples = plutof.get_samples()
events = plutof.get_events()
specimens = plutof.get_specimens_for_samples(samples)

print(json.dumps(specimens, indent=2))
