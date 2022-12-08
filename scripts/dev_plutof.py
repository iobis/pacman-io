from pacmanio.plutof import PlutofReader
import logging
import json


logging.basicConfig()
logging.getLogger("pacmanio").setLevel(logging.DEBUG)


plutof = PlutofReader()
project = plutof.get_project()
samples = plutof.get_samples()
events = plutof.get_events()

print(json.dumps(events, indent=2))
