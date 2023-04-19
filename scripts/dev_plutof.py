from pacmanio.plutof import PlutofReader
import logging


logging.basicConfig()
logging.getLogger("pacmanio").setLevel(logging.DEBUG)


plutof_reader = PlutofReader()

# token = plutof.get_token()
# print(token)

#project = plutof.get_project()
#samples = plutof.get_samples()
#events = plutof.get_events()
#specimens = plutof.get_specimens_for_samples(samples)
#print(json.dumps(specimens, indent=2))

#taxon_id = plutof.find_taxon_id("Mollusca")
#plutof.create_specimen("PAC_98", 83929, taxon_id)

#plutof.create_measurement("Temperature of the water body (°C)", "Temperature of the water body (°C)", "float")

#files = plutof.get_files(material_sample=83929)
#files = plutof.get_files_for_samples(samples)
#print(files)


#events = plutof.get_events()
#print(events)

specimens = plutof_reader.get_specimens(material_sample=83929)
plutof_reader.resolve(specimens, ["samplingevent", "related_materialsample"])
print(specimens)
