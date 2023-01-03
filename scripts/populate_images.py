from pacmanio.plutof import PlutofReader
import logging
import os
import glob
import re


logging.basicConfig()
logging.getLogger("pacmanio").setLevel(logging.DEBUG)
logger = logging.getLogger("pacmanio")

plutof = PlutofReader()
samples = plutof.get_samples()

for filepath in glob.iglob(os.path.expanduser("~/Desktop/temp/PHOTOS_CLEANED/**"), recursive=True):
    if os.path.isfile(filepath):
        sample_name = re.search("PHOTOS_CLEANED/(.*)/", filepath).group(1)
        selected_samples = [sample for sample in samples if sample["name"] == sample_name]
        if len(selected_samples) == 1:
            sample_id = selected_samples[0]["id"]
            filename = os.path.basename(filepath)
            logger.info(filepath)
            plutof.upload_file(filepath, filename, sample_id)
