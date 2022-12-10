from pacmanio.plutof import PlutofReader
from pacmanio.dataset import Dataset
import logging
import numpy as np
import os


logging.basicConfig()
logging.getLogger("pacmanio").setLevel(logging.DEBUG)


plutof = PlutofReader()
archive = plutof.generate_dwca()
archive.export(os.path.expanduser("~/Desktop/temp/pacman.zip"))
