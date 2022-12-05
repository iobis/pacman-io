from pacmanio.dataset import Dataset
import logging


logging.basicConfig()
logging.getLogger("pacmanio").setLevel(logging.DEBUG)


dataset = Dataset("~/Desktop/temp/miri/site_1")

#print(dataset.template.metadata.df)
#print(dataset.template.samples.df)
#print(dataset.template.vouchers.df)

dataset.generate_dwca()
