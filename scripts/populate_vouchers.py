from pacmanio.plutof import PlutofReader
from pacmanio.dataset import Dataset
import logging
import numpy as np


logging.basicConfig()
logging.getLogger("pacmanio").setLevel(logging.DEBUG)


plutof = PlutofReader()
samples = plutof.get_samples()
sample_dict = {sample["name"]: sample["id"] for sample in samples}


def get_vouchers(path):
    dataset = Dataset(path)
    vouchers = dataset.template.vouchers.df.copy()
    vouchers.replace({np.nan: None}, inplace=True)
    vouchers["occurrenceRemarks"] = vouchers[
        ["Morphology", "Growth pattern", "Habitat", "Texture", "Odor", "External color", "Notes"]
    ].apply(lambda values: "; ".join([str(value) for value in values if value is not None]), axis=1)
    vouchers["scientificName"] = vouchers[
        ["Phylum", "Subphylum", "Class", "Order", "Family", "Genus", "Species"]
    ].apply(lambda values: [value for value in values if value is not None][-1], axis=1)
    return vouchers


def populate_vouchers(vouchers):

    for i in range(len(vouchers)):
        taxon_name = vouchers["scientificName"].iloc[i]
        remarks = vouchers["occurrenceRemarks"].iloc[i]
        sample_name = vouchers["Sample / plate"].iloc[i]
        sample_id = sample_dict[sample_name]
        voucher_name = vouchers["Specimen voucher ID"].iloc[i].replace(" ", "_")
        taxon_id = plutof.find_taxon_id(taxon_name)
        plutof.create_specimen(voucher_name, sample_id, taxon_id, remarks=remarks)


#vouchers = get_vouchers("~/Google Drive/My Drive/PacMAN shared folder/Data/site1_sampling2")
#populate_vouchers(vouchers)
#vouchers = get_vouchers("~/Google Drive/My Drive/PacMAN shared folder/Data/site2_sampling2")
#populate_vouchers(vouchers)
#vouchers = get_vouchers("~/Google Drive/My Drive/PacMAN shared folder/Data/site3_sampling2")
#populate_vouchers(vouchers)
#vouchers = get_vouchers("~/Google Drive/My Drive/PacMAN shared folder/Data/site4_sampling2")
#populate_vouchers(vouchers)
#vouchers = get_vouchers("~/Google Drive/My Drive/PacMAN shared folder/Data/site1_sampling3")
#populate_vouchers(vouchers)
#vouchers = get_vouchers("~/Google Drive/My Drive/PacMAN shared folder/Data/site2_sampling3")
#populate_vouchers(vouchers)
#vouchers = get_vouchers("~/Google Drive/My Drive/PacMAN shared folder/Data/site3_sampling3")
#populate_vouchers(vouchers)
#vouchers = get_vouchers("~/Google Drive/My Drive/PacMAN shared folder/Data/site4_sampling3")
#populate_vouchers(vouchers)
