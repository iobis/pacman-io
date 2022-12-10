import pyworms
import logging


logger = logging.getLogger("pacmanio")


def match_names(names):
    logger.debug(f"Matching {len(names)} names")
    all_matches = [pyworms.aphiaRecordsByMatchNames(name) for name in names]
    assert (len(all_matches) == len(names))

    def select_match(matches):
        matches = matches[0]
        good_matches = [match for match in matches if match["match_type"] == "exact" or match["match_type"] == "exact_subgenus"]
        if len(good_matches) > 0:
            return "urn:lsid:marinespecies.org:taxname:" + str(good_matches[0]["AphiaID"])

    matched_ids = list(map(select_match, all_matches))
    return matched_ids
