from scourgify import normalize_address_record
from passyunk.parser import PassyunkParser


def get_normalized_address(address):
    p = PassyunkParser()
    # pass through philly-specific passyunk library
    address_pasyunk = p.parse(address)["components"]["output_address"]
    # attempt to further normalize
    n_address_dict = {
        k: v if v else "" for k, v in normalize_address_record(address).items()
    }
    # remove multiple spaces and combine
    return " ".join(
        (
            n_address_dict["address_line_1"]
            + n_address_dict["address_line_2"]
            + n_address_dict["city"]
            + n_address_dict["state"]
            + n_address_dict["postal_code"]
        )
        .strip()
        .split()
    )
