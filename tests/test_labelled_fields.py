import pandas as pd
import pytest

from phillydb.labelled_fields import label_city_owned_properties


def test_label_city_owned_properties():
    with pytest.raises(ValueError):
        label_city_owned_properties(pd.DataFrame())
