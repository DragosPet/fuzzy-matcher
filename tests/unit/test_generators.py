import pandas as pd
from fuzzy_matcher.generator.generate_data import read_data, expand_names_dataframe


def test_read_data_invalid_path():
    test_df = read_data("test_path_non_existent/test_file.csv")
    assert(test_df.shape[0] == 0)

def test_read_data_valid_path():
    test_df = read_data("input_data/Customer_Names.csv")
    assert(test_df.shape[0] > 0)

def test_expand_names_dataframe():
    test_df = pd.DataFrame({
        "first_name": ["name1", "name2"],
        "last_name": ["nameX", "nameY"]
    })

    expanded_df = expand_names_dataframe(test_df, 10)

    assert(expanded_df.shape[0] == 4)