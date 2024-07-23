from time import perf_counter
import pandas as pd
import tfidf_matcher as tm
from fuzzy_matcher.matcher.match_datasets import preprocess_dataframe


if __name__ == "__main__":
    tic = perf_counter()
    primary_df = pd.read_csv("input_data/primary_names_dataset.csv")
    secondary_df = pd.read_csv("input_data/secondary_names_dataset.csv")
    
    preprocessed_primary_df = preprocess_dataframe(primary_df)
    preprocessed_secondary_df = preprocess_dataframe(secondary_df)

    direct_mapping_df = pd.merge(preprocessed_secondary_df, preprocessed_primary_df, how="left", on="full_name_processed")
    

    # what remains unmapped, will go through fuzzy matching 
    unmapped_df = direct_mapping_df[direct_mapping_df["first_name_y"].isna()].reset_index()
    queries = list(unmapped_df["full_name_processed"].unique())
    candidates = preprocessed_primary_df["full_name_processed"].unique()

    print(tm.matcher(original=queries, lookup=candidates, k_matches=1, ngram_length=3))

    toc = perf_counter()

    print(f"Elapsed: {toc - tic}")
