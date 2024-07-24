from time import perf_counter
import pandas as pd
import tfidf_matcher as tm
from fuzzy_matcher.matcher.match_datasets import preprocess_dataframe

# pipeline config
SAMPLED_RUN = True
SAMPLED_RUN_SIZE = 10000
EXPORT_PATH = "output_data/name_matching_matrix.csv"

if __name__ == "__main__":
    tic = perf_counter()
    primary_df = pd.read_csv("input_data/primary_names_dataset.csv")
    secondary_df = pd.read_csv("input_data/secondary_names_dataset.csv")

    preprocessed_primary_df = preprocess_dataframe(primary_df)
    preprocessed_secondary_df = preprocess_dataframe(secondary_df)

    # to reduce the mapping space, we will try a direct mapping first
    direct_mapping_df = pd.merge(
        preprocessed_secondary_df,
        preprocessed_primary_df,
        how="left",
        on="full_name_processed",
    )

    # what remains unmapped, will go through fuzzy matching
    unmapped_df = direct_mapping_df[
        direct_mapping_df["first_name_y"].isna()
    ].reset_index()
    print(f"Records remaining to be mapped: {unmapped_df.shape[0]}")
    queries = list(unmapped_df["full_name_processed"].unique())
    candidates = preprocessed_primary_df["full_name_processed"].unique()

    if SAMPLED_RUN:
        print(f"Running matching only on a specific sample size : {SAMPLED_RUN_SIZE}")
        queries = unmapped_df["full_name_processed"].unique()[:SAMPLED_RUN_SIZE]
    else:
        queries = unmapped_df["full_name_processed"].unique()

    fuzzy_matched_df = tm.matcher(
        original=queries,
        lookup=candidates,
        k_matches=1,  # for this experiment, we are interesting on getting the best match only
        ngram_length=3,
    )

    toc = perf_counter()
    elapsed = round(toc - tic, 2)
    print(f"Elapsed matching time: {elapsed} s")

    print("Post-processing matching results !")

    dm_final = (
        direct_mapping_df[direct_mapping_df["first_name_y"].isna() == False][
            ["full_name_processed"]
        ]
        .copy()
        .reset_index(drop=True)
    )
    dm_final["match_name_normalized"] = dm_final["full_name_processed"]
    dm_final["similarity_score"] = 100.0
    dm_final["mapping_source"] = "direct_join"
    dm_final.columns = [
        "search_name_normalized",
        "match_name_normalized",
        "similarity_score",
        "mapping_source",
    ]

    fm_final = (
        fuzzy_matched_df[["Original Name", "Lookup 1", "Lookup 1 Confidence"]]
        .copy()
        .reset_index(drop=True)
    )
    fm_final["mapping_source"] = "fuzzy_matching_matrix"
    fm_final.columns = [
        "search_name_normalized",
        "match_name_normalized",
        "similarity_score",
        "mapping_source",
    ]

    unmapped_final = (
        unmapped_df[
            ~unmapped_df["full_name_processed"].isin(
                list(fm_final["search_name_normalized"].unique())
            )
        ][["full_name_processed"]]
        .copy()
        .reset_index(drop=True)
    )
    unmapped_final["match_name_normalized"] = "unmapped"
    unmapped_final["similarity_score"] = 0.0
    unmapped_final["mapping_source"] = "unmapped"
    unmapped_final.columns = [
        "search_name_normalized",
        "match_name_normalized",
        "similarity_score",
        "mapping_source",
    ]

    final_df = pd.concat([dm_final, fm_final, unmapped_final], ignore_index=True)

    print("Grouping of mappings: ")

    print(final_df.groupby(["mapping_source"]).count())

    print(f"Exporting final Dataset to {EXPORT_PATH}")
    final_df.to_csv(EXPORT_PATH, index=False)
