import time
import pandas as pd
from fuzzy_matcher.matcher.match_datasets import (
    process_best_fuzzy_match_baseline,
    preprocess_dataframe,
)

# pipeline config
SAMPLED_RUN = True
SAMPLED_RUN_SIZE = 10000
EXPORT_PATH = "output_data/name_matching_baseline.csv"

if __name__ == "__main__":
    # import generated test data
    primary_df = pd.read_csv("input_data/primary_names_dataset.csv")
    secondary_df = pd.read_csv("input_data/secondary_names_dataset.csv")

    # timing the mapping process
    tic = time.perf_counter()

    # preprocess imported test data
    preprocessed_primary_df = preprocess_dataframe(primary_df)
    preprocessed_secondary_df = preprocess_dataframe(secondary_df)

    preprocessed_secondary_df["full_name_processed"] = preprocessed_secondary_df[
        "full_name_processed"
    ]
    preprocessed_primary_df["full_name_processed"] = preprocessed_primary_df[
        "full_name_processed"
    ]

    candidate_matches = preprocessed_primary_df["full_name_processed"].unique()

    # first, match elements based directly on join
    direct_mapping_df = pd.merge(
        preprocessed_secondary_df,
        preprocessed_primary_df,
        how="left",
        on="full_name_processed",
    )

    # what remains unmapped, will go through fuzzy matching
    unmapped_df = direct_mapping_df[direct_mapping_df["first_name_y"].isna()]
    print(unmapped_df)
    print(f"Records remaining to be mapped: {unmapped_df.shape[0]}")

    if SAMPLED_RUN:
        print(f"Running matching only on a specific sample size : {SAMPLED_RUN_SIZE}")
        queries = unmapped_df["full_name_processed"].unique()[:SAMPLED_RUN_SIZE]
    else:
        queries = unmapped_df["full_name_processed"].unique()

    fuzzy_matched_df = process_best_fuzzy_match_baseline(
        queries=queries, candidates=candidate_matches
    )
    fuzzy_matched_df.columns = [
        "search_name_normalized",
        "match_name_normalized",
        "similarity_score",
        "match_index",
    ]

    toc = time.perf_counter()

    elapsed = round(toc - tic, 2)
    print(f"Elapsed mapping time: {elapsed}")

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
        fuzzy_matched_df[
            ["search_name_normalized", "match_name_normalized", "similarity_score"]
        ]
        .copy()
        .reset_index(drop=True)
    )
    fm_final["mapping_source"] = "fuzzy_matching_baseline"
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
