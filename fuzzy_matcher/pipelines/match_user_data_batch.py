import time
import pandas as pd
from itertools import chain
import asyncio
from fuzzy_matcher.matcher.match_datasets import preprocess_dataframe, process_best_fuzzy_match_batch

# pipeline config
BATCH_SIZE = 1000
SAMPLED_RUN = True

if __name__ == "__main__":
    # import generated test data
    primary_df = pd.read_csv("input_data/primary_names_dataset.csv")
    secondary_df = pd.read_csv("input_data/secondary_names_dataset.csv")
    
    # timing the mapping process
    tic = time.perf_counter()

    # preprocess imported test data
    preprocessed_primary_df = preprocess_dataframe(primary_df)
    preprocessed_secondary_df = preprocess_dataframe(secondary_df)

    
    preprocessed_secondary_df["full_name_processed"] = preprocessed_secondary_df["full_name_processed"]
    preprocessed_primary_df["full_name_processed"] = preprocessed_primary_df["full_name_processed"]

    candidate_matches = preprocessed_primary_df["full_name_processed"].unique()

    # first, match elements based directly on join
    direct_mapping_df = pd.merge(preprocessed_secondary_df, preprocessed_primary_df, how="left", on="full_name_processed")
    

    # what remains unmapped, will go through fuzzy matching 
    unmapped_df = direct_mapping_df[direct_mapping_df["first_name_y"].isna()]
    print(f"Records remaining to be mapped: {unmapped_df.shape[0]}")

    if SAMPLED_RUN:
        queries = unmapped_df["full_name_processed"].unique()[:10000]
    else : 
        queries = unmapped_df["full_name_processed"].unique()

    match_results = []
    for b_r in range(int(len(queries) / BATCH_SIZE) + (len(queries) % BATCH_SIZE > 0)):
        batch_start = b_r * BATCH_SIZE
        batch_end = (b_r + 1) * BATCH_SIZE
        print(f"Evaluating range : {batch_start} - {batch_end}")
        batch_res = process_best_fuzzy_match_batch(
            queries=queries[batch_start:batch_end],
            candidate_strings=candidate_matches
        )
        match_results = list(chain(match_results, batch_res))
    
    toc = time.perf_counter()

    elapsed = round(toc-tic, 2)

    print(f"Elapsed matching time: {elapsed}")

    print("Post-processing matching results !")

    fuzzy_matched_df = pd.DataFrame(match_results)

    final_data = []
    for index, row in direct_mapping_df.iterrows():
        if row["first_name_y"] : 
            final_data.append(
                {
                    "full_name_normalized": row["full_name_processed"],
                    "first_name": row["first_name_y"],
                    "last_name": row["last_name_y"],
                    "similarity_score": 100.0,
                    "mapping_source": "direct_join"
                }
            ) 
        else :
            fuzzy_line_df = fuzzy_matched_df[["string_to_match"] == row["full_name_processed"]]
            if fuzzy_line_df.shape[0] > 0 : 
                first_name = preprocessed_primary_df[preprocessed_primary_df["full_name_processed"] == row["full_name_processed"]]["first_name"][0]
                last_name = preprocessed_primary_df[preprocessed_primary_df["full_name_processed"] == row["full_name_processed"]]["last_name"][0]
                final_data.append(
                    {
                        "full_name_normalized": row["full_name_processed"],
                        "first_name": first_name,
                        "last_name": last_name,
                        "similarity_score": fuzzy_line_df["similarity_score"][0],
                        "mapping_source": "fuzzy_matching"
                    })
            else:
                final_data.append(
                    {
                        "full_name_normalized": row["full_name_processed"],
                        "first_name": "not_matched",
                        "last_name": "not_matched",
                        "similarity_score": 0.0,
                        "mapping_source": "not_matched"
                    })
    final_df = pd.DataFrame(final_data)
    print(final_df)

    print(final_df.groupby(["mapping_source"]).count())
        



