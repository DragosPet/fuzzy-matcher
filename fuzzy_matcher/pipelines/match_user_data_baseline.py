import time
import pandas as pd
import asyncio
from fuzzy_matcher.matcher.match_datasets import process_best_fuzzy_match_baseline, preprocess_dataframe, get_best_fuzzy_match_process, process_best_fuzzy_match_baseline_parallel

# pipeline config

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

    # print(preprocessed_secondary_df.groupby(["full_name_processed"]).count())
    # print(preprocessed_primary_df.groupby(["full_name_processed"]).count())
    # # 
    # print(preprocessed_primary_df.info())
    # print(preprocessed_secondary_df.info())

    candidate_matches = preprocessed_primary_df["full_name_processed"].unique()

    # first, match elements based directly on join
    direct_mapping_df = pd.merge(preprocessed_secondary_df, preprocessed_primary_df, how="left", on="full_name_processed")
    

    # what remains unmapped, will go through fuzzy matching 
    unmapped_df = direct_mapping_df[direct_mapping_df["first_name_y"].isna()]
    print(unmapped_df)
    print(f"Records remaining to be mapped: {unmapped_df.shape[0]}")

    queries = unmapped_df["full_name_processed"].unique()

    # matches_df = process_best_fuzzy_match_baseline(unmapped_df, candidates=candidate_matches)
    # print(matches_df)

    count_l = []


    match_list = []
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        process_best_fuzzy_match_baseline_parallel(
            queries=queries,
            candidates=candidate_matches,
            workers=8,
            matches=match_list,
            count_l=count_l
        )
    )
    loop.run_until_complete(future)

    toc = time.perf_counter()

    elapsed = round(toc-tic,2)

    print(f"Elapsed mapping time: {elapsed}")




    


