import asyncio
import numpy as np
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing.pool import ThreadPool
from rapidfuzz.distance import Levenshtein
from rapidfuzz.process import extractOne, cdist
from rapidfuzz.fuzz import ratio

# set logging basic config
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
    level=logging.INFO,
)

def preprocess_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    """Given names dataframe, apply preprocessing steps to assure basic matching."""
    processed_df = input_df.copy()
    # apply lower + strip any possible whitespaces from the beginning/end of the names
    # we reduce the search space to a single dimension, for simplicity
    processed_df["full_name_processed"] = (
        processed_df["first_name"].astype(str).apply(str.lower).apply(str.strip)
        +
        " "
        +
        processed_df["last_name"].astype(str).apply(str.lower).apply(str.strip)
    )
    return processed_df.drop_duplicates()

def get_best_fuzzy_match_baseline(input_string:str, candidate_strings:list['str']) -> str:
    "Given input query and candidates, calculate best match by comparing input query with all others."
    calculated_distances = []
    for cand in candidate_strings: 
        calculated_distances.append(
            {
                "candidate": cand,
                "similarity": Levenshtein.normalized_similarity(input_string, cand)
            }
        )
    
    best_candidate = max(calculated_distances, key=lambda x: x["similarity"])
    return best_candidate


def get_best_fuzzy_match_process(input_string:str, candidate_strings:list['str']) -> str:
    # use the built-in extraction method
    best_candidate = extractOne(
        query=input_string,
        choices=candidate_strings,
        scorer=ratio
    )
    original_val = input_string
    return (original_val, best_candidate)

def process_best_fuzzy_match_baseline(queries:list['str'], candidates:list["str"]) -> pd.DataFrame:
    count = 0
    matches = []
    for query in queries : 
        match = get_best_fuzzy_match_baseline(query, candidates)
        matches.append(match)
        count +=1
        print(count)
        if count % 10000 == 0:
            print(f"processed {count} records !")
    return pd.DataFrame(matches)

def process_best_fuzzy_match_baseline_parallel(queries:list["str"], candidates:list['str'], workers:int, matches:list['dict']) -> pd.DataFrame:
    print(f"Running on {workers} threads !")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        tasks = {
            executor.submit(get_best_fuzzy_match_process, query, candidates) : query for query in queries
        }
        for task in as_completed(tasks):
            result = tasks[task]
            try:
                matches.append(task.result()    )
            except Exception as exc : 
                print(f"Encountered exception : {exc} for {result}")


        # tasks = [
        #     loop.run_in_executor(
        #         executor,
        #         get_best_fuzzy_match_process,
        #         query, 
        #         candidates
        #     )
        #     for query in queries
        # ]
        # for result in await asyncio.gather(*tasks):
        #     matches.append(result)

def process_best_fuzzy_match_batch(queries: list['str'], candidate_strings: list['str']) -> list['dict']:
    results = []
    if len(queries) > 2500 : 
        logging.error("Provided batch too large, skipping evaluation !")
        return results
    
    dist_mat = cdist(queries, choices=candidate_strings, scorer=ratio, score_cutoff=70, workers=-1)
    for i in range(len(queries)) :
        bm_val = dist_mat[i].max()
        bm_index = dist_mat[i].argmax()
        bm = candidate_strings[bm_index]
        results.append({
            "string_to_match": queries[i],
            "similarity_score": bm_val,
            "matched_string":bm
        })
    return results
    


if __name__ == "__main__":
    primary_df = pd.read_csv("input_data/primary_names_dataset.csv")
    secondary_df = pd.read_csv("input_data/secondary_names_dataset.csv")
    
    preprocessed_primary_df = preprocess_dataframe(primary_df)
    preprocessed_secondary_df = preprocess_dataframe(secondary_df)

    candidate_matches = preprocessed_primary_df["full_name_processed"].unique()
    print(len(candidate_matches))
    queries = preprocessed_primary_df["full_name_processed"].unique()
    print(len(queries))
    #print(get_best_fuzzy_match("mechelle freitagg", candidate_matches))

    print(get_best_fuzzy_match_process("mechelle freitagg", candidate_matches))

    direct_mapping_df = pd.merge(preprocessed_secondary_df, preprocessed_primary_df, how="left", on="full_name_processed")
    

    # what remains unmapped, will go through fuzzy matching 
    unmapped_df = direct_mapping_df[direct_mapping_df["first_name_y"].isna()].reset_index()
    queries = list(unmapped_df["full_name_processed"].unique())
    print(pd.DataFrame(get_computed_distances(queries[:2000], candidate_matches)))
    #print(queries)
    #print(type(queries))

    #matches = process_best_fuzzy_match_baseline(sorted(queries), sorted(candidate_matches))

    # print("Started matching of fields: ")
    # matches = []
    # count = 1
    # for query in queries:
    #     match = get_best_fuzzy_match_process(query, candidate_matches)
    #     matches.append(match)
    #     print(f"processed : {count}")
    #     count += 1
    # print(pd.DataFrame(matches))
