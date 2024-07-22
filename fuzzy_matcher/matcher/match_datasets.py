import numpy as np
import pandas as pd
from rapidfuzz.distance import Levenshtein
from rapidfuzz.process import extractOne, cdist
from rapidfuzz.fuzz import ratio


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
    return processed_df

def get_best_fuzzy_match_baseline(input_string:str, candidate_strings:list['str']) -> str:
    calculated_distances = []
    for cand in candidate_strings: 
        calculated_distances.append(
            {
                "candidate": cand,
                "distance": Levenshtein.distance(input_string, cand)
            }
        )
    
    best_candidate = min(calculated_distances, key=lambda x: x["distance"])
    return best_candidate

def get_computed_distances(queries: list['str'], candidate_strings: list['str']):
    dist_mat = cdist(queries, choices=candidate_strings, scorer=Levenshtein.distance)
    print(dist_mat)


def get_best_fuzzy_match_process(input_string:str, candidate_strings:list['str']) -> str:
    # use the built-in extraction method
    best_candidate = extractOne(
        query=input_string,
        choices=candidate_strings,
        scorer=ratio
    )
    return best_candidate


if __name__ == "__main__":
    primary_df = pd.read_csv("input_data/primary_names_dataset.csv")
    secondary_df = pd.read_csv("input_data/secondary_names_dataset.csv")
    
    preprocessed_primary_df = preprocess_dataframe(primary_df)
    preprocessed_secondary_df = preprocess_dataframe(secondary_df)

    candidate_matches = preprocessed_primary_df["full_name_processed"].to_list()
    queries = preprocessed_primary_df["full_name_processed"].to_list()
    #print(get_best_fuzzy_match("mechelle freitagg", candidate_matches))
    #get_computed_distances(queries[:1000], candidate_matches)

    print(get_best_fuzzy_match_process("mechelle freitagg", candidate_matches))

    print("Started matching of fields: ")
    matches = []
    count = 1
    for query in queries:
        match = get_best_fuzzy_match_process(query, candidate_matches)
        matches.append(match)
        print(f"processed : {count}")
        count += 1
    print(pd.DataFrame(matches))
