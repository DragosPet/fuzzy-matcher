import numpy as np
import pandas as pd
import logging
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
        + " "
        + processed_df["last_name"].astype(str).apply(str.lower).apply(str.strip)
    )
    return processed_df.drop_duplicates()


def get_best_fuzzy_match_process(
    input_string: str, candidate_strings: list["str"]
) -> str:
    """Given an input string and a list of candidates, run distance/similarity comparison and get best match."""
    # use the built-in extraction method
    best_candidate = extractOne(
        query=input_string, choices=candidate_strings, scorer=ratio
    )
    original_val = input_string
    return (original_val, *best_candidate)


def process_best_fuzzy_match_baseline(
    queries: list["str"], candidates: list["str"]
) -> pd.DataFrame:
    """Given set of input queries and matching candidates, run fuzzy matching sequentially."""
    count = 0
    matches = []
    for query in queries:
        match = get_best_fuzzy_match_process(query, candidates)
        matches.append(match)
        count += 1
        if count % 1000 == 0:
            print(f"processed {count} records !")
    return pd.DataFrame(matches)


def process_best_fuzzy_match_batch(
    queries: list["str"], candidate_strings: list["str"]
) -> list["dict"]:
    """Given set of input queries and matching candidates, run fuzzy matching in batch mode."""
    results = []
    if len(queries) > 2500:
        logging.error("Provided batch too large, skipping evaluation !")
        return results

    dist_mat = cdist(
        queries, choices=candidate_strings, scorer=ratio, score_cutoff=70, workers=-1
    )
    for i in range(len(queries)):
        bm_val = dist_mat[i].max()
        bm_index = dist_mat[i].argmax()
        bm = candidate_strings[bm_index]
        results.append(
            {
                "string_to_match": queries[i],
                "similarity_score": bm_val,
                "matched_string": bm,
            }
        )
    return results


if __name__ == "__main__":
    pass
