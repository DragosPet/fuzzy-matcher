import pandas as pd
from fuzzy_matcher.matcher.match_datasets import get_best_fuzzy_match_process, process_best_fuzzy_match_baseline, process_best_fuzzy_match_batch


def test_fuzzy_match_single_input():
    test_string = "Michael Jackson"
    candidates = ["Mike Jackson", "James Hetfield","Sting"]
    res = get_best_fuzzy_match_process(test_string, candidates)
    assert(res[1] == "Mike Jackson")

def test_fuzzy_match_baseline():
    test_strings = ["Michael Jackson", "Curtis Jackson", "Drake"]
    candidates = ["Mike Jackson", "James Hetfield","Sting", "John Lenon", "Drake", "Curtis (50Cent) Jackson"]
    mapped_df = process_best_fuzzy_match_baseline(test_strings, candidates)
    assert(mapped_df.shape[0] > 0)

def test_fuzzy_match_batch():
    test_strings = ["Michael Jackson", "Curtis Jackson", "Drake"]
    candidates = ["Mike Jackson", "James Hetfield","Sting", "John Lenon", "Drake", "Curtis (50Cent) Jackson"]
    mapped_data = process_best_fuzzy_match_batch(queries=test_strings, candidate_strings=candidates)
    assert(len(mapped_data) > 0)