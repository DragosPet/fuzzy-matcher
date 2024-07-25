# Fuzzy Matcher

Generated names fuzzy matching, accounting for variability and random noise in strings.

# Approach

As the given datasets contain usernames, the need for deep semantic understanding might not be justified for the presented problem. Still, a solution flexible enough to capture a wide set of variability that might be induced at random in the strings that need to be matched.

Deterministic, regex-based rules represent a possible solution that can help in the mapping, but they are quite inflexible and require constant monitoring and adjustments to all possible edge-cases.

That is why, in the current project, the solutions are based on calculating ***distance/similarity*** measures between the input strings to be matched and the candidate ones.

Regardless of the approach, ideally the solutions implemented should be:
- executable in a local environment
- testable (to include at least some basic unit tests)
- measurable (as there is a need to scale the solution to large amounts of data, it's needed that we benchmark and compare the execution ot the fuzzy matching processes)

# Data

This project will include an input csv containing **randomized First and Last Names** from Kaggle - [link](https://www.kaggle.com/datasets/sushamnandi/customer-names-dataset).

This data will be used to apply the fuzzy matching process.

# Project Structure

This project uses `Poetry` for dependency and packaging management in Python.

Python version used for building the project is `3.12`.

Package structure : 
```bash
├───fuzzy_matcher
│   ├───generator
│   │   └───__init__.py
│   │   └───generate_data.py
│   ├───matcher
│   │   └───__init__.py
│   │   └───match_datasets.py
│   ├───pipelines
│   │   └───__init__.py
│   │   └───generate_user_pipe.py
│   │   └───match_user_data_baseline.py
│   │   └───match_user_data_batch.py
│   │   └───match_user_data_matrix.py
├───input_data
│   └───Customer_Names.csv
├───output_data
└───tests
    ├───unit
```

## Modules

### generator

The generator module contains code that will build large datasets from the input customer names csv file. Generated files will be used further to run and benchmark the fuzzy matching algorithms.

Code from this module is further embedded in the `pipelines` module and it generates 2 datasets:
- a **primary**, containing an expanded list of First and Last Names, by calculating all possible combinations of First/Last Name from the input file.
- a **secondary**, containing all elements from the primary and a set of variation/noise that include : duplicates, random upper casing, random character insertion, random character duplication, random swaps between first and last name.

The **secondary** dataframe will have to be matched agains the **primary**, which will be considered the point-of-thruth (however, the matching can be done in both directions).

The assumption in the generation pipeline is that most of the Names will be correctly specified, while some of them will be subject to noise (a percentage that can be controlled by the person executing the generation pipeline).

### matcher

This module contains the fuzzy matching code necessary for performing fuzzy matching between the 2 datasets.

### pipelines

Module containing scripts for generating the test dataframes, performing fuzzy matching in **3 approaches** and saving the matching results to separate files into the output_data folder.

Each pipeline is configured to run by default on a sample of the data to be matched, will benchmark the execution time of the methods and will then standardize the outputs and save them to csv files.

# Matching method

Even before applying any fuzzy matching algorithm, we reduce the search space to the concatenation of the First Name and Last Name, in order to execute the matching on a single column - the full name.

After we do this concatenation, we apply, as **preprocessing**, the following transformations : 
- remove duplicates
- trim all whitespaces from beginning/end of strings
- lowercase

Then, in the assumption that we are going to have entries that are not subject to noise, to reduce the fuzzy matching input, we perform a direct mapping based on the full name. These records will be considered as matched from a direct_mapping in the final dataset.

What remains unmapped from this first try, will pass through **fuzzy matching**.

For doing fuzzy matching, the `baseline` approach that was considered is calculating a `normalized similarity score` between the strings to be matched and the matching choices.

More specifically, a `normalized Indel similarity ratio` was selected (this is a variation of the edit/Levensthein distance that gives a higher weight to insertion and deletes), sorting also the strings, to account for any swaps in the first and last names.

An implementation for this metric is available in the `rapidfuzz` package and it contains already some optimizations for the execution of this calculation.
Still, it is a costly, cpu-intense operation, as it will have to compare the input strings with all candidates sequentially and then fetch the best option (the one with the highest similarity).

For optimizing this, there are a couple of possible directions: 
- run the comparisons for all the input strings in parallel (and if the code is executed in a distributed computing engine, like Spark, it can achieve a high parallelization factor).
- try to reduce the search space by including some deterministic rules to filter out some records.
- try to calculate part of these matchings in a batch fashion, for more than one input string at a time.

In this package, we explored a mix of the first and last optimization options (the `batch approach`).
We can calculate a batch of the similarities instead of each one individually and organize them as a matrix and then determine the best/max match on each line for the inputs.

This will be a memory-intensive operation and, depending on the search space, the batch size should be tweaked, to avoid any memory allocation errors. The matrix calculations can be done in parallel, improving further the execution time. This solution can be further optimized by reducing the search space and this would ease also the memory requirements of the code.

The idea of matrix operations can be further leveraged (`matrix` solution) in a third approach, where we can try to calculate word embeddings for the whole input and search spaces and then run some distance metrics on top of these.

The word embeddings calculation, although a memory-intensive operation, can be done in a vectorized fashion. However, the distance metrics would involve running massive matrix multiplication operations, so, in cases when memory can't be scaled up, a neirest-neighbors algorithm might reduce from the memory pressure of the algorithm.

This approach was also tested and packaged in the following repo - [tfidf_matcher](https://github.com/louistsiattalou/tfidf_matcher). We are going to include this package directly in our own, to avoid implementing it again, and use it as it is. In this package, a **tf-idf** vectorizer is used for the embeddings generation and then **KNN** is used for searching their space based on a **cosine-similarity** metric.

Also, some embedders can calculate the word-embeddings by using gpu, so there would be a huge increase in performance, but with the cost of running the process in gpu-enabled infrastructure.

# Benchmarks of the methods

Execution times for the 3 methods explored in the package (ran on a sample of **10.000** inputs) can be found in the following table:

| Method              | Pipeline                              | Execution time(seconds) |
| -----------         | -----------                           | -----------             |
| Baseline            | pipelines/match_user_data_baseline.py | 3562.16                 |
| Batch               | pipelines/match_user_data_batch.py    | **77.8**                |
| Matrix(TF-IDF+KNN)  | pipelines/match_user_data_matrix.py   | 190.5                   |



# Running the pipelines

With `python 3.12` and `poetry` installed and configured, it's enough to run the following command to create a virtual env and install the package : 

```bash
poetry install
```

To test that everything is ok, the unit tests can be ran:

```bash
poetry run pytest -v
```

## Generating Test Datasets

```bash
poetry run python fuzzy_matcher/pipelines/generate_user_pipe/py
```

## Running baseline method

```bash
poetry run python fuzzy_matcher/pipelines/match_user_data_baseline.py
```

## Running batch method

```bash
poetry run python fuzzy_matcher/pipelines/match_user_data_batch.py
```

## Running matrix method

```bash
poetry run python fuzzy_matcher/pipelines/match_user_data_matrix.py
```

# Possible improvement and research directions

To improve the fuzzy matching process further and expand it on some more complicated usecases, the embeddings approach might be explored in depth, especially if at some point, as part of the variations, there might be some abbreviations/nicknames/synonyms.

Also, depending on the usecases for the fuzzy matching needs, it might be necessary to store the embeddings in a vector-database, to serve them further.