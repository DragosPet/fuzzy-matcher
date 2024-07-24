from fuzzy_matcher.generator.generate_data import (
    read_data,
    expand_names_dataframe,
    noisify_dataframe,
    export_data,
)
import time

# Pipe Config
BASE_INPUT_FILE_PATH = "input_data/Customer_Names.csv"
CROSSING_LIMIT = 500  # this will generate 1.000.000 records in the names Dataframe
NOISIFICATION_SAMPLE = 0.4  # this will dictate the percentage of the primary DataFrame that will contain noise
GENERATION_TARGET_PATH = "input_data/"

if __name__ == "__main__":
    # first, read the original file data
    tic = time.perf_counter()

    print("Starting Data Generation process !")

    input_names_df = read_data(BASE_INPUT_FILE_PATH)

    # generate the 2 datasets
    # primary one will be just a crossing between first names and last_names
    primary_df = expand_names_dataframe(
        input_df=input_names_df, list_limit=CROSSING_LIMIT
    )

    # secondary one will be generated from the first, containig
    secondary_df = noisify_dataframe(
        input_df=primary_df, noise_sample_size=NOISIFICATION_SAMPLE
    )

    # export the 2 datasets
    export_data(
        export_data=primary_df,
        export_path="input_data/",
        file_name="primary_names_dataset",
    )

    export_data(
        export_data=secondary_df,
        export_path="input_data/",
        file_name="secondary_names_dataset",
    )

    toc = time.perf_counter()
    elapsed = round(toc - tic, 2)
    print(f"Generation process finished. Elapsed : {elapsed} s")
