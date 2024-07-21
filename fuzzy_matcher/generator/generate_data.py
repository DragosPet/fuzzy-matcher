import os
import logging
import pandas as pd
import random

# set logging basic config
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
    level=logging.INFO,
)


def read_data(input_file_path: str) -> pd.DataFrame:
    """Given input path, read initial data(from csv file), containing First Name and Last Name as input columns."""
    # path validation
    if os.path.exists(input_file_path):
        user_data_input_df = pd.read_csv(input_file_path, sep=",")
        logging.info("Imported data from provided path !")
    else:
        logging.error(
            "Provided path does not exist. Please provide a valid path for the input data. Returning empty DataFrame."
        )
        return pd.DataFrame()

    # structure validation
    if "First Name" not in user_data_input_df.columns:
        logging.error(
            "`First Name` not in list of columns. Please provide an input dataframe with columns : [First Name, Last Name]"
        )
        return pd.DataFrame()
    elif "Last Name" not in user_data_input_df.columns:
        logging.error(
            "`Last Name` not in list of columns. Please provide an input dataframe with columns : [First Name, Last Name]"
        )
        return pd.DataFrame()
    else:
        user_df = user_data_input_df[["First Name", "Last Name"]]
        # Change casing of columns
        user_df.columns = ["first_name", "last_name"]
        return user_df


def multiply_suffixes(input_string: str) -> str:
    """Given input string and a given multiplication factor, multiply final letter to create a suffix."""
    # we want to make sure we append at least one character (we at least double the last character)
    multiplication_factor = random.randint(2, 4)
    end_char = str(input_string)[-1]
    suffix = end_char * (multiplication_factor - 1)
    processed_string = str(input_string) + suffix
    return processed_string


def multiply_random_letters(input_string: str) -> str:
    """Given input string, multiply letters which are candidate for username usage, in a random manner."""
    candidate_letters = ["a", "e", "o", "u", "i"]
    transformed_parts = []
    for char in input_string:
        if char in candidate_letters:
            # multiply with a random int, maximum of 4 characters added
            multiply_factor = random.randint(1, 3)
            if multiply_factor > 0:
                transformed_parts.append(char * multiply_factor)
            else:
                transformed_parts.append(char)
        else:
            transformed_parts.append(char)
    out_string = ""
    out_string = out_string.join(x for x in transformed_parts)
    return out_string


def expand_names_dataframe(
    input_df: pd.DataFrame, list_limit: int = 20
) -> pd.DataFrame:
    """Given Dataframe containing first_name and last_name as columns,
    increase the DataFrame size by calcualting all possible combos between first and last names (with a limit represented by the list_limit).
    """
    logging.info(
        "Generating original Dataset by crossing first names with last names !"
    )
    candidate_last_names = input_df["last_name"].unique().tolist()[0:list_limit]
    expanded_users = []
    for index, row in input_df.iterrows():
        for last_name in candidate_last_names:
            expanded_users.append(
                {"first_name": row["first_name"], "last_name": last_name}
            )
    return pd.DataFrame(expanded_users)


def noisify_dataframe(input_df: pd.DataFrame, noise_sample_size: float) -> pd.DataFrame:
    logging.info("Adding noise to original Dataset !")
    noisification_candidates_df = input_df.sample(frac=noise_sample_size)
    nosification_candidate_functions = {
        0: str.upper,
        1: multiply_suffixes,
        2: multiply_random_letters,
        3: str.lower,
    }
    noisified_data = []
    for index, row in noisification_candidates_df.iterrows():

        d = {
            "index": index,
            "first_name": row["first_name"],
            "last_name": row["last_name"],
        }

        # Apply the noisification functions to one of the columns, randomly
        if random.randint(0, 1) == 0:
            candidate_col = "first_name"
        else:
            candidate_col = "last_name"

        # apply one of the noisification function randomly
        candidate_func_number = random.randint(0, 3)
        candidate_func = nosification_candidate_functions[candidate_func_number]

        # update data based on the random vals

        d[candidate_col] = candidate_func(d[candidate_col])

        noisified_data.append(d)

    # Combine original data and data containing noise
    nosified_df = pd.DataFrame(noisified_data)
    nosified_df = nosified_df.set_index("index")[["first_name", "last_name"]]

    # index-based merge
    merged_df = nosified_df.combine_first(input_df)
    return merged_df


def export_data(export_data: pd.DataFrame, export_path: str, file_name: str) -> None:
    """Given export file path and export data, save it to a local csv file."""
    if not os.path.exists(export_path):
        logging.error("Provided path does not exist, stopping export !")
        exit(1)
    complete_export_file_name = f"{export_path}/{file_name}.csv"
    try:
        export_data.to_csv(complete_export_file_name, index=False, sep=",")
        logging.info(f"Exported data to : {complete_export_file_name} !")
    except:
        logging.error("Exception encountered while exporting file !")


if __name__ == "__main__":
    pass
