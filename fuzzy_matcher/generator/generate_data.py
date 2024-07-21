import os
import logging
import pandas as pd 
import random 
from datetime import date

# set logging basic config 
logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S',
    level=logging.INFO
    )
 
def read_data(input_file_path:str) -> pd.DataFrame: 
    """Given input path, read initial data(from csv file), containing First Name and Last Name as input columns."""
    # path validation
    if os.path.exists(input_file_path):
        user_data_input_df = pd.read_csv(input_file_path, sep=",")
    else : 
        logging.error("Provided path does not exist. Please provide a valid path for the input data. Returning empty DataFrame.")
        return pd.DataFrame()
    
    # structure validation
    if "First Name" not in user_data_input_df.columns :
        logging.error("`First Name` not in list of columns. Please provide an input dataframe with columns : [First Name, Last Name]")
        return pd.DataFrame()
    elif "Last Name" not in user_data_input_df.columns :
        logging.error("`Last Name` not in list of columns. Please provide an input dataframe with columns : [First Name, Last Name]")
        return pd.DataFrame()
    else :
        user_df = user_data_input_df[["First Name", "Last Name"]]
        # Change casing of columns
        user_df.columns = ["first_name","last_name"]
        return user_df
    
def multiply_suffixes(input_string:str, multiplication_factor:int) -> str : 
    """Given input string and a given multiplication factor, multiply final letter to create a suffix."""
    # we want to make sure we append at least one character (we at least double the last character)
    if multiplication_factor > 1 :
        end_char = str(input_string)[-1]
        suffix = end_char * (multiplication_factor - 1)
        processed_string = str(input_string) + suffix
    else : 
        logging.error("Please provide a multiplication factor greater than 1")
        processed_string = str(input_string)
    return processed_string

def multiply_random_letters(input_string:str) -> str:
    """Given input string, multiply letters which are candidate for username usage, in a random manner."""
    candidate_letters = ["a","e","o","u","i"]
    transformed_parts = []
    for char in input_string:
        if char in candidate_letters : 
            # multiply with a random int, maximum of 4 characters added
            multiply_factor = random.randint(0,3)
            if multiply_factor > 0 : 
                transformed_parts.append(char * multiply_factor)
            else :
                transformed_parts.append(char)
        else :
            transformed_parts.append(char)
    out_string = ""                            
    out_string = out_string.join(x for x in transformed_parts)
    return out_string

def expand_names_dataframe(input_df: pd.DataFrame, list_limit:int=20) -> pd.DataFrame:
    """Given Dataframe containing first_name and last_name as columns,
    increase the DataFrame size by calcualting all possible combos between first and last names (with a limit represented by the list_limit).
    """
    candidate_last_names = input_df["last_name"].unique().tolist()[0:list_limit]
    expanded_users = []
    for index, row in input_df.iterrows():
        for last_name in candidate_last_names:
            expanded_users.append(
                {
                    "first_name": row["first_name"],
                    "last_name": last_name
                }
            )
    return pd.DataFrame(expanded_users)



if __name__ == "__main__":
    test_username_data = read_data("input_data/Customer_Names.csv")
    print(test_username_data)
    print(multiply_suffixes("test",2))
    print(multiply_suffixes("test",4))

    print(multiply_random_letters("test"))

    expanded_usernames_df = expand_names_dataframe(test_username_data, 500)
    print(expanded_usernames_df)
    print(expanded_usernames_df.info())
