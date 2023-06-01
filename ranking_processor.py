from typing import List, Union
import pandas as pd
from datetime import datetime
from constants import OK_MESSAGE,total_rows_from_source,ACTIONS_NOT_IN_RIGHT_ORDER

class RankingProcessor:
    def __init__(self, ranking_dict_df):
        self.ranking_dict_df = ranking_dict_df
        # Validate input
        if not isinstance(self.ranking_dict_df, pd.DataFrame):
            raise TypeError("ranking_dict_df must be a pandas DataFrame")
        if "c_activity" not in self.ranking_dict_df.columns or "Rank" not in self.ranking_dict_df.columns:
            raise ValueError("ranking_dict_df must have 'c_activity' and 'Rank' columns")

    def is_ordered(self, rankings):
        """
        Takes a list of lists of rankings and returns 1 if all the lists are ordered, else 0.
        """
        for r in rankings:
            if r != sorted(r):
                return 0
        return 1

    def split_on_value(self, lst: List[Union[int, float]], delimiter: Union[int, float]) -> List[List[Union[int, float]]]:
        # Type assertions
        assert isinstance(lst, list), "Input must be a list."
        assert isinstance(delimiter, (int, float)), "Delimiter value must be an integer or float."

        result = []
        sublist = [delimiter]
        for item in lst:
            if item == delimiter:
                result.append(sublist)
                sublist = [delimiter]
            else:
                sublist.append(item)
        result.append(sublist)

        sublists = [sub for sub in result if sub]

        # Remove 0 from each sub-list
        for i in range(len(sublists)):
            sublists[i] = [x for x in sublists[i] if x != 0]

        return sublists[1:]

    def lists_to_ranks(self, *input_lists ):


        # Create a dictionary to map c_activity values to ranks
        rank_dict = dict(zip(self.ranking_dict_df["c_activity"], self.ranking_dict_df["Rank"]))

        # Loop over each input list and map each value to its corresponding rank
        result = []
        for lst in input_lists:
            ranks = [rank_dict.get(item) for item in lst]
            result.append(ranks)

        return result
def ranking_proc_phase(unified_df, ranking_dict):
    unified_df['Process_Step'] = unified_df['Process_Step'].str.lower().str.strip()
    ranking_dict['Process_Step'] = ranking_dict['Process_Step'].str.lower().str.strip()

    # Check if the first value of 'Process_Step' column for each 'unique_ID' is not 'Applied'
    first_process_not_applied = unified_df.groupby('unique_ID')['Process_Step'].transform('first') != 'applied'

    # Update 'Comments' column with 'First Process not Applied' for the corresponding rows
    unified_df.loc[first_process_not_applied, 'Comments'] = 'First Process not Applied'

    # identify entries where first activity is not applied
    unified_df['id_first_activity_applied'] = 0  # Initialize the new column with 0
    unified_df.loc[
        ~first_process_not_applied, 'id_first_activity_applied'] = 1  # Set 1 for rows where first_process_not_applied is False

    # Define the mapping dictionary
    mapping_dict = {
        'Business Research': {'date': pd.to_datetime('2022-05-01'), 'format': '%b-%y'},
        'Data Analytics': {'date': pd.to_datetime('2022-10-01'), 'format': '%b-%y'},
        'Business Translation': {'date': pd.to_datetime('2023-04-01'), 'format': '%b-%y'}
    }

    def get_last_update(row):
        department = row['Department_ST']  # Get the department value for the row
        update_date = row['new_creation_time']

        if department in mapping_dict:
            if update_date > mapping_dict[department]['date']:
                return 1
        return 0

    unified_df['new_creation_time'] = pd.to_datetime(unified_df['new_creation_time'])
    unified_df['updated'] = unified_df.apply(get_last_update, axis=1)
    golden_source_df = pd.merge(unified_df, ranking_dict, on=['Department_ST', 'Process_Step', 'updated'], how='left')

    # Fill any null values in the "Process Step" column with an empty string
    golden_source_df['updated'].fillna('', inplace=True)

    # Reset the index if needed
    golden_source_df.reset_index(drop=True, inplace=True)

    # Format 'update_date' to month-year
    golden_source_df['last_update'] = golden_source_df['last_update'].dt.strftime('%B-%Y')

    # Create a helper function to check if a series is sorted
    def check_sorted(s):
        if s.is_monotonic_increasing:
            return OK_MESSAGE
        else:
            return ACTIONS_NOT_IN_RIGHT_ORDER

    # Apply the function to each group (unique_Id), and assign results to a new column 'Comments'
    golden_source_df['Comments'] = golden_source_df.groupby('unique_ID')['rank'].transform(check_sorted)

    # Create a helper function for the 'red_flag' column
    def flag_sorted(s):
        if s.is_monotonic_increasing:
            return 0
        else:
            return 1

    # Apply the function to each group (unique_Id), and assign results to a new column 'red_flag'
    golden_source_df['red_flag'] = golden_source_df.groupby('unique_ID')['rank'].transform(flag_sorted)


    return unified_df
