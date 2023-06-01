import os
import pandas as pd
from datetime import timedelta, datetime
import numpy as np

from constants import total_rows_from_source,COLUMNS_TO_DROP_FROM_GOLDEN_SOURCE,OUTPUT_FILE_PATH_TEMPLATE,LOCATION_MAPPING

def shared_cleaning(initial_input_df: pd.DataFrame, key: str) -> pd.DataFrame:
    # Check input types
    assert isinstance(initial_input_df, pd.DataFrame), "initial_input_df should be a pandas DataFrame"
    assert isinstance(key, str), "key should be a string"

    # Find rows with the same activity done at the same time by the same candidate
    same_activity_df_serie = initial_input_df.groupby(key).apply(lambda x: x.duplicated(subset=['Creation time'], keep=False))
    same_activity_df = same_activity_df_serie.to_frame()
    same_activity_df.reset_index(inplace=True)
    same_activity_df.rename(columns={0: 'Activity_done_same_time_ID'}, inplace=True)

    # Merge the result in the main DataFrame
    input_df = pd.merge(initial_input_df, same_activity_df, left_index=True, right_on='level_1')
    input_df = input_df.rename(columns={key+'_x': key}).drop(columns=key+'_y')
    input_df['new_creation_time'] = input_df ['Creation time']



    # Create a new column called 'Disqualified' that value 1 when the activity is either disqualified or auto-disqualified
    def ID_is_disqualified(row):
        if row['New_Activity'] == "auto-disqualified" or row['New_Activity'] == 'disqualified':
            return 1
        return 0

    input_df['Disqualified'] = input_df.apply(lambda row: ID_is_disqualified(row), axis=1)

    # ---- Methodology to count the number of applications a candidate has done ---
    # create new column entrance = 1 when activity is apply or sourced or upload to job
    input_df['entrance'] = input_df['New_Activity'].apply(
        lambda x: 1 if x in ['applied', 'sourced', 'uploaded to job'] else 0)
    # group the dataframe by ID
    cumulative_sum = input_df.groupby(key)['entrance'].apply(lambda x: (x == 1).cumsum())
    # use the 'shift' method to shift the values of 'cumulative_sum' by 1
    shifted_cumsum = cumulative_sum.groupby(input_df[key])
    # use the 'fillna' method to replace the NaN values with 0s
    shifted_cumsum = shifted_cumsum.fillna(0)
    # assign the values of 'shifted_cumsum' to the 'nb of application' column
    input_df['Nb_of_appl_entrance'] = shifted_cumsum

    # ---- Methodology to count the number of applications a candidate has done ---
    # group the dataframe by key
    cumulative_sum = input_df.groupby(key)['Disqualified'].apply(lambda x: (x == 1).cumsum())
    # use the 'shift' method to shift the values of 'cumulative_sum' by 1
    shifted_cumsum = cumulative_sum.groupby(input_df[key]).shift(1)
    # use the 'fillna' method to replace the NaN values with 0s
    shifted_cumsum = shifted_cumsum.fillna(0)
    # assign the values of 'shifted_cumsum' to the 'nb of application' column
    input_df['Nb_of_appl_disq'] = 1 + shifted_cumsum
    input_df['nb_of_app_difference'] = input_df['Nb_of_appl_entrance'] - input_df['Nb_of_appl_disq']

    return input_df

def shared_processing(input_df: pd.DataFrame, key: str) -> pd.DataFrame:
    """
    This function takes an input dataframe and a key column as parameters.
    It performs some calculations on the input dataframe to generate new columns and returns the modified dataframe as output.

    Args:
        input_df: A pandas DataFrame containing the input data.
        key: A string representing the column name to be used as the key for grouping.

    Returns:
        A pandas DataFrame with additional columns generated by the function.

    Raises:
        TypeError: If the input dataframe is not a pandas DataFrame.
        TypeError: If the key is not a string.
        ValueError: If the key column does not exist in the input dataframe.
    """

    # Check input types
    if not isinstance(input_df, pd.DataFrame):
        raise TypeError("Input dataframe must be a pandas DataFrame.")
    if not isinstance(key, str):
        raise TypeError("Key column name must be a string.")

    # Check if key column exists in input dataframe
    if key not in input_df.columns:
        raise ValueError(f"Key column {key} does not exist in input dataframe.")

    # Add a new column to the DataFrame to indicate whether the sum of
    # nb_of_app_difference values in each group is evenly divisible by the number of values in the group

    def check_disqualification(x):
        if sum(x['nb_of_app_difference']) != 0:
            if sum(x['Nb_of_appl_disq']) % sum(x['nb_of_app_difference']) == 0:
                return 'OK'
            else:
                return 'KO'
        else:
            return 'OK'

    # Apply the function to each group and then using the results to set the 'ID_disqualified_OK' value for all rows in each group
    grouped_results = input_df.groupby(key).apply(check_disqualification)
    input_df['ID_disqualified_OK'] = input_df[key].map(grouped_results)

    # Calculate number of activities performed by each candidate (grouped by the specified key column)
    input_df['ID_Nb_Act'] = input_df.groupby([key])['New_Activity'].transform('count')

    # Calculate number of distinct activities performed by each candidate (grouped by the specified key column)
    input_df['ID_Nb_Act_Distinct'] = input_df.groupby([key])['New_Activity'].transform('nunique')

    # Calculate number of times each candidate has performed each activity (grouped by both the specified key column and the 'New_Activity' column)
    input_df['ID_Nb_Replicate_Act'] = input_df.groupby([key, 'New_Activity'])['New_Activity'].transform('count')

    # Create a new column called 'ID_last_activity' that indicates whether each row represents the last activity performed by each candidate (based on the maximum 'new_creation_time' value for each candidate)
    input_df['ID_last_activity'] = np.where(
        input_df.groupby(key)['new_creation_time'].transform('max').eq(input_df['new_creation_time']), 1, 0)

    # Create a new column called 'ID_first_activity' that indicates whether each row represents the first activity performed by each candidate (based on the minimum 'new_creation_time' value for each candidate)
    input_df['ID_first_activity'] = np.where(
        input_df.groupby(key)['new_creation_time'].transform('min').eq(input_df['new_creation_time']), 1, 0)

    return input_df



    ######---------------------- Data Cleaning and preliminary processing  -----------------------------------------#####

def final_processing(concatenated_df: pd.DataFrame) -> pd.DataFrame:
    """
    This function takes a concatenated pandas DataFrame as input and performs several processing steps to generate a modified DataFrame.
    The modified DataFrame includes additional columns generated by the function.

    Args:
        concatenated_df: A pandas DataFrame containing the concatenated data.

    Returns:
        A pandas DataFrame with additional columns generated by the function.

    Raises:
        TypeError: If the input dataframe is not a pandas DataFrame.
        ValueError: If the input dataframe is empty.
    """

    # Check input type
    if not isinstance(concatenated_df, pd.DataFrame):
        raise TypeError("Input dataframe must be a pandas DataFrame.")

    # Check if input dataframe is empty
    if concatenated_df.empty:
        raise ValueError("Input dataframe is empty.")

    total_rows_without_reffered_a_candidate = len(concatenated_df)
    # Split the 'new_Job' column by '-'
    concatenated_df[['Department', 'Job Position', 'Location', 'Specificities']] = concatenated_df['new_Job'].str.split('-', n=3, expand=True)

    # Remove leading/trailing whitespace from the 'Department', 'Job Position', 'Location', and 'Specificities' columns
    concatenated_df['Department'] = concatenated_df['Department'].str.strip()
    concatenated_df['Job Position'] = concatenated_df['Job Position'].str.strip()
    concatenated_df['Location'] = concatenated_df['Location'].str.strip()
    concatenated_df['Specificities'] = concatenated_df['Specificities'].str.strip()

    # Apply the mapping to create the 'country' column
    concatenated_df['country'] = concatenated_df['Location'].map(LOCATION_MAPPING)

    # Replace all departments linked to service team to 'Service Team'
    concatenated_df['Department_ST'] = concatenated_df['Department'].replace(
        dict.fromkeys(['IT', 'Marketing', 'Finance', 'Office Management'], 'Service Team'))


    # Keep the latest of rollup activity
    concatenated_df = concatenated_df.sort_values(by=['unique_ID', 'new_creation_time'])
    concatenated_df['Keep_last_Activity'] = concatenated_df.groupby(
        ['unique_ID', (concatenated_df['New_Activity'] != concatenated_df['New_Activity'].shift()).cumsum()])[
        'new_creation_time'].apply(lambda x: (x == x.max()).astype(int))
    concatenated_df = concatenated_df.loc[concatenated_df['Keep_last_Activity'] == 1]

    total_rows_after_keep_roll_up = len(concatenated_df)
    print(
        f"Total rows with keep last roll up  : {total_rows_after_keep_roll_up} ({total_rows_after_keep_roll_up/ total_rows_from_source * 100:.2f}%)")

    print(
        f"Total rows dropped in this step: { total_rows_without_reffered_a_candidate - total_rows_after_keep_roll_up }")

    return concatenated_df

def process_step_stage(unified_df: pd.DataFrame, process_step_df: pd.DataFrame , targets_df: pd.DataFrame ) -> pd.DataFrame:
    """
    Process step stage of data processing pipeline.

    :param unified_df: DataFrame containing data to be processed.
    :param process_step_df: DataFrame containing process step data.
    :return: Processed DataFrame.
    """

    # format the New_activity Column for further processing
    process_step_df['New_Activity'] = process_step_df['New_Activity'].str.lower().str.strip()
    total_rows_before_process_step = len(unified_df)
    # Create a dictionary mapping New_Activity values to Process_Step values
    total_rows_after_keep_roll_up = len(unified_df)
    total_rows_before_process_step = len(unified_df)
    print(f"Total rows before Process Step stage: {total_rows_before_process_step} "
          f"({total_rows_before_process_step / total_rows_from_source * 100:.2f}%)")
    print(f"Total rows dropped in this step: {total_rows_after_keep_roll_up - total_rows_before_process_step}")

    # Merge the two DataFrames on the New_Department and New_Activity columns
    # Create DataFrames for manual review based on the "ID_disqualified_OK" column

    # Drop rows where 'ID_disqualified_OK' is not 'OK'
    golden_source_df = unified_df.loc[unified_df['ID_disqualified_OK'] == 'OK']

    # Get the number of unique values in the 'unique_ID' column of dropped rows
    total_applications_dropped = len(
        unified_df[~unified_df['unique_ID'].isin(golden_source_df['unique_ID'])]['unique_ID'].unique())

    # Calculate total rows without KOs
    total_rows_without_Kos = len(golden_source_df)

    # Calculate total rows dropped in this step
    total_rows_before_process_step = len(unified_df)
    total_rows_dropped = total_rows_before_process_step - total_rows_without_Kos

    # Report the results
    print(
        f"Total rows without KOs: {total_rows_without_Kos} ({total_rows_without_Kos / total_rows_from_source * 100:.2f}%)")
    print(f"Total rows dropped in this step: {total_rows_dropped}")
    print(f"Total applications dropped at this step : {total_applications_dropped}")

    IDs_KO_for_hr_review_df = unified_df.loc[unified_df['ID_disqualified_OK'] != 'OK']
    total_rows_for_HR_review = len(IDs_KO_for_hr_review_df)
    print(
        f"Total rows for HR Manual review: {total_rows_for_HR_review} ({total_rows_for_HR_review / total_rows_from_source * 100:.2f}%)")

    golden_source_df = pd.merge(golden_source_df, process_step_df, on=['Department_ST', 'New_Activity'], how='left')

    # Fill any null values in the "Process Step" column with an empty string
    golden_source_df['Process_Step'].fillna('', inplace=True)

    total_rows_with_process_step = len(golden_source_df)
    print(
        f"Total rows with Process Step: {total_rows_with_process_step} ({total_rows_with_process_step / total_rows_from_source * 100:.2f}%)")
    print(f"Total rows dropped in this step: {total_rows_without_Kos - total_rows_with_process_step}")

    # Create two DataFrames based on the "Process Step" column
    golden_source_df = golden_source_df[golden_source_df["Process_Step"] != ""]
    golden_source_df_without_process = golden_source_df[golden_source_df["Process_Step"] == ""]


    total_rows_with_process_step_not_blank = len(golden_source_df)
    print(
        f"Total rows with Process Step not blank: {total_rows_with_process_step_not_blank} ({total_rows_with_process_step_not_blank / total_rows_from_source * 100:.2f}%)")
    print(f"Total rows dropped in this step: {total_rows_with_process_step - total_rows_with_process_step_not_blank}")
    # Keep the latest of rollup process
    golden_source_df = golden_source_df.sort_values(by=['unique_ID', 'new_creation_time'])
    golden_source_df['Keep_last_Process'] = golden_source_df.groupby(
        ['unique_ID', (golden_source_df['Process_Step'] != golden_source_df['Process_Step'].shift()).cumsum()])[
        'new_creation_time'].apply(lambda x: (x == x.max()).astype(int))
    golden_source_df = golden_source_df.loc[golden_source_df['Keep_last_Process'] == 1]

    total_rows_after_keep_roll_up = len(golden_source_df)
    print(
        f"Total rows with keep last roll up: {total_rows_after_keep_roll_up} ({total_rows_after_keep_roll_up / total_rows_from_source * 100:.2f}%)")
    print(f"Total rows dropped in this step: {total_rows_with_process_step_not_blank - total_rows_after_keep_roll_up}")
    # Create a new column called 'ID_last_Process' that indicates whether each row represents the last Process by ID
    golden_source_df['ID_last_Process'] = np.where(
        golden_source_df.groupby('unique_ID')['new_creation_time'].transform('max').eq(
            golden_source_df['new_creation_time']), 1, 0)

    IDs_KO_for_hr_review_df.to_excel('IDs_KO_for_hr_review.xlsx', index=False)
    # Add the time Diffrence between consecutive Process Steps, sort by Time
    # Sort the DataFrame by 'Candidate' and 'Creation time'
    golden_source_df = golden_source_df.sort_values(by=['unique_ID', 'new_creation_time'])
    # Reset the index
    golden_source_df = golden_source_df.reset_index(drop=True)
    # Create a new column to store the time difference in hours
    golden_source_df['time_diff_in_hours'] = (golden_source_df['new_creation_time'] -
                                              golden_source_df.groupby('unique_ID')[
                                                  'new_creation_time'].shift()).dt.total_seconds() // 3600
    golden_source_df['time_diff_in_hours'] = golden_source_df['time_diff_in_hours'].fillna(0).astype(int)

    # Create a new column to store the time difference in days as an integer
    golden_source_df['time_diff_in_days'] = (golden_source_df['new_creation_time'] -
                                             golden_source_df.groupby('unique_ID')[
                                                 'new_creation_time'].shift()).dt.total_seconds() / (24 * 3600)
    golden_source_df['time_diff_in_days'] = golden_source_df['time_diff_in_days'].fillna(0).round(2)

    # Calculate the cumulative time difference in days for each unique_ID
    golden_source_df['cummulative_time_diff_in_days'] = golden_source_df.groupby('unique_ID')[
        'time_diff_in_days'].cumsum()

    # Fill missing values in the 'Specificities' column with 'Core'
    golden_source_df['Specificities'] = golden_source_df['Specificities'].fillna('Core')

    # Define conditions for the 'autotest_subset_vanilla' column based on specified criteria for each unique_ID
    conditions = (golden_source_df['Specificities'] == '') & \
                 (golden_source_df['Department_ST'] == 'Business Research') & \
                 (golden_source_df['Job Position'].isin(['Research Analyst',
                                                         'Senior Research Analyst',
                                                         'Research Associate']))
    # Create a boolean mask based on the conditions
    id_is_vanilla_mask = conditions
    id_hr_interview_mask = ~conditions

    golden_source_df['id_is_vanilla'] = golden_source_df['unique_ID'].isin(
        golden_source_df.loc[id_is_vanilla_mask, 'unique_ID']).astype(int)

    golden_source_df['id_not_vanilla'] = golden_source_df['unique_ID'].isin(
        golden_source_df.loc[id_hr_interview_mask, 'unique_ID']).astype(int)



    # Subtract 'auto_test_times' from 'time_diff_in_days' where 'id_is_vanilla' is 1, else 0. Set to 0 if the difference is negative.
    # Assuming your DataFrame is named 'golden_source_df'
    def subtract_auto_test(row):
        auto_test_rows = golden_source_df[(golden_source_df['unique_ID'] == row['unique_ID']) & (
                    golden_source_df['Process_Step'] == 'Automated test')
                    & (golden_source_df['id_is_vanilla']  == 1)]
        if len(auto_test_rows) > 0:
            result = row['cummulative_time_diff_in_days'] - auto_test_rows['cummulative_time_diff_in_days'].iloc[0]
            return max(result, 0)  # Replace negative values with 0
        else:
            return 0

    golden_source_df['Cum_Time_diff_from_autotest'] = golden_source_df.apply(subtract_auto_test, axis=1)

    def subtract_hr_interview(row):
        auto_test_rows = golden_source_df[(golden_source_df['unique_ID'] == row['unique_ID']) & (
                golden_source_df['Process_Step'] == 'HR Interview')

                & (golden_source_df['id_is_vanilla']  == 0 )]

        if len(auto_test_rows) > 0:
            result = row['cummulative_time_diff_in_days'] - auto_test_rows['cummulative_time_diff_in_days'].iloc[0]
            return max(result, 0)  # Replace negative values with 0
        else:
            return 0

    golden_source_df['Cum_Time_diff_from_HR_Interview'] = golden_source_df.apply(subtract_hr_interview, axis=1)

    # Initialize the 'autotest_subset_vanilla' column with 0
    golden_source_df['autotest_subset_vanilla'] = 0

    # Add 'hr_interview_subset' column and set the value to 1 for rows where conditions are not met and have 'Process_Step' equal to 'Offer'
    golden_source_df['hr_interview_subset'] = 0

    # Create a boolean mask based on the conditions
    vanilla_mask = golden_source_df['Process_Step'].eq('Offer') & conditions
    hr_interview_mask = golden_source_df['Process_Step'].eq('Offer') & ~conditions

    # Set the value of 'autotest_subset_vanilla' to 1 for rows that meet the conditions and have 'Process_Step' equal to 'Offer'
    golden_source_df.loc[vanilla_mask, 'autotest_subset_vanilla'] = 1
    golden_source_df.loc[hr_interview_mask, 'hr_interview_subset'] = 1
    # Create a new column 'id_is_vanilla' which is 1 if any row of a unique_ID is in vanilla_mask, and 0 otherwise



    # Add a new column to show the previous process step for each application
    golden_source_df['previous_process_step'] = golden_source_df.groupby('unique_ID')['Process_Step'].shift(1)

    # Add a new column to show whether each application is still in pipeline
    golden_source_df['ID_in_pipeline'] = golden_source_df.groupby('unique_ID')['Process_Step'].transform(
        lambda x: int(x.iloc[-1] not in ['Out of Process', 'Hired']))

    # Add a new column to show whether each application has been hired
    golden_source_df['ID_is_hired'] = golden_source_df.groupby('unique_ID')['Process_Step'].transform(
        lambda x: int(x.iloc[-1] == 'Hired'))

    # Create a dictionary mapping 'unique_ID' to 'new_creation_time' for rows where 'Process_Step' is 'Hired'
    hiring_dates_mapping = golden_source_df.loc[golden_source_df['Process_Step'] == 'Hired'].groupby('unique_ID')[
        'new_creation_time'].first().to_dict()

    # Add the 'id_hiring_date' column to 'golden_source_df' using the mapping
    golden_source_df['id_hiring_date'] = golden_source_df['unique_ID'].map(hiring_dates_mapping)

    # Add a new column to show whether each application is out of process
    golden_source_df['ID_is_out_of_process'] = golden_source_df.groupby('unique_ID')['Process_Step'].transform(
        lambda x: int(x.iloc[-1] == 'Out of Process'))

    # Create a new column named "Stage_advancement" in the DataFrame that concatenates the "Process_Step" column with the "previous_process_step" column
    golden_source_df['Stage_advancement'] = golden_source_df['previous_process_step'].fillna('') + ' ==> ' + \
                                            golden_source_df['Process_Step']

    targets_df['Stage_advancement'] = targets_df['Stage_advancement'].str.lower().str.strip()
    golden_source_df['Stage_advancement']=golden_source_df['Stage_advancement'].str.lower().str.strip()
    golden_source_df = pd.merge(golden_source_df, targets_df, on=['Department_ST', 'Stage_advancement'], how='left')


    # Drop specified columns from the DataFrame
    try:
        golden_source_df.drop(COLUMNS_TO_DROP_FROM_GOLDEN_SOURCE, axis=1, inplace=True)

    except KeyError as e:
        # Handle KeyError if any of the specified columns are not present in the DataFrame
        print(f"Error: {e} column(s) not found in DataFrame.")

    except Exception as e:
        # Handle any other exceptions that might occur
        print(f"Error: {e} occurred.")

    finally:
        # Write the updated DataFrame to excel
        # Create a timestamp using the current date
        now = datetime.now()
        timestamp = now.strftime("%d-%m")
        file_name = OUTPUT_FILE_PATH_TEMPLATE.format(timestamp)
        # Save the unified DataFrame to an Excel file
        golden_source_df.to_excel(file_name, index=False)

    return golden_source_df






