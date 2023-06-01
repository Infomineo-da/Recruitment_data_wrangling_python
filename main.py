# Import necessary modules
from processing_toolkit import preliminary_processing,not_moved_to_job_data_processor,moved_to_job_data_processor
from ranking_processor import ranking_proc_phase
import warnings
from Toolkit import final_processing ,process_step_stage

# Import constants
from constants import *
from datetime import datetime
from helper_functions import read_file,validate_dataframe



# Disable warnings for cleaner output
warnings.filterwarnings('ignore')

if __name__ == "__main__":
    ### --------------------------- LOAD FILES and Validate input ------------------------------------###

    # Import activity_report CSV file into a Pandas dataframe
    try:
        activity_report_df = read_file(ACTIVITY_REPORT_PATH)
        # Validate if the dataframe has all the required columns , and it's not empty
        if not validate_dataframe(activity_report_df, ACTIVITY_REPORT_COLS):
            exit(1)

    except FileNotFoundError:
        print(ERROR_ACTIVITY_REPORT_NOT_FOUND)
        exit(1)

    # Import activity dictionary Excel sheet into a Pandas dataframe
    try:
        activity_dict_df = read_file(ACTIVITY_DICT_PATH)
        if not validate_dataframe(activity_dict_df, ACTIVITY_DICTIONARY_COLS):
            exit(1)
    except FileNotFoundError:
        print(ERROR_ACTIVITY_DICT_NOT_FOUND)
        exit(1)

    # Import HR name list Excel sheet into a Pandas dataframe
    try:
        hr_names_df = read_file(HR_NAMES_PATH)
        if not validate_dataframe(hr_names_df, HR_NAMES_COLS):
            exit(1)
    except FileNotFoundError:
        print(ERROR_HR_NAMES_NOT_FOUND)
        exit(1)

    # Import Process_Step Excel sheet into a Pandas dataframe
    try:
        process_step_df = read_file(PROCESS_STEP_PATH)
        if not validate_dataframe(process_step_df, PROCESS_STEP_COLS):
            exit(1)
    except FileNotFoundError:
        print(ERROR_PROCESS_STEP_NOT_FOUND)
        exit(1)

        # Import Targets Excel sheet into a Pandas dataframe
    try:
        targets_df = read_file(TARGETS_STEP_PATH)
        if not validate_dataframe(targets_df, TARGETS_COLS):
            exit(1)

    except FileNotFoundError:
        print(ERROR_TARGETS_FILE_NOT_FOUND)
        exit(1)

    # Load the ranking dictionary dataframe and convert the 'c_activity' column to lowercase
    try:
        ranking_dict_df = read_file(RANKING_DICT_PATH)
        assert isinstance(ranking_dict_df, pd.DataFrame), "ranking_dict_df must be a pandas DataFrame"

    except FileNotFoundError:
        print(ERROR_RANKING_DICT_NOT_FOUND)
        exit(1)

    #### -------------------------- Separate candidates with 'moved to job position' from the rest ------------------------- ####
    # Separate candidates with 'moved to job position' from the rest
    try:
        moved_to_job_df, not_moved_to_job_df = preliminary_processing(activity_report_df, activity_dict_df, hr_names_df)
    except Exception as e:
        print(f"{ERROR_PRELIMINARY_PROCESSING_FAILED.format(str(e))}")
        exit(1)


    #### ------------------- Process moved to job and not moved to job candidates seperately   ------------------------- ####

    try:
        # Process the two sub dataframes for candidates who moved to job position and those who did not
        not_moved_to_job_df = not_moved_to_job_data_processor(not_moved_to_job_df)
        moved_to_job_first_only_df, moved_time_activity_report_df = moved_to_job_data_processor(moved_to_job_df)
        # Concatenate the three dataframes
        golden_source_df = pd.concat([not_moved_to_job_df, moved_to_job_first_only_df, moved_time_activity_report_df])
        golden_source_df.drop('level_0', axis=1, inplace=True)
        golden_source_df.reset_index(inplace=True)
        # Further process the concatenated dataframe
        unified_df = final_processing(golden_source_df)

    except Exception as e:
        print(f"{ERROR_SUB_DATAFRAME_CREATION_FAILED.format(str(e))}")
        exit(1)

    # Process Step Phase ----------------------------------------------------------------------------------------
    try:
        # Call the process_step_stage function with the necessary parameters
        golden_source_df = process_step_stage(unified_df, process_step_df, targets_df)

    except Exception as e:
        # Handle any exceptions that occur during the execution
        print("An error occurred:", str(e))



    # Ranking processor phase -----------------------------------------------------------------------------------

    try:
        golden_source_df_with_ranking = ranking_proc_phase(golden_source_df,ranking_dict_df)

        # Create a timestamp using the current date
        now = datetime.now()
        timestamp = now.strftime("%d-%m")
        file_name = fr'.\output_data\Golden_source_with_ranking_processor-{timestamp}.xlsx'
        golden_source_df_with_ranking.to_excel(file_name, index=False)

    except Exception as e:
        print(f"{ERROR_RANKING_PROCESSOR_FAILED.format(str(e))}")
        exit(1)


