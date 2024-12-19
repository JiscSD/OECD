import pandas as pd
from pathlib import Path
from yaml import safe_load
import os
from datetime import datetime
from Functions.logger import get_logger

logger = get_logger()

def archive_file(file_path, archive_folder, prefix):
    """
    Archives the given file by moving it to the archive folder with a timestamp.
    Args:
        file_path (Path): The file to be archived.
        archive_folder (Path): The folder where the file should be archived.
        prefix (str): Prefix for the archived file name.
    """
    if file_path.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_folder.mkdir(parents=True, exist_ok=True)
        archived_file = archive_folder / f"{prefix}_{timestamp}{file_path.suffix}"
        file_path.rename(archived_file)
        logger.info(f"Archived {file_path.name} to {archived_file}")

def identify_changes(old_file, new_file, result_file):
    """
    Compares two Excel files (old and new) to identify changes:
    - New Inserts
    - Deleted Records
    """
    try:
        # Load old and new Excel files
        logger.info(f"Loading old data file: {old_file}")
        old_df = pd.read_excel(old_file)
        logger.info(f"Loading new data file: {new_file}")
        new_df = pd.read_excel(new_file)

        # Check for column mismatch
        if not all(old_df.columns == new_df.columns):
            logger.error("Column mismatch detected! Ensure both files have the same structure.")
            return

        # Reset index for proper comparison
        old_df.reset_index(drop=True, inplace=True)
        new_df.reset_index(drop=True, inplace=True)

        # Merge old and new DataFrames
        logger.info("Merging old and new data for comparison...")
        combined_df = old_df.merge(new_df, how="outer", indicator=True, suffixes=("_old", "_new"))

        # Initialize Change_Type column
        combined_df["Change_Type"] = None

        # Identify Deleted Records
        combined_df.loc[combined_df["_merge"] == "left_only", "Change_Type"] = "Deleted"

        # Identify New Inserts
        combined_df.loc[combined_df["_merge"] == "right_only", "Change_Type"] = "New Insert"

        # Identify Updated Records (version or is_final changes)
        logger.info("Identifying updated records...")
        for column in ["version", "is_final"]:
            column_old = f"{column}_old"
            column_new = f"{column}_new"

            if column_old in combined_df.columns and column_new in combined_df.columns:
                updated_mask = (
                    (combined_df["_merge"] == "both") &
                    (combined_df[column_old] != combined_df[column_new])
                )
                combined_df.loc[updated_mask, "Change_Type"] = "Updated"

        # Remove rows with no changes
        logger.info("Filtering rows with no changes...")
        combined_df = combined_df[combined_df["Change_Type"].notna()]

        # Drop the '_merge' column
        if "_merge" in combined_df.columns:
            combined_df.drop(columns=["_merge"], inplace=True)

        # Sort the changes by specific columns (1, 2, 5, 6)
        logger.info("Sorting records by columns...")
        sort_columns = combined_df.columns[[0, 1, 4, 5]].tolist()
        combined_df.sort_values(by=sort_columns, inplace=True)

        # Archive the existing result file before saving the new one
        archive_folder = result_file.parent / "archive"
        archive_file(result_file, archive_folder, "data_changes")

        # Save the results to the result file
        if not combined_df.empty:
            combined_df.to_excel(result_file, index=False)
            logger.info(f"Changes saved successfully to {result_file}")
        else:
            logger.info("No changes detected. No result file created.")

    except Exception as e:
        logger.error(f"Error during comparison: {e}")

def fetch_and_identify_changes():
    """
    Fetches new data using the fetcher script and compares it with the existing data.
    Updates the old file with the new one if successful.
    """
    try:
        # Dynamically determine project root
        current_dir = Path(__file__).resolve().parent
        project_root = current_dir.parent

        # Resolve the path to config.yaml
        config_path = project_root / "config.yaml"

        # Load configurations
        with open(config_path, "r") as f:
            config = safe_load(f)
        
        old_file = project_root / config["PATHS"]["OLD_FILE"]
        new_file = project_root / config["PATHS"]["NEW_FILE"]
        result_file = project_root / config["PATHS"]["RESULT_FILE"]

        # Dynamically resolve the path to data_fetcher.py
        fetch_and_save_path = project_root / "Functions" / "data_fetcher.py"

        # Verify data_fetcher.py exists
        if not fetch_and_save_path.exists():
            logger.error(f"data_fetcher.py not found at: {fetch_and_save_path}")
            return

        # Fetch new data
        logger.info(f"Running data_fetcher.py from: {fetch_and_save_path}")
        result = os.system(f'python "{fetch_and_save_path}"')

        if result == 0:
            logger.info("New data fetched successfully. Proceeding to compare changes...")
        else:
            logger.error("Error running data_fetcher.py. Stopping the comparison process.")
            return

        # Compare old and new data
        if old_file.exists():
            logger.info("Comparing old and new data files...")
            identify_changes(old_file, new_file, result_file)

            # Replace old file with new file after comparison
            archive_folder = project_root / config["PATHS"]["ARCHIVE_FOLDER"]
            archive_file(old_file, archive_folder, "old_data")
            new_file.rename(old_file)
            logger.info("Old file successfully updated with new data.")
        else:
            logger.info("Old file not found. Creating baseline...")
            new_file.rename(old_file)
            logger.info("Baseline file created successfully.")

    except Exception as e:
        logger.error(f"Error during fetch and identify changes: {e}")
