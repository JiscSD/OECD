import requests
import pandas as pd
from pathlib import Path
from yaml import safe_load
from Functions.logger import get_logger
import os

logger = get_logger()

def generate_and_save_api_data():
    """
    Fetches and saves data and metadata for 'New Insert' records from the data_changes file.
    Saves the data as CSV and metadata as XML.
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

        # Load paths from config
        data_changes_file = project_root / config["PATHS"]["DATA_CHANGES_FILE"]
        output_folder = project_root / "output"
        output_folder.mkdir(exist_ok=True)  # Create the output folder if it doesn't exist

        # Check if data_changes file exists
        if not data_changes_file.exists():
            print(f"No data changes file found: '{data_changes_file}'")
            logger.info(f"No data changes file found: '{data_changes_file}'")
            return 

        # Load data_changes Excel file
        logger.info(f"Loading data changes file: {data_changes_file}")
        df = pd.read_excel(data_changes_file)

        # Filter for 'New Insert' and Is Final = True
        new_inserts = df[(df['Change_Type'] == 'New Insert') & (df['Is Final'] == True)]

        if new_inserts.empty:
            logger.info("No records with 'New Insert' type and 'Is Final' = TRUE found.")
            return

        # Iterate through the filtered records
        for _, row in new_inserts.iterrows():
            dataflow_id = row['Dataflow ID']
            agency_id = row['Agency ID']
            version = row['Version']

            # Construct API URLs using configuration
            data_query_api = config["API"]["DATA_QUERY"].format(agency_id=agency_id, dataflow_id=dataflow_id)
            structure_query_api = config["API"]["STRUCTURE_QUERY"].format(agency_id=agency_id, dataflow_id=dataflow_id, version=version)

            # Define output file names
            data_file_name = output_folder / f"{agency_id}_{dataflow_id}_ALL.csv"
            structure_file_name = output_folder / f"{agency_id}_{dataflow_id}_metadata.xml"

            # Download and save the data query API response
            try:
                logger.info(f"Downloading data from: {data_query_api}")
                data_response = requests.get(data_query_api)
                if data_response.status_code == 200:
                    with open(data_file_name, "wb") as f:
                        f.write(data_response.content)
                    logger.info(f"Data successfully saved to: {data_file_name}")
                else:
                    logger.error(f"Failed to download data query API: {data_query_api}, Status code: {data_response.status_code}")
            except Exception as e:
                logger.error(f"Error downloading data query API: {e}")

            # Download and save the structure query API response
            try:
                logger.info(f"Downloading metadata from: {structure_query_api}")
                structure_response = requests.get(structure_query_api)
                if structure_response.status_code == 200:
                    with open(structure_file_name, "wb") as f:
                        f.write(structure_response.content)
                    logger.info(f"Metadata successfully saved to: {structure_file_name}")
                else:
                    logger.error(f"Failed to download structure query API: {structure_query_api}, Status code: {structure_response.status_code}")
            except Exception as e:
                logger.error(f"Error downloading structure query API: {e}")

            
            logger.info(f"Completed download for Dataflow ID: {dataflow_id}, Agency ID: {agency_id}, Version: {version}")
            logger.info("-" * 80)

    except Exception as e:
        logger.error(f"An error occurred while fetching and saving API data: {e}")

if __name__ == "__main__":
    logger.info("Starting API downloader...")
    generate_and_save_api_data()
    logger.info("API downloader completed.")
