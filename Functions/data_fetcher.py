import requests
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path
from yaml import safe_load
import sys
from datetime import datetime
#from Functions.logger import get_logger

# Initialize logger
#logger = get_logger()

# Dynamically add the project root to Python's module search path
project_root = Path(__file__).resolve().parents[1]  # Go two levels up to the root
# Dynamically determine project root
#current_dir = Path(__file__).resolve().parent
#project_root = current_dir.parent

sys.path.insert(0, str(project_root))


def fetch_and_save_api_data() -> bool:
    """
    Fetches data from the OECD API (Dataflow), parses XML, and saves it to an Excel file.
    Returns:
        bool: True if successful, False otherwise.
    """
    #logger.info("Starting fetch_and_save_api_data process...")
    try:
        # Load configuration file
        config_path = project_root / "config.yaml"
        if not config_path.exists():
            #logger.error(f"Configuration file not found at: {config_path}")
            return False

        #logger.info(f"Loading configuration file: {config_path}")
        with open(config_path, "r") as f:
            config = safe_load(f)

        # Define output file path
        output_file = project_root / config["PATHS"]["NEW_FILE"]
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Archive the previous file if it exists
        if output_file.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_folder = project_root / config["PATHS"]["ARCHIVE_FOLDER"]
            archive_folder.mkdir(parents=True, exist_ok=True)  # Ensure archive folder exists
            archived_file = archive_folder / f"all_dataflows_new_{timestamp}.xlsx"
            
            # Move the existing file to the archive folder with a timestamp
            output_file.rename(archived_file)
            #logger.info(f"Archived previous file to {archived_file}")
        

        # Fetch dataflow API
        dataflow_api = config["API"]["DATAFLOW_INFO"]
        #logger.info(f"Fetching data from API: {dataflow_api}")
        response = requests.get(dataflow_api)

        if response.status_code == 200:
            try:
                # Parse XML data
                #logger.info("Parsing XML content...")
                root = ET.fromstring(response.content)
                ns = {
                    "structure": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
                    "message": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
                    "common": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
                    "xml": "http://www.w3.org/XML/1998/namespace"
                }

                # Find all Dataflow elements
                dataflows = root.findall("message:Structures/structure:Dataflows/structure:Dataflow", ns)

                # Extract dataflow information
                data = []
                for dataflow in dataflows:
                    dataflow_id = dataflow.attrib.get("id")
                    agency_id = dataflow.attrib.get("agencyID")
                    version = dataflow.attrib.get("version")
                    is_final = dataflow.attrib.get("isFinal")
                    name_elem = dataflow.find("common:Name[@xml:lang='en']", ns)
                    name_en = name_elem.text if name_elem is not None else None

                    structure_elem = dataflow.find("structure:Structure", ns)
                    ref_id = None
                    if structure_elem is not None:
                        ref_elem = structure_elem.find(".//Ref")
                        if ref_elem is not None:
                            ref_id = ref_elem.attrib.get("id")

                    data.append({
                        "Dataflow ID": dataflow_id,
                        "Agency ID": agency_id,
                        "Version": version,
                        "Is Final": is_final,
                        "Name (en)": name_en,
                        "Ref ID": ref_id
                    })

                # Convert to DataFrame
                #logger.info("Converting data to DataFrame...")
                df = pd.DataFrame(data)

                # Save DataFrame to Excel
                #logger.info(f"Saving data to Excel file: {output_file}")
                df.to_excel(output_file, index=False)
                #logger.info("Data successfully saved.")

            except ET.ParseError as e:
                #logger.error(f"Error parsing XML content: {e}")
                return False
        else:
            #logger.error(f"Failed to fetch data from {dataflow_api}. Status Code: {response.status_code}")
            return False

        #logger.info("fetch_and_save_api_data process completed successfully.")
        return True

    except Exception as e:
        #logger.error(f"An error occurred: {e}")
        return False


if __name__ == "__main__":
    # Fallback if no file is provided (like old code)
    output_file = sys.argv[1] if len(sys.argv) > 1 else "all_dataflows_previous_1.xlsx"

    # Overwrite config path dynamically if fallback is used
    config_path = project_root / "config.yaml"
    if config_path.exists():
        with open(config_path, "r") as f:
            config = safe_load(f)
        config["PATHS"]["NEW_FILE"] = output_file

    fetch_and_save_api_data()
