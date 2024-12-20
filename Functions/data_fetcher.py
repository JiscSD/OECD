import requests
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path
from yaml import safe_load
import shutil
import sys

# Dynamically add the project root to Python's module search path
project_root = Path(__file__).resolve().parents[1]  # Go two levels up to the root
sys.path.insert(0, str(project_root))

def archive_existing_file():
    """
    Archives the existing file to the archive folder before starting the process.
    """
    try:
        config_path = project_root / "config.yaml"
        if not config_path.exists():
            return False

        with open(config_path, "r") as f:
            config = safe_load(f)

        # Define paths
        output_file = project_root / config["PATHS"]["NEW_FILE"]
        archive_folder = project_root / config["PATHS"]["ARCHIVE_FOLDER"]

        if output_file.exists():
            # Ensure archive folder exists
            archive_folder.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archived_file = archive_folder / f"all_dataflows_{timestamp}.xlsx"

            # Copy the file to the archive folder
            shutil.copy2(output_file, archived_file)

            # Verify the archive file exists and matches original
            if not archived_file.exists() or archived_file.stat().st_size != output_file.stat().st_size:
                print("Error: Archived file verification failed.")
                return False

            # Ensure the original file remains in place
            return True
        else:
            return True

    except Exception as e:
        print(f"Error during archival: {e}")
        return False


def fetch_and_save_api_data() -> bool:
    """
    Fetches data from the OECD API (Dataflow), parses XML, and saves it to an Excel file.
    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        # Load configuration file
        config_path = project_root / "config.yaml"
        if not config_path.exists():
            return False

        with open(config_path, "r") as f:
            config = safe_load(f)

        # Define output file path
        output_file = project_root / config["PATHS"]["NEW_FILE"]
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Fetch dataflow API
        dataflow_api = config["API"]["DATAFLOW_INFO"]
        response = requests.get(dataflow_api)

        if response.status_code == 200:
            try:
                # Parse XML data
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
                df = pd.DataFrame(data)

                # Save DataFrame to Excel
                df.to_excel(output_file, index=False)

            except ET.ParseError:
                return False
        else:
            return False

        return True

    except Exception:
        return False

if __name__ == "__main__":
    # Archive the existing file before starting the process
    if not archive_existing_file():
        print("Error archiving existing file.")
        sys.exit(1)

    output_file = sys.argv[1] if len(sys.argv) > 1 else "all_dataflows_previous_1.xlsx"

    config_path = project_root / "config.yaml"
    if config_path.exists():
        with open(config_path, "r") as f:
            config = safe_load(f)
        config["PATHS"]["NEW_FILE"] = output_file

    fetch_and_save_api_data()
