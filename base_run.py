import os
import sys
from pathlib import Path
from yaml import safe_load

try:
    from Functions.data_fetcher import fetch_and_save_api_data
    from Functions.data_comparator import fetch_and_identify_changes
    from Functions.api_downloader import generate_and_save_api_data
    from Functions.logger import get_logger
except ImportError as e:
    print(f"Failed to import required modules: {e}")
    sys.exit(1)

# Dynamically add the project root to Python's module search path
current_dir = Path(__file__).resolve().parent  # Path to the current script
project_root = current_dir  # Set project_root to the same location as base_run.py
sys.path.insert(0, str(project_root))  # Add project root to the module search path

# Initialize logger
logger = get_logger()

def load_config() -> dict:
    """
    Load the configuration file.
    Returns:
        dict: Configuration data.
    """
    config_path = project_root / "config.yaml"  # Look for config.yaml in the same location as base_run.py
    if not config_path.exists():
        logger.error(f"Configuration file not found at: {config_path}")
        sys.exit(1)

    with open(config_path, "r") as f:
        return safe_load(f)

def rename_new_to_old(config: dict) -> None:
    """
    Renames the new file to the old file name after successful data fetching.
    Args:
        config (dict): Configuration data containing file paths.
    """
    new_file_path = project_root / config["PATHS"]["NEW_FILE"]
    old_file_path = project_root / config["PATHS"]["OLD_FILE"]

    if new_file_path.exists():
        logger.info(f"Renaming {new_file_path} to {old_file_path}")
        old_file_path.unlink(missing_ok=True)  # Remove the old file if it exists
        new_file_path.rename(old_file_path)
        logger.info(f"File renamed successfully: {new_file_path} -> {old_file_path}")
    else:
        logger.warning(f"New file not found: {new_file_path}. Skipping rename operation.")

def main() -> None:
    logger.info("Starting main job orchestration...")

    # Load configuration
    config = load_config()

    # Track if an error occurred
    error_occurred = False

    # Step 1: Fetch new API data and save the baseline file
    if not error_occurred:
        logger.info("Executing data fetching job...")
        try:
            fetch_and_save_api_data()
        except Exception as e:
            logger.error(f"Error during data fetching job: {e}", exc_info=True)
            error_occurred = True

    # Step 2: Rename the new file to old file if data fetching succeeded
    if not error_occurred:
        try:
            rename_new_to_old(config)
        except Exception as e:
            logger.error(f"Error during renaming new file to old file: {e}", exc_info=True)
            error_occurred = True

    # Final check for errors
    if error_occurred:
        logger.error("Job orchestration failed due to earlier errors.")
        sys.exit(1)  # Exit with failure status
    else:
        logger.info("Job execution complete.")

if __name__ == "__main__":
    main()
