import os
import sys
from pathlib import Path

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
project_root = current_dir.parent  
sys.path.insert(0, str(project_root))  # Add project root to the module search path

# Initialize logger
logger = get_logger()

def main() -> None:
    logger.info("Starting main job orchestration...")

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

    
    # Final check for errors
    if error_occurred:
        logger.error("Job orchestration failed due to earlier errors.")
        sys.exit(1)  # Exit with failure status
    else:
        logger.info("Job execution complete.")

if __name__ == "__main__":
    main()
