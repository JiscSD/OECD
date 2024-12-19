import logging
from pathlib import Path
from datetime import datetime

def get_logger():
    # Dynamically determine project root
    current_dir = Path(__file__).resolve().parent
    project_root = current_dir.parent

    # Define log folder and log file path relative to project root
    log_folder = project_root / "logs"
    log_folder.mkdir(parents=True, exist_ok=True)  # Create log folder if it doesn't exist
    log_file = log_folder / f"job_execution_{datetime.now().strftime('%Y%m%d')}.log"

    # Create a logger
    logger = logging.getLogger("project_logger")
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers
    if not logger.hasHandlers():
        # File Handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
