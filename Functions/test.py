import os
import yaml

# Dynamically construct the path to config.yaml
config_path = os.path.join(os.path.dirname(__file__), "..", "config.yaml")

# Load and read the config file
def read_config():
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        print("Configuration Loaded Successfully:")
        print(config)
    except FileNotFoundError:
        print(f"Error: Config file not found at {config_path}")
    except yaml.YAMLError as e:
        print(f"Error parsing config file: {e}")

if __name__ == "__main__":
    read_config()
