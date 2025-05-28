import sys
import os

# Add repo root to sys.path
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(repo_root)

from config.aerodemo_config import CONFIG

print(CONFIG)