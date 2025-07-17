import streamlit as st
import sys
import os
from pathlib import Path

# Add src directory to path
sys.path.append(str(Path(__file__).parent / "src"))

from ui.main_ui import main

if __name__ == "__main__":
    main()