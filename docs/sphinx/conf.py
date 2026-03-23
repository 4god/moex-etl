import os
import sys

project = "MOEX Data Vault Workshop"
author = "Workshop Team"
release = "1.0.0"

sys.path.insert(0, os.path.abspath("../.."))
sys.path.insert(0, os.path.abspath("../../dags"))

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "alabaster"
html_static_path = ["_static"]

autodoc_mock_imports = [
    "airflow",
    "airflow.decorators",
    "airflow.providers",
    "kafka",
    "requests",
]
