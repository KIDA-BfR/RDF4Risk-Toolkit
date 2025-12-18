


# Quick Setup Guide

This guide explains how to set up a **Python environment** and **run the Streamlit apps**.

---

## 1. Create a Python Virtual Environment (Recommended)

It is strongly recommended to use a virtual environment to keep your system clean and avoid package conflicts.

### a) Navigate to your project directory
In the console (e.g., Command Prompt, Terminal, or PowerShell):

```bash
# Example: Switch to the desired drive (if needed)
N:

# Change directory to your project folder
cd path\to\your\project
```

### b) Create the virtual environment

```bash
python -m venv .venv
```
This will create a `.venv` folder in your project directory.

---

## 2. Activate the Virtual Environment

- **Windows:**

```bash
.venv\Scripts\activate
```

- **macOS/Linux:**

```bash
source .venv/bin/activate
```

If activated successfully, you will see the environment name (`(.venv)`) appear in front of your console prompt.

---

## 3. Install Dependencies

Make sure you are inside the activated environment. Then install all required libraries:

```bash
pip install -r requirements.txt
```

---

## 4. Run the Streamlit App

After activating the environment and installing the dependencies:

1. Make sure you are in the folder where your Streamlit script (e.g., `Home.py`, `tablegenerator.py`) is located.
   
2. Then run:

```bash
streamlit run Home.py
```

> If you switched drives (e.g., from `C:` to `N:`), remember to `cd` into the correct folder first.

Example:
```bash
N:
cd Projects\MyStreamlitApp
streamlit run Home.py
```

---

# Notes
- Always activate your virtual environment before running or installing anything.
- To deactivate the environment after work, just type:

```bash
deactivate
```

---

# Quick Commands Summary

```bash
# Navigate to your project directory
N:
cd path\to\your\project

# Create and activate virtual environment
python -m venv .venv
.venv\Scripts\activate   # (Windows)
source .venv/bin/activate # (macOS/Linux)

# Install all required packages
pip install -r requirements.txt

# Run your Streamlit app
streamlit run Home.py
```

Author and Developer: 
Michael Zarske 
Taras Günther 