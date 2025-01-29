# Wikipedia External Links Scanner

A **streaming Python script** that searches multiple Wikipedia language editions for external links to specific domains, identifies the user who first introduced each link, and collects user information and recent contributions.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Notes and Limitations](#notes-and-limitations)
- [Licence](#licence)

## Overview

This project allows you to:
1. Read a list of **domains** from a CSV file (`domains.csv`).
2. Read a list of **Wikipedia language codes** from a CSV file (`wiki_versions.csv`).
3. For each `(language, domain)` pair, search for external links in Wikipedia using the `exturlusage` API.
4. For each link found, **immediately** find the user who introduced it by scanning the **revision history** of that Wikipedia page.
5. Collect the user’s additional metadata (e.g. edit count, registration date) and some of their recent edits (via `usercontribs`).
6. Write out:
   - `results.csv`: details about each external link usage (including the introduction user).
   - `user_info_all.csv`: user details for each discovered user.
   - `user_contributions_all.csv`: recent contributions for each discovered user.

This **streaming approach** minimises memory usage by writing data to CSV as soon as it’s discovered and by de-duplicating `(lang, user)` pairs on disk (rather than storing in memory).

## Features

- **Parallel requests**: uses multi-threading to speed up API calls.
- **Memory-friendly**: writes results as they are generated; does not hold all data in memory at once.
- **User metadata**: captures relevant user info (edit count, registration, user rights, etc.).
- **User contributions**: fetches up to a specified limit of recent user contributions.

## Requirements

- **Python 3.7+** (tested on Python 3.9+).
- Packages specified in [requirements.txt](requirements.txt):
  - `requests`
  - `pandas`
- A Unix-like environment (Linux, macOS, or WSL on Windows) is recommended, as the script relies on the `sort` command for on-disk de-duplication of `(lang, user)` pairs.  
  If you are on Windows without a Unix shell, you can install Git Bash or adapt the script to another deduplication method (e.g. SQLite).

## Installation

1. **Clone** this repository:

   ```bash
   git clone https://github.com/yourusername/wikipedia-external-links-scanner.git
   cd wikipedia-external-links-scanner
   ```
   
2. **Install Python dependencies**:
   	
	```bash
	pip install -r requirements.txt
	```

3. Prepare your input CSVs inside the sources/ directory:
- domains.csv: a list of domains to search (e.g. example.com, one per line).
- wiki_versions.csv: a list of all Wikipedia language codes, with columns Wikipedia Name and Language Code.
   
## Usage
   
1. Make sure your sources/ folder has:
- `domains.csv`
- `wiki_versions.csv`

2. Run the main script:

`python3 wiki_search.py`

Where `wiki_search.py` is the script that:

- Reads the input files.
- Fetches external links in a streaming fashion.
- Finds the user who introduced each link.
- Writes results to `results.csv`.
- De-duplicates `(lang, user)` pairs on disk via `sort | uniq`.
- Fetches user info + contributions.
- Generates `user_info_all.csv` and `user_contributions_all.csv`.

3. Check the output CSVs:

- results_with_user.csv
- user_info_all.csv
- user_contributions_all.csv
