import csv
import requests
import os
import pandas as pd
from datetime import datetime

# GitHub API Token (set your GitHub Personal Access Token here)
GITHUB_TOKEN = ""  # Recommended to set as an environment variable
GITHUB_API_URL = "https://api.github.com/repos"
HEADERS = {
    'Authorization': f'token {GITHUB_TOKEN}'
}


def fetch_commit_details(project_name, commit_id):
    """Fetch commit details from GitHub API."""
    try:
        url = f"{GITHUB_API_URL}/{project_name}/commits/{commit_id}"
        response = requests.get(url, headers=HEADERS)

        if response.status_code == 200:
            commit_data = response.json()
            commit_date = commit_data['commit']['committer']['date']
            commit_message = commit_data['commit']['message']
            return commit_date, commit_message
        else:
            print(f"Error fetching data for {project_name}/{commit_id}: {response.status_code}")
            return None, None
    except Exception as e:
        print(f"Exception occurred: {e}")
        return None, None


def append_commit_details(input_csv, output_csv):
    """Append commit details to the input CSV and save as output CSV."""
    # Load the input CSV
    df = pd.read_csv(input_csv)

    # Check for required columns
    if 'Commit_ID' not in df.columns or 'project_name' not in df.columns:
        print("Input CSV must contain 'CommitID' and 'project_name' columns.")
        return

    # Add new columns
    df['Commit_date'] = None
    df['Commit_message'] = None

    # Fetch commit details for each row
    for index, row in df.iterrows():
        commit_id = row['Commit_ID']
        project_name = row['project_name']

        commit_date, commit_message = fetch_commit_details(project_name, commit_id)

        if commit_date and commit_message:
            df.at[index, 'Commit_date'] = commit_date
            df.at[index, 'Commit_message'] = commit_message

    # Save the updated DataFrame to a new CSV
    df.to_csv(output_csv, index=False)
    print(f"Updated CSV saved to {output_csv}")


if __name__ == "__main__":
    mode = "large"

    input_csv = "NICHE_large_sampled.csv"  # Replace with your input CSV file path
    output_csv = "commit_NICHE_large_sampled.csv"  # Replace with your desired output CSV file path

    if not GITHUB_TOKEN:
        print("Please set your GitHub Personal Access Token as an environment variable GITHUB_TOKEN.")
    else:
        append_commit_details(input_csv, output_csv)

    mode = "medium"

    input_csv = "NICHE_medium_sampled.csv"  # Replace with your input CSV file path
    output_csv = "commit_NICHE_medium_sampled.csv"  # Replace with your desired output CSV file path

    if not GITHUB_TOKEN:
        print("Please set your GitHub Personal Access Token as an environment variable GITHUB_TOKEN.")
    else:
        append_commit_details(input_csv, output_csv)

    mode = "small"

    input_csv = "NICHE_small_sampled.csv"  # Replace with your input CSV file path
    output_csv = "commit_NICHE_small_sampled.csv"  # Replace with your desired output CSV file path

    append_commit_details(input_csv, output_csv)