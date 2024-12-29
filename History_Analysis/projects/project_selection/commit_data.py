import os
import time
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def create_github_session(token):
    session = requests.Session()
    session.headers.update({
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    })

    retry_strategy = Retry(
        total=5,
        backoff_factor=60,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session
def get_commit_details(session, repo_name, commit_sha):
    url = f"https://api.github.com/repos/{repo_name}/commits/{commit_sha}"
    print(f"Fetching details for commit {commit_sha} of project {repo_name}...")

    response = session.get(url)
    print(f"Response received for commit {commit_sha}: {response.status_code}")

    if response.status_code == 403:
        print("Rate limit reached. Waiting for 60 seconds...")
        time.sleep(60)
        return None

    if response.status_code != 200:
        print(f"Failed to fetch commit {commit_sha} for {repo_name}: {response.status_code}")
        return None

    commit_details = response.json()
    commit_date = commit_details["commit"]["committer"]["date"]
    commit_message = commit_details["commit"]["message"]
    files_modified = [file["filename"] for file in commit_details.get("files", [])]

    return {
        "commit_date": commit_date,
        "commit_message": commit_message,
        "files_modified": files_modified
    }


def collect_project_commit_details_from_csv(csv_path, token):
    session = create_github_session(token)
    all_projects_data = []

    # Load project names and commit IDs from the CSV
    project_data = pd.read_csv(csv_path)
    if not {"project_name", "Commit_ID"}.issubset(project_data.columns):
        print("Error: The CSV file must contain 'project_name' and 'Commit_ID' columns.")
        return None
    project_data = project_data[["project_name", "Commit_ID"]]
    #remove duplicates
    project_data = project_data.drop_duplicates()
    for _, row in project_data.iterrows():
        project_name = row["project_name"]
        commit_id = row["Commit_ID"]

        print(f"Collecting data for project: {project_name}, commit: {commit_id}")
        try:
            commit_details = get_commit_details(session, project_name, commit_id)
            if commit_details:
                commit_details["project_name"] = project_name
                commit_details["Commit_ID"] = commit_id
                all_projects_data.append(commit_details)
        except Exception as e:
            print(f"Error fetching data for {project_name}, commit {commit_id}: {e}")

    return all_projects_data
def main(mode):
    # Specify the path to the CSV file containing project names
    csv_path = f"NICHE_{mode}_sampled.csv"  # Replace with your CSV file path
    github_token = ""  # Replace with your GitHub token

    # Collect commit details for all projects listed in the CSV
    all_commit_data = collect_project_commit_details_from_csv(csv_path, github_token)

    # Convert the collected data to a DataFrame
    if all_commit_data:
        df = pd.DataFrame(all_commit_data)
        output_path = f"commit_details_{mode}.csv"
        df.to_csv(output_path, index=False)
        print(f"Commit details saved to {output_path}")
    else:
        print("No commit data collected.")

if __name__ == "__main__":
    main("large")
    main("medium")
    main("small")