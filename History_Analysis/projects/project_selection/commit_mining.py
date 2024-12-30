import os
import pandas as pd
from pydriller import RepositoryMining

def get_all_commit_details_pydriller(repo_path):
    all_commit_data = []
    try:
        for commit in RepositoryMining(repo_path).traverse_commits():
            all_commit_data.append({
                "commit_date": commit.committer_date,
                "commit_message": commit.msg,
                "files_modified": [mod.filename for mod in commit.modifications],
                "CommitID": commit.hash
            })
    except Exception as e:
        print(f"Error processing repository {repo_path}: {e}")
    return all_commit_data

def collect_project_commit_details_from_csv(csv_path):
    all_projects_data = []

    # Load project names from the CSV
    project_data = pd.read_csv(csv_path)
    if "project_name" not in project_data.columns:
        print("Error: The CSV file must contain a 'project_name' column.")
        return None

    for _, row in project_data.iterrows():
        project_name = row["project_name"]
        repo_path = os.path.join("cloned_repos", project_name)  # Assumes repos are in a 'cloned_repos' folder

        if not os.path.exists(repo_path):
            print(f"Repository path not found: {repo_path}. Skipping...")
            continue

        print(f"Collecting all commit data for project: {project_name}")
        try:
            project_commit_data = get_all_commit_details_pydriller(repo_path)
            for commit_data in project_commit_data:
                commit_data["project_name"] = project_name
                all_projects_data.append(commit_data)
        except Exception as e:
            print(f"Error fetching data for project {project_name}: {e}")

    return all_projects_data

if __name__ == "__main__":
    # Specify the path to the CSV file containing project names
    csv_path = "path_to_project_list.csv"  # Replace with your CSV file path

    # Collect commit details for all projects listed in the CSV
    all_commit_data = collect_project_commit_details_from_csv(csv_path)

    # Convert the collected data to a DataFrame
    if all_commit_data:
        df = pd.DataFrame(all_commit_data)
        output_path = "commit_details_pydriller.csv"
        df.to_csv(output_path, index=False)
        print(f"Commit details saved to {output_path}")
    else:
        print("No commit data collected.")
