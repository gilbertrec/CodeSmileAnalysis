import csv
from pydriller import Repository
import os

# Separator character for modified files
separator = "|"


def read_csv(file_path):
    """Read project name and commit hash from CSV."""
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        return list(reader)


def extract_commit_details(repo_path, commit_id):
    """Extract commit details using PyDriller."""
    details = []

    for commit in Repository(repo_path, single=commit_id).traverse_commits():
        modified_files = separator.join([mod.filename for mod in commit.modified_files])
        details.append({
            'commit_date': commit.committer_date,
            'commit_number_from_start': commit.order,
            'commit_message': commit.msg,
            'modified_files': modified_files
        })
    return details


def write_csv(file_path, data, fieldnames):
    """Write extracted details to a CSV file."""
    with open(file_path, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def analyser(input_csv_path,base_path):
    """Analyze commits based on the input CSV path."""
    projects = read_csv(input_csv_path)

    commit_details = []

    for project in projects:
        repo_path = project['project_name']
        repo_path = os.path.join(base_path,repo_path)# Local path to the repository
        commit_id = project['Commit_ID']  # Commit hash

        if os.path.exists(repo_path):
            try:
                details = extract_commit_details(repo_path, commit_id)
                for detail in details:
                    commit_details.append({
                        'project_name': project['project_name'],
                        'Commit_ID': commit_id,
                        'commit_date': detail['commit_date'],
                        'commit_number_from_start': detail['commit_number_from_start'],
                        'commit_message': detail['commit_message'],
                        'modified_files': detail['modified_files']
                    })
            except Exception as e:
                print(f"Error processing {repo_path} - {commit_id}: {e}")
        else:
            print(f"Repository path {repo_path} does not exist.")

    # Write results to output CSV file
    output_csv_path = f"date_commits_{os.path.basename(input_csv_path)}"
    fieldnames = ['project_name', 'Commit_ID', 'commit_date', 'commit_number_from_start', 'commit_message',
                  'modified_files']
    write_csv(output_csv_path, commit_details, fieldnames)


def main():
    """Main function to analyze different datasets."""
    for csv_file in ["NICHE_small_sampled.csv", "NICHE_medium_sampled.csv", "NICHE_large_sampled.csv"]:
        print(f"Processing {csv_file}...")
        analyser(csv_file, "/home/gilberto-1/github/NICHE_projects/project_history_analysis")


if __name__ == "__main__":
    main()
