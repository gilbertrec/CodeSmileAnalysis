import csv
import requests

# Input and Output file paths
input_csv = 'NICHE_large_sampled.csv'  # Replace with your input CSV file path
output_csv = 'release_large_output_extra.csv'  # Replace with your desired output CSV file path
mined_csv = 'release_large_output.csv'  # Replace with your desired output CSV file path
GITHUB_TOKEN = ""  # Replace with your GitHub personal access token
GITHUB_API_URL = "https://api.github.com"

def get_release_commits(owner, repo):
    """Retrieve the list of commit hashes associated with releases."""
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    releases_url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/releases"
    page = 1
    commit_hashes = []

    while True:
        response = requests.get(f"{releases_url}?page={page}", headers=headers)
        if response.status_code == 200:
            releases = response.json()
            if not releases:
                break

            for release in releases:
                tag_name = release.get("tag_name")
                # Fetch the tag details to find the associated commit hash
                tag_url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/git/ref/tags/{tag_name}"
                tag_response = requests.get(tag_url, headers=headers)
                if tag_response.status_code == 200:
                    tag_data = tag_response.json()
                    commit_sha = tag_data.get("object", {}).get("sha")
                    if commit_sha:
                        commit_hashes.append(commit_sha)
        else:
            print(f"Failed to fetch releases for {owner}/{repo}, status code: {response.status_code}")
            break
        page += 1

    return commit_hashes

def get_mined_commits(reader):
    """read already mined commits from a file"""
    mined_commits = set()
    for row in reader:
        mined_commits.add(row['Project'])
    return mined_commits
def main():
    # Read the input CSV
    with open(input_csv, 'r') as infile, open(output_csv, 'w', newline='') as outfile, open(mined_csv, 'r') as mined_file:
        reader = csv.DictReader(infile)
        mined_reader = csv.DictReader(mined_file)
        writer = csv.writer(outfile)
        mined_projects = get_mined_commits(mined_reader)
        # Write header for the output CSV
        writer.writerow(['Project', 'CommitHash'])
        projects = set()
        for row in reader:
            if row['project_name'] not in mined_projects:
                projects.add(row['project_name'])

        for project in projects:
            owner = project.split('/')[0]
            repo_name = project.split('/')[1]

            try:
                commit_hashes = get_release_commits(owner, repo_name)
                for commit_hash in commit_hashes:
                    writer.writerow([project, commit_hash])
                print("Added commit hash for project:", project)
            except Exception as e:
                print(f"Error processing project {project}: {e}")

if __name__ == '__main__':
    main()
