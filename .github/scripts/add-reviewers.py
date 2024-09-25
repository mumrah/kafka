import argparse
import json
import shlex
import subprocess
from typing import Optional


def read_commiter_info(gh_username: str, field: str) -> Optional[str]:
    cmd = f"git config --blob asf-committers:committers.gitconfig committer.{gh_username}.{field}"
    p = subprocess.run(
        shlex.split(cmd),
        capture_output=True
    )
    if p.returncode == 0:
        return p.stdout.decode().strip()
    else:
        return None


def run_gh_command(cmd: str) -> str:
    "Run a gh CLI command and return the stdout"
    p = subprocess.run(
        shlex.split(cmd),
        capture_output=True
    )
    if p.returncode == 4:
        # Not auth'd
        print(p.stderr.decode())
        exit(1)
    elif p.returncode == 0:
        return p.stdout.decode().strip()
    else:
        print(f"Had an error running '{cmd}'.\nSTDOUT: {p.stdout.decode()}\nSTDERR: {p.stderr.decode()}")
        exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate the reviewers for a PR")
    parser.add_argument("pull_request", type=int, help="Pull Request number")
    args = parser.parse_args()

    reviews_json = run_gh_command(f"gh pr view {args.pull_request} --json reviews")
    reviews = json.loads(reviews_json)

    approvers = []
    commenters = []
    for review in reviews.get("reviews", []):
        login = review.get("author", {}).get("login")
        state = review.get("state")
        if state == "APPROVED":
            approvers.append(login)
        elif state == "COMMENT":
            commenters.append(login)
        elif state == "REQUEST_CHANGES":
            exit(1)

    reviewers = []
    for approver in approvers:
        name = read_commiter_info(approver, "name")
        email = read_commiter_info(approver, "email")
        if name:
            reviewers.append(f"{name} <{email}>")

    print("Reviewers: " + ",".join(reviewers))