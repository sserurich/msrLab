#!/usr/bin/env python3
"""Extract merged pull-request summaries and per-file details from GitHub."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from github_config import load_dotenv

DEFAULT_REPOSITORY = "apache/kafka"
DEFAULT_PULL_NUMBERS = (
    12398,
    13175,
    12751,
    12660,
    12532,
    12289,
    14151,
)
DEFAULT_SUMMARY_OUTPUT = Path("data/sserurich_kafka_merged_pull_requests.csv")
DEFAULT_FILES_OUTPUT = Path("data/sserurich_kafka_merged_pull_requests_files.csv")
API_URL = "https://api.github.com"
API_VERSION = "2026-03-10"
MAX_RETRIES = 5

SUMMARY_COLUMNS = (
    "pull_number",
    "title",
    "author",
    "author_association",
    "state",
    "merged_at",
    "created_at",
    "updated_at",
    "closed_at",
    "base_branch",
    "head_branch",
    "commits",
    "additions",
    "deletions",
    "changed_files",
    "labels",
    "html_url",
    "body",
)

FILE_COLUMNS = (
    "RepoName",
    "PRNumber",
    "MergeCommit",
    "merged_at",
    "Filename",
    "Status",
    "added",
    "deleted",
)


class GitHubAPIError(Exception):
    """Raised when a GitHub API request cannot be completed."""

    def __init__(self, status: int | None, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract summary and per-file data for merged Apache Kafka pull requests."
    )
    parser.add_argument("--repo", default=DEFAULT_REPOSITORY)
    parser.add_argument("--pull-number", type=int, action="append", dest="pull_numbers")
    parser.add_argument("--output", type=Path, default=DEFAULT_SUMMARY_OUTPUT)
    parser.add_argument("--files-output", type=Path, default=DEFAULT_FILES_OUTPUT)
    parser.add_argument(
        "--token",
        default=os.environ.get("GITHUB_TOKEN"),
        help="GitHub token; GITHUB_TOKEN and .env are also supported.",
    )
    parser.add_argument("--no-auth", action="store_true")
    return parser.parse_args()


def validate_repository(repository: str) -> tuple[str, str]:
    if repository.count("/") != 1:
        raise ValueError("Repository must use owner/repo format.")
    owner, name = repository.split("/", 1)
    if not owner or not name:
        raise ValueError("Repository owner and name must both be non-empty.")
    return owner, name


def validate_pull_numbers(pull_numbers: list[int] | None) -> list[int]:
    if pull_numbers is None:
        return list(DEFAULT_PULL_NUMBERS)
    if not pull_numbers or any(number <= 0 for number in pull_numbers):
        raise ValueError("At least one positive pull request number is required.")
    if len(set(pull_numbers)) != len(pull_numbers):
        raise ValueError("Pull request numbers must not contain duplicates.")
    return pull_numbers


def build_headers(token: str | None) -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": API_VERSION,
        "User-Agent": "msrLab-pull-request-extractor",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def wait_for_rate_limit(payload: dict[str, Any]) -> bool:
    if payload.get("resource") != "rate_limit" or "reset" not in payload:
        return False
    delay = max(0, int(payload["reset"]) - int(time.time()))
    print(f"GitHub rate limit reached. Waiting {delay} seconds...", file=sys.stderr)
    time.sleep(delay)
    return True


def request_json(url: str, token: str | None) -> Any:
    request = Request(url, headers=build_headers(token), method="GET")
    for attempt in range(MAX_RETRIES):
        try:
            with urlopen(request, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            payload = json.loads(error.read().decode("utf-8", errors="replace"))
            if error.code == 403 and wait_for_rate_limit(payload):
                continue
            raise GitHubAPIError(error.code, payload.get("message", error.reason)) from error
        except (URLError, TimeoutError, json.JSONDecodeError) as error:
            if attempt == MAX_RETRIES - 1:
                raise GitHubAPIError(None, f"Unable to read GitHub API: {error}") from error
            delay = 2 ** attempt
            print(f"GitHub request failed; retrying in {delay} seconds...", file=sys.stderr)
            time.sleep(delay)
    raise AssertionError("Unreachable")


def get_pull_request(owner: str, repo: str, number: int, token: str | None) -> dict[str, Any]:
    url = f"{API_URL}/repos/{quote(owner, safe='')}/{quote(repo, safe='')}/pulls/{number}"
    pull_request = request_json(url, token)
    print(f"url:{url}")
    print(f"pull request:{pull_request}")
    if not isinstance(pull_request, dict):
        raise GitHubAPIError(None, f"Pull request {number} did not return a JSON object.")
    if pull_request.get("number") != number:
        raise GitHubAPIError(None, f"Pull request {number} returned an unexpected response.")
    if pull_request.get("merged_at") is None:
        raise GitHubAPIError(None, f"Pull request {number} is not merged.")
    return pull_request


def get_pull_request_files(
    owner: str, repo: str, number: int, token: str | None
) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    page = 1
    while True:
        url = (
            f"{API_URL}/repos/{quote(owner, safe='')}/{quote(repo, safe='')}/pulls/"
            f"{number}/files?per_page=100&page={page}"
        )
        payload = request_json(url, token)
        if not isinstance(payload, list):
            raise GitHubAPIError(None, f"Files for pull request {number} did not return a list.")
        files.extend(payload)
        if len(payload) < 100:
            break
        page += 1
    return files


def get_or_default(payload: dict[str, Any], key: str, default: Any = None) -> Any:
    return payload.get(key) or default


def build_summary_rows(pull_request: dict[str, Any]) -> list[dict[str, Any]]:
    user = pull_request.get("user") or {}
    base = pull_request.get("base") or {}
    head = pull_request.get("head") or {}
    labels = pull_request.get("labels") or []
    values = (
        ("pull_number", get_or_default(pull_request, "number")),
        ("title", get_or_default(pull_request, "title", "")),
        ("author", get_or_default(user, "login", "")),
        ("author_association", get_or_default(pull_request, "author_association", "")),
        ("state", get_or_default(pull_request, "state", "")),
        ("merged_at", get_or_default(pull_request, "merged_at", "")),
        ("created_at", get_or_default(pull_request, "created_at", "")),
        ("updated_at", get_or_default(pull_request, "updated_at", "")),
        ("closed_at", get_or_default(pull_request, "closed_at", "")),
        ("base_branch", get_or_default(base, "ref", "")),
        ("head_branch", get_or_default(head, "ref", "")),
        ("commits", get_or_default(pull_request, "commits", 0)),
        ("additions", get_or_default(pull_request, "additions", 0)),
        ("deletions", get_or_default(pull_request, "deletions", 0)),
        ("changed_files", get_or_default(pull_request, "changed_files", 0)),
        ("labels", ";".join(str(label.get("name", "")) for label in labels)),
        ("html_url", get_or_default(pull_request, "html_url", "")),
        ("body", get_or_default(pull_request, "body", "")),
    )
    return [dict(values)]


def build_file_rows(
    owner: str,
    repo: str,
    number: int,
    pull_request: dict[str, Any],
    token: str | None,
) -> list[dict[str, Any]]:
    repository = f"{owner}/{repo}"
    merge_commit = pull_request.get("merge_commit_sha") or ""
    merged_at = pull_request.get("merged_at") or ""
    rows: list[dict[str, Any]] = []
    for changed_file in get_pull_request_files(owner, repo, number, token):
        rows.append(
            {
                "RepoName": repository,
                "PRNumber": number,
                "MergeCommit": merge_commit,
                "merged_at": merged_at,
                "Filename": changed_file.get("filename") or "",
                "Status": changed_file.get("status") or "",
                "added": changed_file.get("additions") or 0,
                "deleted": changed_file.get("deletions") or 0,
            }
        )
    return rows


def write_csv(output_path: Path, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    load_dotenv()
    args = parse_args()
    try:
        owner, repo = validate_repository(args.repo)
        pull_numbers = validate_pull_numbers(args.pull_numbers)
        if args.no_auth and args.token:
            raise ValueError("--no-auth cannot be combined with a token.")
        token = None if args.no_auth else args.token
        if not token:
            print(
                "No GitHub token was provided. Unauthenticated requests are limited to 60 per hour.",
                file=sys.stderr,
            )

        summary_rows: list[dict[str, Any]] = []
        file_rows: list[dict[str, Any]] = []
        for number in pull_numbers:
            pull_request = get_pull_request(owner, repo, number, token)
            summary_rows.extend(build_summary_rows(pull_request))
            file_rows.extend(build_file_rows(owner, repo, number, pull_request, token))

        write_csv(args.output, SUMMARY_COLUMNS, summary_rows)
        write_csv(args.files_output, FILE_COLUMNS, file_rows)
    except (GitHubAPIError, ValueError, OSError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    print(f"Wrote {len(summary_rows)} summary rows to {args.output}")
    print(f"Wrote {len(file_rows)} file rows to {args.files_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
