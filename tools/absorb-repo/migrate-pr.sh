#!/bin/bash

pull_request=$1


_jq_head_repo_owner=".headRepositoryOwner = .headRepositoryOwner.login"
_jq_head_repo_name=".headRepository = .headRepository.name"

__jq_reviewed_by="with_entries(if .key == \"reviews\" then .value = [.value[].author.login] else . end)"
__jq_review_requested_from="with_entries(if .key == \"reviewRequests\" then .value = [.value[].slug] end)"
_jq_reviewers="$__jq_reviewed_by | $__jq_review_requested_from | .reviewers = ([.reviews, .reviewRequests] | flatten) | del(.reviews) | del(.reviewRequests)"

jq_query="$_jq_head_repo_owner | $_jq_head_repo_name | $_jq_reviewers"

pr_data=$(gh pr view $pull_request --json title,body,headRefName,headRepositoryOwner,headRepository,reviews,reviewRequests | jq "$jq_query")

pr_title=$(echo $pr_data | jq -r '.title')
pr_body=$(echo $pr_data | jq -r \""(migrated from $pull_request)\r\n\r\n\""' + .body')
pr_head_repo=$(echo $pr_data | jq -r '.headRepository')
pr_head_repo_owner=$(echo $pr_data | jq -r '.headRepositoryOwner')
pr_head_ref=$(echo $pr_data | jq -r '.headRefName')
pr_reviewers=$(echo $pr_data | jq -r '.reviewers' | sed 's/\[/-r /' | sed 's/,/-r /g' | sed 's/\]//')

# Assumes this script's directory has a sibling directory called `git-filter-repo`
# which contains a copy of the `git-filter-repo` script.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GIT_FILTER_REPO_DIR="$(realpath "$SCRIPT_DIR/../git-filter-repo")"

current_branch="$(git rev-parse --abbrev-ref HEAD)"
migrated_ref="$pr_head_repo/$pr_head_ref"
remote_name="$pr_head_repo-orig"

case $pr_head_repo in
    "worker" | "codecov-api")
        subdirectory="apps/$pr_head_repo"
        ;;
    "shared")
        subdirectory="libs/$pr_head_repo"
        ;;
    *)
        echo "Uh oh"
        exit 1
        ;;
esac

if ! $(git remote | grep $remote_name); then
    echo "Adding remote for $pr_head_repo_owner/$pr_head_repo..."
    git remote add $remote_name git@github.com:$pr_head_repo_owner/$pr_head_repo || true
    echo "Done"
fi

echo "Checking out $pr_head_repo/$pr_head_ref as _$migrated_ref..."
git fetch $remote_name $pr_head_ref
git checkout -b "_$migrated_ref" $remote_name/$pr_head_ref

# Going back to the starting branch to run branch mutation
git checkout "$current_branch"
python "$GIT_FILTER_REPO_DIR/git-filter-repo" --force --refs "_$migrated_ref" --to-subdirectory-filter "$subdirectory"

# Create a branch in this repository to merge into
git checkout -b $migrated_ref
git merge "_$migrated_ref" --allow-unrelated-histories --no-edit
git branch -D _$migrated_ref
git remote remove $remote_name

git push origin $migrated_ref
gh pr create --title "$pr_title" --body "$pr_body" $pr_reviewers
