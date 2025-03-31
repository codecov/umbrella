#!/bin/sh

# Command line arguments
repo_name="$1"
remote_url="$2"
subdirectory="$3"

# Variables used throughout the script
local_main_checkout="$repo_name-main"
current_branch="$(git rev-parse --abbrev-ref HEAD)"

# Assumes this script's directory has a sibling directory called `git-filter-repo`
# which contains a copy of the `git-filter-repo` script.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
GIT_FILTER_REPO_DIR="$(realpath "$SCRIPT_DIR/../git-filter-repo")"

function usage() {
    echo "Usage:"
    echo "  $0 <name> <remote-url> <subdirectory>"
    echo "  Example: $0 worker git@github.com:codecov/worker.git apps/worker"
    echo ""
    echo "This script absorbs a repository into the monorepo in a way that (mostly)"
    echo "preserves its history. History will be rewritten so that the repository's"
    echo "contents will be, or \"will have always been\", in a subdirectory, but"
    echo "otherwise the authors and dates of each commit will be preserved. Operations"
    echo "like \`git blame\` can continue past the point where the repository was"
    echo "absorbed."
    echo ""
    echo "Arguments:"
    echo "- <name> is the name of the repository you are absorbing. Used to name the git"
    echo "  remote and the branch used to open a PR."
    echo ""
    echo "- <remote-url> must be a valid git remote URL that you have access to. Example:"
    echo "  \`git@github.com:codecov/worker.git\`"
    echo ""
    echo "- <subdirectory> is a path relative to the repository root where <repository>"
    echo "  should be placed."
    echo ""
    echo "This script must be run from inside a git repository."
    exit 1
}

if [ $# -ne 3 ] || [ "$(git rev-parse --is-inside-work-tree)" != "true" ]; then
    usage
fi

if [ -d "$subdirectory" ]; then
    echo "Removing existing $subdirectory to make room to absorb $repo_name..."
    git rm "$subdirectory"
    git commit -m "Removing existing $subdirectory to make room to absorb $repo_name"
fi

echo "Adding \`$remote_url\` as a remote named \`$repo_name\`..."
git remote add $repo_name $remote_url

git ls-remote $repo_name | grep main > /dev/null && branch="main" || branch="master"
git fetch $repo_name $branch
echo "Found repository with default branch \`$branch\`"
echo ""

echo "Checking out \`$repo_name/$branch\` locally as \`$local_main_checkout\`..."
git checkout -b "$local_main_checkout" $repo_name/$branch
echo "Done"
echo ""

echo "Rewriting history to put all of $repo_name's contents inside \`$subdirectory\`..."
# We have to go back to the branch we started on in order to access the `git-filter-repo`
# custom command.
git checkout "$current_branch"
python "$GIT_FILTER_REPO_DIR/git-filter-repo" --force --refs "$local_main_checkout" --to-subdirectory-filter "$subdirectory"
echo "Done"
echo ""

echo "Merging the rewritten \`$local_main_checkout\` into \`$current_branch\`"
git merge "$local_main_checkout" --allow-unrelated-histories --no-edit
echo "Done"
echo ""

echo "Cleaning up after ourselves..."
git branch -D $local_main_checkout
git remote remove $repo_name
echo "Done"
