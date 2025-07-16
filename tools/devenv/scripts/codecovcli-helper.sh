#!/bin/bash

# List git remotes, use `origin` if it's found else use the first one
IFS=$'\n' read -r -d '' -a remotes < <( git remote && printf '\0' )
origin=$([[ "${remotes[*]}" =~ "origin" ]] && echo "origin" || echo "${remotes[0]}")

# Process the origin to get a key of the format `host:owner/repo`
# Example: git@github.com:codecov/codecov-cli.git -> github.com:codecov/codecov-cli
# Example: https://matt-codecov@bitbucket.org:codecov/codecov-demo
key=$(git remote get-url $origin \
    | sed -E 's/(git@|https:\/\/)([a-zA-Z0-9-]+@){0,1}(github\.com|bitbucket\.org|gitlab\.com)[\/:]([a-zA-Z0-9-]+)\/([a-zA-Z0-9-]+)(.git){0,1}.*/\3:\4\/\5/')

token=$(cat ~/.local-codecov-tokens 2> /dev/null | grep "$key=" | cut -d '=' -f 2-)

CODECOV_TOKEN=$token codecovcli --enterprise-url=http://localhost:8080 "$@"
