import logging
import os
import urllib.parse as urllib_parse

import httpx

from shared.torngit.base import TokenType, TorngitBaseAdapter
from shared.torngit.enums import Endpoints
from shared.torngit.exceptions import (
    TorngitClientError,
    TorngitClientGeneralError,
    TorngitObjectNotFoundError,
    TorngitServer5xxCodeError,
    TorngitServerUnreachableError,
)
from shared.torngit.response_types import ProviderPull
from shared.torngit.status import Status
from shared.utils.urls import url_concat

log = logging.getLogger(__name__)

METRICS_PREFIX = "services.torngit.bitbucket"


class Bitbucket(TorngitBaseAdapter):
    _OAUTH_AUTHORIZE_URL = "https://bitbucket.org/site/oauth2/authorize"
    _OAUTH_ACCESS_TOKEN_URL = "https://bitbucket.org/site/oauth2/access_token"
    service = "bitbucket"
    api_url = "https://api.bitbucket.org"
    service_url = "https://bitbucket.org"
    urls = {
        "repo": "{username}/{name}",
        "owner": "{username}",
        "user": "{username}",
        "issues": "{username}/{name}/issues/{issueid}",
        "commit": "{username}/{name}/commits/{commitid}",
        "commits": "{username}/{name}/commits",
        "src": "{username}/{name}/src/{commitid}/{path}",
        "create_file": "{username}/{name}/create-file/{commitid}?at={branch}&filename={path}&content={content}",
        "tree": "{username}/{name}/src/{commitid}",
        "branch": "{username}/{name}/branch/{branch}",
        "pull": "{username}/{name}/pull-requests/{pullid}",
        "compare": "{username}/{name}",
    }

    async def _send_request(self, client, method, url, kwargs, log_dict):
        try:
            res = await client.request(method.upper(), url, **kwargs)
            logged_body = None
            if res.status_code >= 300 and res.text is not None:
                logged_body = res.text
            log.log(
                logging.WARNING if res.status_code >= 300 else logging.INFO,
                "Bitbucket HTTP %s",
                res.status_code,
                extra=dict(body=logged_body, **log_dict),
            )
            return res
        except (httpx.NetworkError, httpx.TimeoutException):
            raise TorngitServerUnreachableError("Bitbucket was not able to be reached.")

    async def api(
        self, client, version, method, path, json=False, body=None, token=None, **kwargs
    ):
        url = f"{self.api_url}/{version}.0{path}"
        headers = {
            "Accept": "application/json",
            "User-Agent": os.getenv("USER_AGENT", "Default"),
        }

        url = url_concat(url, kwargs)

        if json:
            headers["Content-Type"] = "application/json"
        elif body is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        token_to_use = token or self.token
        headers["Authorization"] = f"Bearer {token_to_use['key']}"

        kwargs = {
            "json": body if body is not None and json else None,
            "data": body if body is not None and not json else None,
            "headers": headers,
        }
        log_dict = {
            "event": "api",
            "endpoint": path,
            "method": method,
            "bot": token_to_use.get("username"),
            "repo_slug": self.slug,
        }

        res = await self._send_request(client, method, url, kwargs, log_dict)

        if res.status_code == 401 and callable(self._on_token_refresh):
            new_token = None
            try:
                new_token = await self.refresh_token(client)
            except (TorngitClientGeneralError, TorngitServer5xxCodeError):
                log.warning(
                    "Bitbucket token refresh failed, raising original 401",
                    extra=log_dict,
                )
            if new_token is not None:
                headers["Authorization"] = f"Bearer {new_token['key']}"
                kwargs["headers"] = headers
                await self._on_token_refresh(new_token)
                res = await self._send_request(client, method, url, kwargs, log_dict)

        if res.status_code == 599:
            raise TorngitServerUnreachableError(
                "Bitbucket was not able to be reached, server timed out."
            )
        elif res.status_code >= 500:
            raise TorngitServer5xxCodeError("Bitbucket is having 5xx issues")
        elif res.status_code >= 300:
            message = f"Bitbucket API: {res.reason_phrase}"
            raise TorngitClientGeneralError(
                res.status_code,
                response_data={"content": res.content},
                message=message,
            )
        if res.status_code == 204:
            return None
        elif "application/json" in res.headers.get("Content-Type"):
            return res.json()
        else:
            return res.text

    async def refresh_token(self, client):
        """
        Exchanges the stored refresh token for a new access + refresh token pair.
        Returns the new token dict, or None if no refresh token is stored.

        ! side effect: updates self._token
        """
        current_token = self.token
        if not current_token.get("secret"):
            log.warning("Trying to refresh Bitbucket token with no refresh_token saved")
            return None

        res = await client.request(
            "POST",
            self._OAUTH_ACCESS_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": current_token["secret"],
            },
            auth=(self._oauth["key"], self._oauth["secret"]),
        )
        if res.status_code >= 500:
            raise TorngitServer5xxCodeError("Bitbucket is having 5xx issues")
        if res.status_code >= 400:
            raise TorngitClientGeneralError(
                res.status_code,
                response_data={"content": res.content},
                message="Bitbucket token refresh failed",
            )
        data = res.json()
        new_token = {
            "key": data["access_token"],
            "secret": data.get("refresh_token", ""),
        }
        self.set_token(new_token)
        return new_token

    def generate_redirect_url(self, redirect_url, state=None):
        """
        Returns the Bitbucket OAuth 2.0 authorization URL to redirect the user to.
        """
        params: dict = {
            "client_id": self._oauth["key"],
            "response_type": "code",
            "redirect_uri": redirect_url,
        }
        if state is not None:
            params["state"] = state
        return f"{self._OAUTH_AUTHORIZE_URL}?{urllib_parse.urlencode(params)}"

    def generate_access_token(self, code, redirect_url):
        """
        Exchanges an OAuth 2.0 authorization code for an access token.
        Returns a dict with 'key' (access token) and 'secret' (refresh token).
        """
        r = httpx.post(
            self._OAUTH_ACCESS_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_url,
            },
            auth=(self._oauth["key"], self._oauth["secret"]),
        )
        if r.status_code >= 500:
            raise TorngitServer5xxCodeError("Bitbucket is having 5xx issues")
        elif r.status_code >= 400:
            message = f"Bitbucket OAuth2: {r.reason_phrase}"
            raise TorngitClientGeneralError(
                r.status_code,
                response_data={"content": r.content},
                message=message,
            )
        data = r.json()
        return {
            "key": data["access_token"],
            "secret": data.get("refresh_token", ""),
        }

    async def get_authenticated_user(self):
        async with self.get_client() as client:
            return await self.api(client, "2", "get", "/user")

    async def post_webhook(self, name, url, events, secret, token=None):
        # https://confluence.atlassian.com/bitbucket/webhooks-resource-735642279.html
        # https://confluence.atlassian.com/bitbucket/event-payloads-740262817.html
        async with self.get_client() as client:
            res = await self.api(
                client,
                "2",
                "post",
                f"/repositories/{self.slug}/hooks",
                body={
                    "description": name,
                    "active": True,
                    "events": events,
                    "url": url,
                },
                json=True,
                token=token,
            )
        res["id"] = res["uuid"][1:-1]
        return res

    async def edit_webhook(self, hookid, name, url, events, secret, token=None):
        # https://confluence.atlassian.com/bitbucket/webhooks-resource-735642279.html#webhooksResource-PUTawebhookupdate
        async with self.get_client() as client:
            res = await self.api(
                client,
                "2",
                "put",
                f"/repositories/{self.slug}/hooks/{hookid}",
                body={
                    "description": name,
                    "active": True,
                    "events": events,
                    "url": url,
                },
                json=True,
                token=token,
            )
        res["id"] = res["uuid"][1:-1]
        return res

    async def delete_webhook(self, hookid, token=None):
        # https://confluence.atlassian.com/bitbucket/webhooks-resource-735642279.html#webhooksResource-DELETEthewebhook
        async with self.get_client() as client:
            try:
                await self.api(
                    client,
                    "2",
                    "delete",
                    f"/repositories/{self.slug}/hooks/{hookid}",
                    token=token,
                )
            except TorngitClientError as ce:
                if ce.code == 404:
                    raise TorngitObjectNotFoundError(
                        response_data=ce.response_data,
                        message=f"Webhook with id {hookid} does not exist",
                    )
                raise
            return True

    async def get_is_admin(self, user, token=None):
        user_uuid = "{" + user["service_id"] + "}"
        workspace_slug = self.data["owner"]["username"]
        async with self.get_client() as client:
            groups = await self.api(
                client,
                "2",
                "get",
                f"/workspaces/{workspace_slug}/permissions",
                q=f'permission="owner" AND user.uuid="{user_uuid}"',
                token=token,
            )
        return bool(groups["values"])

    async def list_teams(self, token=None):
        teams, page = [], None
        async with self.get_client() as client:
            while True:
                if page is not None:
                    kwargs = {"page": page, "token": token}
                else:
                    kwargs = {"token": token}
                res = await self.api(client, "2", "get", "/user/workspaces", **kwargs)
                teams.extend(
                    {
                        "name": workspace["workspace"]["slug"],
                        "id": workspace["workspace"]["uuid"][1:-1],
                        "email": None,
                        "username": workspace["workspace"]["slug"],
                    }
                    for workspace in res["values"]
                )

                if not res.get("next"):
                    break
                url = res["next"]
                parsed = urllib_parse.urlparse(url)
                page = urllib_parse.parse_qs(parsed.query)["page"][0]

            return teams

    async def get_pull_request_commits(self, pullid, token=None):
        commits, page = [], None
        async with self.get_client() as client:
            while True:
                # https://confluence.atlassian.com/bitbucket/pullrequests-resource-423626332.html#pullrequestsResource-GETthecommitsforapullrequest
                if page is not None:
                    kwargs = {"page": page, "token": token}
                else:
                    kwargs = {"token": token}
                res = await self.api(
                    client,
                    "2",
                    "get",
                    f"/repositories/{self.slug}/pullrequests/{pullid}/commits",
                    **kwargs,
                )
                commits.extend([c["hash"] for c in res["values"]])
                if not res.get("next"):
                    break
                url = res["next"]
                parsed = urllib_parse.urlparse(url)
                page = urllib_parse.parse_qs(parsed.query)["page"][0]
        return commits

    async def _get_teams_and_username_to_list(self, username=None, token=None):
        if username is None:
            teams = await self.list_teams(token)
            usernames = {team["username"] for team in teams}
            usernames.add(self.data["owner"]["username"])
        else:
            usernames = [username]

        return usernames

    async def _fetch_page_of_repos(self, client, username, token, page):
        # https://confluence.atlassian.com/display/BITBUCKET/repositories+Endpoint#repositoriesEndpoint-GETalistofrepositoriesforanaccount
        res = await self.api(
            client,
            "2",
            "get",
            f"/repositories/{username}",
            page=page,
            token=token,
        )

        repos = []
        for repo in res.get("values", []):
            repo_name_arr = repo["full_name"].split("/", 1)

            repos.append(
                {
                    "owner": {
                        "service_id": repo["owner"]["uuid"][1:-1],
                        "username": repo_name_arr[0],
                    },
                    "repo": {
                        "service_id": repo["uuid"][1:-1],
                        "name": repo_name_arr[1],
                        "language": self._validate_language(repo["language"]),
                        "private": repo["is_private"],
                        "branch": "main",
                    },
                }
            )
        return (repos, res.get("next"))

    async def list_repos(self, username=None, token=None):
        data, page = [], 0
        usernames = await self._get_teams_and_username_to_list(username, token)
        log.info("Bitbucket: fetching repos from teams", extra={"usernames": usernames})
        async with self.get_client() as client:
            for team in usernames:
                page = 0
                try:
                    while True:
                        page += 1
                        repos, has_next = await self._fetch_page_of_repos(
                            client, team, token, page
                        )
                        data.extend(repos)
                        if len(repos) == 0 or not has_next:
                            break
                except TorngitClientError:
                    log.warning(
                        "Unable to fetch repos from team on Bitbucket",
                        extra={"team_name": team},
                    )
        log.info(
            "Bitbucket: finished fetching repos",
            extra={"usernames": usernames, "repos": data},
        )
        return data

    async def list_repos_generator(self, username=None, token=None):
        """
        New version of list_repos() that should replace the old one after safely
        rolling out in the worker.
        """
        usernames = await self._get_teams_and_username_to_list(username, token)
        log.info("Bitbucket: fetching repos from teams", extra={"usernames": usernames})
        async with self.get_client() as client:
            for team in usernames:
                page = 0
                try:
                    while True:
                        page += 1
                        repos, has_next = await self._fetch_page_of_repos(
                            client, team, token, page
                        )
                        yield repos
                        if len(repos) == 0 or not has_next:
                            break
                except TorngitClientError:
                    log.warning(
                        "Unable to fetch repos from team on Bitbucket",
                        extra={"team_name": team},
                    )
        log.info(
            "Bitbucket: finished fetching repos",
            extra={"usernames": usernames},
        )

    async def get_pull_request(self, pullid, token=None) -> ProviderPull | None:
        # https://confluence.atlassian.com/display/BITBUCKET/pullrequests+Resource#pullrequestsResource-GETaspecificpullrequest
        async with self.get_client() as client:
            try:
                res = await self.api(
                    client,
                    "2",
                    "get",
                    f"/repositories/{self.slug}/pullrequests/{pullid}",
                    token=token,
                )
            except TorngitClientError as ce:
                if ce.code == 404:
                    raise TorngitObjectNotFoundError(
                        response_data=ce.response_data,
                        message=f"PR with id {pullid} does not exist",
                    )
                raise
            # the commit sha is only {12}. need to get full sha
            base = await self.api(
                client,
                "2",
                "get",
                "/repositories/{}/commit/{}".format(
                    self.slug, res["destination"]["commit"]["hash"]
                ),
                token=token,
            )
            head = await self.api(
                client,
                "2",
                "get",
                "/repositories/{}/commit/{}".format(
                    self.slug, res["source"]["commit"]["hash"]
                ),
                token=token,
            )
        return ProviderPull(
            author={
                "id": str(res["author"]["uuid"][1:-1]) if res["author"] else None,
                "username": (
                    (res["author"].get("nickname") or res["author"].get("username"))
                    if res["author"]
                    else None
                ),
            },
            base={
                "branch": res["destination"]["branch"]["name"],
                "commitid": base["hash"],
            },
            head={"branch": res["source"]["branch"]["name"], "commitid": head["hash"]},
            state={"OPEN": "open", "MERGED": "merged", "DECLINED": "closed"}.get(
                res["state"]
            ),
            title=res["title"],
            id=str(pullid),
            number=str(pullid),
            merge_commit_sha=(
                res.get("merge_commit", {}).get("hash")
                if res["state"] == "MERGED"
                else None
            ),
        )

    async def post_comment(self, issueid, body, token=None):
        # https://confluence.atlassian.com/display/BITBUCKET/issues+Resource#issuesResource-POSTanewcommentontheissue
        async with self.get_client() as client:
            res = await self.api(
                client,
                "2",
                "post",
                f"/repositories/{self.slug}/pullrequests/{issueid}/comments",
                body={"content": {"raw": body}},
                json=True,
                token=token,
            )
            return res

    async def edit_comment(self, issueid, commentid, body, token=None):
        # https://confluence.atlassian.com/display/BITBUCKET/pullrequests+Resource+1.0#pullrequestsResource1.0-PUTanupdateonacomment
        # await self.api('1', 'put', '/repositories/%s/pullrequests/%s/comments/%s' % (self.slug, issueid, commentid),
        #                body=dict(content=body), token=token)
        async with self.get_client() as client:
            try:
                res = await self.api(
                    client,
                    "2",
                    "put",
                    f"/repositories/{self.slug}/pullrequests/{issueid}/comments/{commentid}",
                    body={"content": {"raw": body}},
                    json=True,
                    token=token,
                )
            except TorngitClientError as ce:
                if ce.code == 404:
                    raise TorngitObjectNotFoundError(
                        response_data=ce.response_data,
                        message=f"Comment {commentid} from PR {issueid} cannot be found",
                    )
                raise
            return res

    async def delete_comment(self, issueid, commentid, token=None):
        # https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Busername%7D/%7Brepo_slug%7D/pullrequests/%7Bpull_request_id%7D/comments/%7Bcomment_id%7D
        async with self.get_client() as client:
            try:
                await self.api(
                    client,
                    "2",
                    "delete",
                    f"/repositories/{self.slug}/pullrequests/{issueid}/comments/{commentid}",
                    token=token,
                )
            except TorngitClientError as ce:
                if ce.code == 404:
                    raise TorngitObjectNotFoundError(
                        response_data=ce.response_data,
                        message=f"Comment {commentid} from PR {issueid} cannot be found",
                    )
                raise
            return True

    async def get_commit_status(self, commit, token=None):
        # https://confluence.atlassian.com/bitbucket/buildstatus-resource-779295267.html
        statuses = await self.get_commit_statuses(commit, _in_loop=True, token=token)
        return str(statuses)

    async def get_commit_statuses(self, commit, token=None, _in_loop=None):
        statuses, page = [], 0
        status_keys = {
            "INPROGRESS": "pending",
            "SUCCESSFUL": "success",
            "FAILED": "failure",
        }
        async with self.get_client() as client:
            while True:
                page += 1
                # https://api.bitbucket.org/2.0/repositories/atlassian/aui/commit/d62ae57/statuses
                res = await self.api(
                    client,
                    "2",
                    "get",
                    f"/repositories/{self.slug}/commit/{commit}/statuses",
                    page=page,
                    token=token,
                )
                _statuses = res["values"]
                if len(_statuses) == 0:
                    break
                statuses.extend(
                    [
                        {
                            "time": s["updated_on"],
                            "state": status_keys.get(s["state"]),
                            "description": s["description"],
                            "url": s["url"],
                            "context": s["key"],
                        }
                        for s in _statuses
                    ]
                )
                if not res.get("next"):
                    break
            return Status(statuses)

    async def set_commit_status(
        self,
        commit,
        status,
        context,
        description,
        url,
        merge_commit=None,
        token=None,
        coverage=None,
    ):
        token = self.get_token_by_type_if_none(token, TokenType.status)
        # https://confluence.atlassian.com/bitbucket/buildstatus-resource-779295267.html
        status = {
            "pending": "INPROGRESS",
            "success": "SUCCESSFUL",
            "error": "FAILED",
            "failure": "FAILED",
        }.get(status)
        assert status, "status not valid"
        async with self.get_client() as client:
            try:
                res = await self.api(
                    client,
                    "2",
                    "post",
                    f"/repositories/{self.slug}/commit/{commit}/statuses/build",
                    body={
                        "state": status,
                        "key": "codecov-" + context,
                        "name": context.replace("/", " ").capitalize() + " Coverage",
                        "url": url,
                        "description": description,
                    },
                    token=token,
                )
            except Exception:
                res = await self.api(
                    client,
                    "2",
                    "put",
                    f"/repositories/{self.slug}/commit/{commit}/statuses/build/codecov-{context}",
                    body={
                        "state": status,
                        "name": context.replace("/", " ").capitalize() + " Coverage",
                        "url": url,
                        "description": description,
                    },
                    token=token,
                )

            if merge_commit:
                try:
                    res = await self.api(
                        client,
                        "2",
                        "post",
                        f"/repositories/{self.slug}/commit/{merge_commit[0]}/statuses/build",
                        body={
                            "state": status,
                            "key": "codecov-" + merge_commit[1],
                            "name": merge_commit[1].replace("/", " ").capitalize()
                            + " Coverage",
                            "url": url,
                            "description": description,
                        },
                        token=token,
                    )
                except Exception:
                    res = await self.api(
                        client,
                        "2",
                        "put",
                        f"/repositories/{self.slug}/commit/{merge_commit[0]}/statuses/build/codecov-{context}",
                        body={
                            "state": status,
                            "name": merge_commit[1].replace("/", " ").capitalize()
                            + " Coverage",
                            "url": url,
                            "description": description,
                        },
                        token=token,
                    )
            # check if the commit is a Merge
            return res

    async def get_commit(self, commit, token=None):
        # https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/commit/%7Bnode%7D
        async with self.get_client() as client:
            try:
                data = await self.api(
                    client,
                    "2",
                    "get",
                    f"/repositories/{self.slug}/commit/{commit}",
                    token=token,
                )
            except TorngitClientError as ce:
                if ce.code == 404:
                    raise TorngitObjectNotFoundError(
                        response_data=ce.response_data,
                        message=f"Commit {commit} cannot be found",
                    )
                raise
            username = data["author"].get("user", {}).get("nickname")
            author_raw = (
                data["author"].get("raw", "")[:-1].rsplit(" <", 1)
                if " <" in data["author"].get("raw", "")
                else None
            )

            userid = data["author"].get("user", {}).get("uuid", "")[1:-1] or None

            # We used to look up the userid from the username if no uuid provided but
            # BitBucket has deprecated the '/users/{username}' endpoint for privacy reasons so we
            # have to use '/users/{account_id}' instead
            # https://developer.atlassian.com/bitbucket/api/2/reference/resource/users/%7Busername%7D
            account_id = data["author"].get("user", {}).get("account_id")
            if not userid and account_id:
                res = await self.api(
                    client, "2", "get", f"/users/{account_id}", token=token
                )
                userid = res["uuid"][1:-1]

            return {
                "author": {
                    "id": userid,
                    "username": username,
                    "name": author_raw[0] if author_raw else None,
                    "email": author_raw[1] if author_raw else None,
                },
                "commitid": commit,
                "parents": [p["hash"] for p in data["parents"]],
                "message": data["message"],
                "timestamp": data["date"],
            }

    async def get_branches(self, token=None):
        # https://confluence.atlassian.com/display/BITBUCKET/repository+Resource+1.0#repositoryResource1.0-GETlistofbranches
        async with self.get_client() as client:
            res = await self.api(
                client,
                "2",
                "get",
                f"/repositories/{self.slug}/refs/branches",
                token=token,
                pagelen="100",
            )
            return [(k["name"], k["target"]["hash"]) for k in res["values"]]

    async def get_branch(self, name, token=None):
        async with self.get_client() as client:
            res = await self.api(
                client,
                "2",
                "get",
                f"/repositories/{self.slug}/refs/branches/{name}",
                token=token,
            )
            return {
                "name": res["name"],
                "sha": res["target"]["hash"],
            }

    async def get_pull_requests(self, state="open", token=None):
        state = {"open": "OPEN", "merged": "MERGED", "close": "DECLINED"}.get(state)
        pulls, page = [], 0
        async with self.get_client() as client:
            while True:
                page += 1
                # https://confluence.atlassian.com/display/BITBUCKET/pullrequests+Resource#pullrequestsResource-GETalistofopenpullrequests
                res = await self.api(
                    client,
                    "2",
                    "get",
                    f"/repositories/{self.slug}/pullrequests",
                    state=state,
                    page=page,
                    token=token,
                )
                if len(res["values"]) == 0:
                    break
                pulls.extend([pull["id"] for pull in res["values"]])
                if not res.get("next"):
                    break
            return pulls

    async def find_pull_request(
        self, commit=None, branch=None, state="open", token=None
    ):
        state = {"open": "OPEN", "merged": "MERGED", "close": "DECLINED"}.get(state, "")
        page = 0
        async with self.get_client() as client:
            if commit or branch:
                while True:
                    page += 1
                    # https://confluence.atlassian.com/display/BITBUCKET/pullrequests+Resource#pullrequestsResource-GETalistofopenpullrequests
                    res = await self.api(
                        client,
                        "2",
                        "get",
                        f"/repositories/{self.slug}/pullrequests",
                        state=state,
                        page=page,
                        token=token,
                    )
                    _prs = res["values"]
                    if len(_prs) == 0:
                        break

                    if commit:
                        for pull in _prs:
                            if commit.startswith(pull["source"]["commit"]["hash"]):
                                return str(pull["id"])
                    else:
                        for pull in _prs:
                            if pull["source"]["branch"]["name"] == branch:
                                return str(pull["id"])

                    if not res.get("next"):
                        break

    async def get_pull_request_files(self, pullid, token=None):
        # https://developer.atlassian.com/cloud/bitbucket/rest/api-group-pullrequests/#api-repositories-workspace-repo-slug-pullrequests-pull-request-id-diffstat-get
        async with self.get_client() as client:
            try:
                res = await self.api(
                    client,
                    "2",
                    "get",
                    f"/repositories/{self.slug}/pullrequests/{pullid}/diffstat",
                    token=token,
                )
                filenames = [data["new"]["path"] for data in res.get("values")]
                return filenames
            except TorngitClientError as ce:
                if ce.code == 404:
                    raise TorngitObjectNotFoundError(
                        response_data=ce.response_data,
                        message=f"PR with id {pullid} does not exist",
                    )
                raise

    async def get_repository(self, token=None):
        async with self.get_client() as client:
            if self.data["repo"].get("service_id") is None:
                # https://confluence.atlassian.com/display/BITBUCKET/repository+Resource#repositoryResource-GETarepository
                res = await self.api(
                    client, "2", "get", "/repositories/" + self.slug, token=token
                )
            else:
                res = await self.api(
                    client,
                    "2",
                    "get",
                    "/repositories/%7B{}%7D/%7B{}%7D".format(
                        self.data["owner"]["service_id"],
                        self.data["repo"]["service_id"],
                    ),
                    token=token,
                )
            username, repo = tuple(res["full_name"].split("/", 1))
            return {
                "owner": {
                    "service_id": res["owner"]["uuid"][1:-1],
                    "username": username,
                },
                "repo": {
                    "service_id": res["uuid"][1:-1],
                    "private": res["is_private"],
                    "branch": "main",
                    "language": self._validate_language(res["language"]),
                    "name": repo,
                },
            }

    async def get_repo_languages(
        self, token=None, language: str | None = None
    ) -> list[str]:
        """
        Gets the languages belonging to this repository. Bitbucket has no way to
        track languages, so we'll return a list with the existing language
        Param:
            language: the language belonging to the repository.language key
        Returns:
            List[str]: A list of language names
        """
        languages = []

        if language:
            languages.append(language.lower())

        return languages

    async def get_authenticated(self, token=None):
        async with self.get_client() as client:
            if self.data["repo"].get("private"):
                # Verify user can access the private repo (raises on 4xx/5xx)
                await self.api(
                    client, "2", "get", "/repositories/" + self.slug, token=token
                )
            workspace = self.data["owner"]["username"]
            response = await self.api(
                client,
                "2",
                "get",
                f"/repositories/{workspace}",
                role="contributor",
                q=f'full_name="{self.slug}"',
                token=token,
            )
            can_edit = bool(response.get("values"))
            return (True, can_edit)

    async def get_source(self, path, ref, token=None):
        # https://confluence.atlassian.com/bitbucket/src-resources-296095214.html
        async with self.get_client() as client:
            try:
                src = await self.api(
                    client,
                    "2",
                    "get",
                    "/repositories/{}/src/{}/{}".format(
                        self.slug, ref, path.replace(" ", "%20")
                    ),
                    token=token,
                )
            except TorngitClientError as ce:
                if ce.code == 404:
                    raise TorngitObjectNotFoundError(
                        response_data=ce.response_data,
                        message=f"Path {path} not found at {ref}",
                    )
                raise
            return {"commitid": None, "content": src.encode()}

    async def get_compare(
        self, base, head, context=None, with_commits=True, token=None
    ):
        # https://developer.atlassian.com/bitbucket/api/2/reference/resource/snippets/%7Busername%7D/%7Bencoded_id%7D/%7Brevision%7D/diff%C2%A0%E2%80%A6
        # https://api.bitbucket.org/2.0/repositories/markadams-atl/test-repo/diff/1b03803..fcba34b
        # IMPORANT it is reversed
        async with self.get_client() as client:
            diff = await self.api(
                client,
                "2",
                "get",
                f"/repositories/{self.slug}/diff/{head}..{base}",
                context=context or 1,
                token=token,
            )

        commits = []
        if with_commits:
            commits = [{"commitid": head}, {"commitid": base}]
            # No endpoint to get commits yet... ugh

        return {"diff": self.diff_to_json(diff), "commits": commits}

    async def get_commit_diff(self, commit, context=None, token=None):
        # https://confluence.atlassian.com/bitbucket/diff-resource-425462484.html
        async with self.get_client() as client:
            diff = await self.api(
                client,
                "2",
                "get",
                "/repositories/"
                + self.data["owner"]["username"]
                + "/"
                + self.data["repo"]["name"]
                + "/diff/"
                + commit,
                token=token,
            )
        return self.diff_to_json(diff)

    async def list_top_level_files(self, ref, token=None):
        return await self.list_files(ref, dir_path="", token=None)

    async def list_files(self, ref, dir_path, token=None):
        page = None
        has_more = True
        files = []
        async with self.get_client() as client:
            while has_more:
                # https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Busername%7D/%7Brepo_slug%7D/src#get
                if page is not None:
                    kwargs = {"page": page, "token": token}
                else:
                    kwargs = {"token": token}
                results = await self.api(
                    client,
                    "2",
                    "get",
                    f"/repositories/{self.slug}/src/{ref}/{dir_path}",
                    **kwargs,
                )
                files.extend(results["values"])
                if "next" in results:
                    url = results["next"]
                    parsed = urllib_parse.urlparse(url)
                    page = urllib_parse.parse_qs(parsed.query)["page"][0]
                else:
                    has_more = False
        return [
            {"path": f["path"], "type": self._bitbucket_type_to_torngit_type(f["type"])}
            for f in files
        ]

    def _bitbucket_type_to_torngit_type(self, val):
        if val == "commit_file":
            return "file"
        elif val == "commit_directory":
            return "folder"
        return "other"

    async def get_ancestors_tree(self, commitid, token=None):
        async with self.get_client() as client:
            res = await self.api(
                client,
                "2",
                "get",
                f"/repositories/{self.slug}/commits",
                token=token,
                include=commitid,
            )
        start = res["values"][0]["hash"]
        commit_mapping = {
            val["hash"]: [k["hash"] for k in val["parents"]] for val in res["values"]
        }
        return self.build_tree_from_commits(start, commit_mapping)

    def get_external_endpoint(self, endpoint: Endpoints, **kwargs):
        if endpoint == Endpoints.commit_detail:
            return self.urls["commit"].format(
                username=self.data["owner"]["username"],
                name=self.data["repo"]["name"],
                commitid=kwargs["commitid"],
            )
        raise NotImplementedError()

    async def get_best_effort_branches(self, commit_sha: str, token=None) -> list[str]:
        """
        Gets a 'best effort' list of branches this commit is in.
        If a branch is returned, this means this commit is in that branch. If not, it could still be
            possible that this commit is in that branch
        Args:
            commit_sha (str): The sha of the commit we want to look at
        Returns:
            List[str]: A list of branch names
        """
        return []

    async def is_student(self):
        return False
