import logging
from collections.abc import Mapping, Sequence
from typing import Any

from shared.torngit.base import TorngitBaseAdapter
from shared.torngit.exceptions import TorngitObjectNotFoundError

log = logging.getLogger(__name__)


async def fetch_current_yaml_from_provider_via_reference(
    ref: str, repository_service: TorngitBaseAdapter
) -> str | None:
    repoid = repository_service.data["repo"]["repoid"]
    location = await determine_commit_yaml_location(ref, repository_service)
    if not location:
        log.info(
            "We were not able to find the yaml on the provider API",
            extra={"commit": ref, "repoid": repoid},
        )
        return None
    log.info(
        "Yaml was found on provider API",
        extra={"commit": ref, "repoid": repoid, "location": location},
    )
    try:
        content = await repository_service.get_source(location, ref)
        return content["content"]
    except TorngitObjectNotFoundError:
        log.exception(
            "File not in %s for commit", extra={"commit": ref, "location": location}
        )
        return None


async def determine_commit_yaml_location(
    ref: str, repository_service: TorngitBaseAdapter
) -> str | None:
    """
        Determines where in `ref` the codecov.yaml is, in a given repository
        We currently look for the yaml in two different kinds of places
            - Root level of the repository
            - Specific folders that we know some customers use:
                - `dev`
                - `.github`
    Args:
        ref (str): The ref. Could be a branch name, tag, commit sha.
        repository_service (torngit.base.TorngitBaseAdapter): The torngit handler that can fetch this data.
            Indirectly determines the repository
    Returns:
        str | None: The path of the codecov.yaml file we found. Or `None,` if not found
    """
    possible_locations = [
        "codecov.yml",
        ".codecov.yml",
        "codecov.yaml",
        ".codecov.yaml",
    ]
    acceptable_folders = {"dev", ".github"}
    top_level_files = await repository_service.list_top_level_files(ref)
    top_level_yaml = _search_among_files(possible_locations, top_level_files)
    if top_level_yaml is not None:
        return top_level_yaml
    all_folders = {f["path"] for f in top_level_files if f["type"] == "folder"}
    possible_folders = all_folders & acceptable_folders
    for folder in possible_folders:
        files_inside_folder = await repository_service.list_files(ref, folder)
        yaml_inside_folder = _search_among_files(
            possible_locations, files_inside_folder
        )
        if yaml_inside_folder:
            return yaml_inside_folder
    return None


def _search_among_files(
    desired_filenames: Sequence[str], all_files: Sequence[Mapping[str, Any]]
) -> str | None:
    for file in all_files:
        file_path = file.get("path")
        if file.get("name") in desired_filenames or (
            isinstance(file_path, str) and file_path.split("/")[-1] in desired_filenames
        ):
            return file["path"]
    return None
