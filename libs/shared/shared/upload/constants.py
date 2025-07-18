from enum import StrEnum

ci = {
    "travis": {
        "title": "Travis-CI",
        "icon": "travis",
        "require_token_when_public": False,
        "instructions": "travis",
        "build_url": "https://travis-ci.com/{owner.username}/{repo.name}/jobs/{upload.job_code}",
    },
    "azure_pipelines": {
        "title": "Azure",
        "icon": "azure_pipelines",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,
    },
    "docker": {
        "title": "Docker",
        "icon": "custom",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,
    },
    "buildbot": {
        "title": "Buildbot",
        "icon": "buildbot",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,
    },
    "cirrus-ci": {
        "title": "Cirrus CI",
        "icon": "cirrus-ci",
        "require_token_when_public": False,
        "instructions": "generic",
        "build_url": "https://cirrus-ci.com/build/{upload.build_code}",
    },
    "codebuild": {
        "title": "AWS Codebuild",
        "icon": "codebuild",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,
    },
    "codefresh": {
        "title": "Codefresh",
        "icon": "custom",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": "https://g.codefresh.io/repositories/{owner.username}/{repo.name}/builds/{upload.build_code}",
    },
    "bitbucket": {
        "title": "Bitbucket Pipelines",
        "icon": "bitbucket",
        "require_token_when_public": False,
        "instructions": "generic",
        "build_url": "https://bitbucket.org/{owner.username}/{repo.name}/addon/pipelines/home#!/results/{upload.job_code}",
    },
    "circleci": {
        "title": "CircleCI",
        "icon": "circleci",
        "require_token_when_public": False,
        "instructions": "circleci",
        "build_url": "https://circleci.com/{service_short}/{owner.username}/{repo.name}/{upload.build_code}#tests/containers/{upload.job_code}",
    },
    "buddybuild": {
        "title": "buddybuild",
        "icon": "custom",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,
    },
    "buddy": {
        "title": "buddy",
        "icon": "custom",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,
    },
    "github-actions": {
        "title": "GitHub Actions",
        "icon": "github-actions",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,
    },
    "solano": {
        "title": "Solano",
        "icon": "custom",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,
    },
    "teamcity": {
        "title": "TeamCity",
        "icon": "teamcity",
        "require_token_when_public": True,
        "instructions": "teamcity",
        "build_url": None,
    },
    "appveyor": {
        "title": "AppVeyor",
        "icon": "appveyor",
        "require_token_when_public": False,
        "instructions": "appveyor",
        "build_url": None,
    },
    "wercker": {
        "title": "Wercker",
        "icon": "wercker",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": "https://app.wercker.com/#build/{upload.build_code}",
    },
    "shippable": {
        "title": "Shippable",
        "icon": "shippable",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,  # provided in upload,
    },
    "codeship": {
        "title": "Codeship",
        "icon": "codeship",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,  # provided in upload,
    },
    "drone.io": {
        "title": "Drone.io",
        "icon": "drone.io",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,  # provided in upload,
    },
    "jenkins": {
        "title": "Jenkins",
        "icon": "jenkins",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,  # provided in upload,
    },
    "semaphore": {
        "title": "Semaphore",
        "icon": "semaphore",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": "https://semaphoreapp.com/{owner.username}/{repo.name}/branches/{commit.branch}/builds/{upload.build_code}",
    },
    "gitlab": {
        "title": "GitLab CI",
        "icon": "gitlab",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": "https://gitlab.com/{owner.username}/{repo.name}/builds/{upload.build_code}",
    },
    "bamboo": {
        "title": "Bamboo",
        "icon": "bamboo",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,
    },
    "buildkite": {
        "title": "BuildKite",
        "icon": "buildkite",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,  # provided in upload,
    },
    "bitrise": {
        "title": "Bitrise",
        "icon": "bitrise",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,  # provided in upload,
    },
    "greenhouse": {
        "title": "Greenhouse",
        "icon": "greenhouse",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,
    },
    "heroku": {
        "title": "Heroku",
        "icon": "heroku",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,
    },
    "woodpecker": {
        "title": "WoodpeckerCI",
        "icon": "custom",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,  # provided in upload,
    },
    "custom": {
        "title": "Custom",
        "icon": "custom",
        "require_token_when_public": True,
        "instructions": "generic",
        "build_url": None,  # provided in upload,
    },
}

errors = {
    "travis": {
        "tokenless-general-error": "\nERROR: Tokenless uploads are only supported for public repositories on Travis that can be verified through the Travis API. Please use an upload token if your repository is private and specify it via the -t flag. You can find the token for this repository at the url below on codecov.io (login required):\n\nRepo token: {}\nDocumentation: https://docs.codecov.io/docs/about-the-codecov-bash-uploader#section-upload-token",
        "tokenless-stale-build": "\nERROR: The coverage upload was rejected because the build is out of date. Please make sure the build is not stale for uploads to process correctly.",
        "tokenless-bad-status": "\nERROR: The build status does not indicate that the current build is in progress. Please make sure the build is in progress or was finished within the past 4 minutes to ensure reports upload properly.",
    }
}

global_upload_token_providers = [
    "github",
    "github_enterprise",
    "gitlab",
    "gitlab_enterprise",
    "bitbucket",
    "bitbucket_server",
]


class UploadErrorCode(StrEnum):
    FILE_NOT_IN_STORAGE = "file_not_in_storage"
    REPORT_EXPIRED = "report_expired"
    REPORT_EMPTY = "report_empty"
    PROCESSING_TIMEOUT = "processing_timeout"
    UNSUPPORTED_FILE_FORMAT = "unsupported_file_format"

    # We don't want these - try to add error cases when they arise
    UNKNOWN_PROCESSING = "unknown_processing"
    UNKNOWN_STORAGE = "unknown_storage"

    UNKNOWN_ERROR_CODE = "unknown_error_code"
