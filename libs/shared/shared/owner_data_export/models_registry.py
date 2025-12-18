from django.apps import apps

"""
Registry of models for owner data export.

Models are listed in dependency order - parents before children.
This ensures UPSERT imports don't violate foreign key constraints.
"""

EXPORTABLE_MODELS = [
    # Root models
    "codecov_auth.Owner",
    "codecov_auth.User",
    # Models depending on Owner
    "codecov_auth.OwnerProfile",
    "codecov_auth.Session",
    "codecov_auth.OrganizationLevelToken",
    "codecov_auth.UserToken",
    "core.Repository",
    # Models depending on Repository
    "core.Branch",
    "core.Commit",
    "core.Pull",
    "codecov_auth.RepositoryToken",
    "reports.RepositoryFlag",
    # Models depending on Commit
    "core.CommitNotification",
    "core.CommitError",
    "reports.CommitReport",
    "compare.CommitComparison",
    # Models depending on CommitReport
    "reports.ReportResults",
    "reports.ReportLevelTotals",
    "reports.ReportSession",
    # Models depending on ReportSession
    "reports.UploadError",
    "reports.UploadLevelTotals",
    "reports.UploadFlagMembership",
    # Models depending on CommitComparison
    "compare.FlagComparison",
    "compare.ComponentComparison",
]

# Models exported to TimescaleDB - separate SQL file
TIMESCALE_MODELS = [
    "timeseries.Dataset",
    "timeseries.Measurement",
    "ta_timeseries.Testrun",
]

# Models excluded from export
EXCLUDED_MODELS = [
    # Unnecessary, sensitive data
    "codecov_auth.OwnerExport",
    # Test Analytics models
    "test_analytics.Flake",
    "test_analytics.TAUpload",
    # Static analysis models
    "staticanalysis.StaticAnalysisSuite",
    "staticanalysis.StaticAnalysisSingleFileSnapshot",
    "staticanalysis.StaticAnalysisSuiteFilepath",
    # Label analysis models
    "labelanalysis.LabelAnalysisRequest",
    "labelanalysis.LabelAnalysisProcessingError",
    # Internal/config
    "rollouts.FeatureFlag",
    "rollouts.FeatureFlagVariant",
    "codecov_metrics.UserOnboardingLifeCycleMetrics",
    "user_measurements.UserMeasurement",
    "upload_breadcrumbs.UploadBreadcrumb",
    "bundle_analysis.CacheConfig",
    # Internal cleanup tracking
    "codecov_auth.OwnerToBeDeleted",
    "codecov_auth.OwnerInstallationNameToUseForTask",
    # System tables
    "core.Version",
    "core.Constants",
    # Account/billing
    "codecov_auth.Account",
    "codecov_auth.AccountsUsers",
    "codecov_auth.StripeBilling",
    "codecov_auth.InvoiceBilling",
    "codecov_auth.Plan",
    "codecov_auth.Tier",
    "codecov_auth.SentryUser",
    "codecov_auth.OktaUser",
    "codecov_auth.OktaSettings",
    # GitHub App - this will be a new app
    "codecov_auth.GithubAppInstallation",
]

# Fields to completely exclude from export
EXCLUDED_FIELDS: dict[str, list[str]] = {
    # Token values - must regenerate
    "codecov_auth.UserToken": ["token"],
    "codecov_auth.RepositoryToken": ["key"],
    "codecov_auth.OrganizationLevelToken": ["token"],
    "codecov_auth.Session": ["token"],
    # Billing IDs
    "codecov_auth.Owner": ["stripe_customer_id", "stripe_subscription_id"],
}

# Fields to nullify
NULLIFIED_FIELDS: dict[str, list[str]] = {
    # OAuth tokens
    "codecov_auth.Owner": ["oauth_token"],
}

# Models that require explicit opt-in due to security sensitivity
# These are excluded by default but can be included if requested
SENSITIVE_MODELS = [
    "codecov_auth.UserToken",
    "codecov_auth.RepositoryToken",
    "codecov_auth.OrganizationLevelToken",
    "codecov_auth.Session",
]


def get_model_class(model_path: str):
    """
    Get Django model class from 'app_label.ModelName' string.
    Example: get_model_class("core.Repository") -> Repository
    """
    app_label, model_name = model_path.split(".")
    return apps.get_model(app_label, model_name)


def get_excluded_fields(model_path: str) -> list[str]:
    """Get fields to exclude from export for a model."""
    return EXCLUDED_FIELDS.get(model_path, [])


def get_nullified_fields(model_path: str) -> list[str]:
    """Get fields to nullify in export for a model."""
    return NULLIFIED_FIELDS.get(model_path, [])
