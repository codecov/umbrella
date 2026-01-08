import uuid

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
    "core.Repository",
    # Models depending on Repository
    "core.Branch",
    "core.Commit",
    "core.Pull",
    "reports.RepositoryFlag",
    # Models depending on Commit
    "core.CommitError",
    "reports.CommitReport",
    # Models depending on CommitReport
    "reports.ReportResults",
    "reports.ReportLevelTotals",
    "reports.ReportSession",
    # Models depending on ReportSession
    "reports.UploadError",
    "reports.UploadLevelTotals",
    "reports.UploadFlagMembership",
]

# Models exported to TimescaleDB - separate SQL file
TIMESCALE_MODELS = [
    "timeseries.Dataset",
    "timeseries.Measurement",
]

# Models excluded from export
EXCLUDED_MODELS = [
    # Unnecessary, sensitive data
    "codecov_auth.OwnerExport",
    # Test Analytics models
    "test_analytics.Flake",
    "test_analytics.TAUpload",
    "test_analytics.TAPullComment",
    "ta_timeseries.Testrun",
    "ta_timeseries.AggregateHourly",
    "ta_timeseries.AggregateDaily",
    "ta_timeseries.BranchAggregateHourly",
    "ta_timeseries.BranchAggregateDaily",
    "ta_timeseries.TestAggregateHourly",
    "ta_timeseries.TestAggregateDaily",
    "ta_timeseries.BranchTestAggregateHourly",
    "ta_timeseries.BranchTestAggregateDaily",
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
    # BA - can likely be reinstated when self-hosting
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
    "codecov_auth.OktaUser",
    "codecov_auth.StripeBilling",
    "codecov_auth.InvoiceBilling",
    "codecov_auth.SentryUser",
    "codecov_auth.OktaSettings",
    # Can be ignored when app is run in self-hosted mode
    "codecov_auth.Plan",
    "codecov_auth.Tier",
    # Comparison models can be recreated during runtime
    "compare.CommitComparison",
    "compare.FlagComparison",
    "compare.ComponentComparison",
    # GitHub App - this will be a new app
    "codecov_auth.GithubAppInstallation",
    # Tokens/sessions - must be regenerated
    "codecov_auth.Session",
    "codecov_auth.OrganizationLevelToken",
    "codecov_auth.UserToken",
    "codecov_auth.RepositoryToken",
    # Data doesn't make sense within self-hosted mode
    "core.CommitNotification",
]

# Fields to nullify (must have null=True in model definition)
NULLIFIED_FIELDS: dict[str, list[str]] = {
    # OAuth tokens and billing info
    "codecov_auth.Owner": [
        "oauth_token",
        "stripe_customer_id",
        "stripe_subscription_id",
        "stripe_coupon_id",
        "plan",
        "plan_provider",
        "plan_user_count",
        "plan_auto_activate",
        "trial_start_date",
        "trial_end_date",
        "trial_status",
        "trial_fired_by",
        "pretrial_users_count",
        "invoice_details",
        "delinquent",
        "organizations",
        "admins",
        "integration_id",
        "permission",
        "sentry_user_id",
        "sentry_user_data",
        "account_id",
        "user_id",
    ],
    "core.Repository": [
        "using_integration",
        "webhook_secret",
        "image_token",
    ],
}

# Fields with NOT NULL constraints that need default values instead of null
DEFAULT_FIELDS: dict[str, dict[str, any]] = {
    "codecov_auth.Owner": {
        "uses_invoice": False,
    },
    "core.Repository": {
        "upload_token": uuid.uuid4,
    },
}


def get_model_class(model_path: str):
    """
    Get Django model class from 'app_label.ModelName' string.
    Example: get_model_class("core.Repository") -> Repository
    """
    app_label, model_name = model_path.split(".")
    return apps.get_model(app_label, model_name)


def get_nullified_fields(model_path: str) -> list[str]:
    """Get fields to nullify (set to NULL) in export for a model."""
    return list(NULLIFIED_FIELDS.get(model_path, []))


def get_default_fields(model_path: str) -> dict[str, any]:
    """
    Get fields that need default values in export for a model.
    These are sensitive fields with NOT NULL constraints.
    Values can be direct values or callables (factories) that generate values.
    """
    defaults = DEFAULT_FIELDS.get(model_path, {})
    return {
        field: value() if callable(value) else value
        for field, value in defaults.items()
    }
