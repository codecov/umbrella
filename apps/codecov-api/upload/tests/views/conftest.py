import pytest


@pytest.fixture
def mock_prometheus_metrics(mocker):
    return mocker.patch("upload.metrics.API_UPLOAD_COUNTER.labels")


@pytest.fixture
def mock_bundle_analysis_metrics(mocker):
    return mocker.patch(
        "upload.views.bundle_analysis.BUNDLE_ANALYSIS_UPLOAD_VIEWS_COUNTER.labels"
    )
