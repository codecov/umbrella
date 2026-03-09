from shared.reports.enums import UploadState, UploadType


def test_enums():
    assert UploadState.choices() == (
        (1, "UPLOADED"),
        (2, "PROCESSED"),
        (3, "ERROR"),
        (4, "FULLY_OVERWRITTEN"),
        (5, "PARTIALLY_OVERWRITTEN"),
        (6, "MERGED"),
    )
    assert UploadType.choices() == ((1, "UPLOADED"), (2, "CARRIEDFORWARD"))
