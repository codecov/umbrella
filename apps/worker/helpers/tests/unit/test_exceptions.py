from helpers.exceptions import ReportEmptyError


class TestReportEmptyError:
    def test_default_message(self):
        """Test default message when no details provided."""
        error = ReportEmptyError()
        assert str(error) == "No coverage files found in upload."
        assert error.empty_files == []
        assert error.total_files == 0

    def test_with_archive_path(self):
        """Test that archive_path is stored."""
        error = ReportEmptyError(archive_path="/path/to/archive")
        assert error.archive_path == "/path/to/archive"

    def test_all_files_empty(self):
        """When all files are empty, use simplified message."""
        error = ReportEmptyError(
            empty_files=["file1.xml", "file2.xml"],
            total_files=2,
        )
        assert str(error) == "No coverage files found in upload."
        assert error.empty_files == ["file1.xml", "file2.xml"]
        assert error.total_files == 2

    def test_some_files_empty(self):
        """When some files are empty, list them."""
        error = ReportEmptyError(
            empty_files=["empty1.xml", "empty2.xml"],
            total_files=5,
        )
        assert "2 of 5 file(s) were empty" in str(error)
        assert "empty1.xml" in str(error)
        assert "empty2.xml" in str(error)

    def test_many_empty_files_truncated(self):
        """When more than 5 files empty, truncate the list."""
        empty_files = [f"file{i}.xml" for i in range(10)]
        error = ReportEmptyError(
            empty_files=empty_files,
            total_files=15,
        )
        message = str(error)
        assert "10 of 15 file(s) were empty" in message
        # First 5 files should be listed
        assert "file0.xml" in message
        assert "file4.xml" in message
        # 6th file should not be in the list
        assert "file5.xml" not in message
        # Should indicate there are more
        assert "(and 5 more)" in message
