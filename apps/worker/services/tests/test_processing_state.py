import pytest

from database.tests.factories.core import (
    CommitFactory,
    ReportFactory,
    RepositoryFactory,
    UploadFactory,
)
from services.processing.state import (
    ProcessingState,
    should_perform_merge,
    should_trigger_postprocessing,
)
from shared.reports.enums import UploadState

@pytest.mark.django_db(databases={"default"})
class TestProcessingState:
    @pytest.fixture
    def setup_commit(self, dbsession):
        repository = RepositoryFactory.create()
        dbsession.add(repository)
        dbsession.flush()
        commit = CommitFactory.create(repository=repository)
        dbsession.add(commit)
        dbsession.flush()
        report = ReportFactory.create(commit=commit)
        dbsession.add(report)
        dbsession.flush()
        return repository, commit, report

    def _create_upload(self, dbsession, report, state_id):
        upload = UploadFactory.create(
            report=report,
            state="uploaded",
            state_id=state_id,
        )
        dbsession.add(upload)
        dbsession.flush()
        return upload

    def test_get_upload_numbers_empty(self, dbsession, setup_commit):
        _, commit, _ = setup_commit
        state = ProcessingState(commit.repoid, commit.commitid, dbsession)
        numbers = state.get_upload_numbers()
        assert numbers.processing == 0
        assert numbers.processed == 0

    def test_get_upload_numbers_with_uploads(self, dbsession, setup_commit):
        _, commit, report = setup_commit
        self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)
        self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)
        self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, dbsession)
        numbers = state.get_upload_numbers()
        assert numbers.processing == 2
        assert numbers.processed == 1

    def test_mark_upload_as_processed(self, dbsession, setup_commit):
        _, commit, report = setup_commit
        upload = self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, dbsession)
        state.mark_upload_as_processed(upload.id_)
        dbsession.flush()

        dbsession.refresh(upload)
        assert upload.state_id == UploadState.PROCESSED.db_id
        # state string is intentionally NOT set here -- the finisher's
        # idempotency check uses state="processed" to detect already-merged uploads
        assert upload.state == "uploaded"

    def test_mark_uploads_as_merged(self, dbsession, setup_commit):
        _, commit, report = setup_commit
        u1 = self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)
        u2 = self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, dbsession)
        state.mark_uploads_as_merged([u1.id_, u2.id_])
        dbsession.flush()

        dbsession.refresh(u1)
        dbsession.refresh(u2)
        assert u1.state_id == UploadState.MERGED.db_id
        assert u1.state == "merged"
        assert u2.state_id == UploadState.MERGED.db_id
        assert u2.state == "merged"

    def test_mark_uploads_as_merged_empty_list(self, dbsession, setup_commit):
        _, commit, _ = setup_commit
        state = ProcessingState(commit.repoid, commit.commitid, dbsession)
        state.mark_uploads_as_merged([])

    def test_get_uploads_for_merging(self, dbsession, setup_commit):
        _, commit, report = setup_commit
        u1 = self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)
        u2 = self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)
        self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)
        self._create_upload(dbsession, report, UploadState.MERGED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, dbsession)
        merging = state.get_uploads_for_merging()
        assert merging == {u1.id_, u2.id_}

    def test_get_uploads_for_merging_respects_batch_size(self, dbsession, setup_commit):
        _, commit, report = setup_commit
        for _ in range(15):
            self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, dbsession)
        merging = state.get_uploads_for_merging()
        assert len(merging) == 10

    def test_mark_uploads_as_processing_is_noop(self, dbsession, setup_commit):
        _, commit, report = setup_commit
        upload = self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, dbsession)
        state.mark_uploads_as_processing([upload.id_])

        dbsession.refresh(upload)
        assert upload.state_id == UploadState.UPLOADED.db_id

    def test_clear_in_progress_uploads_sets_error_on_uploaded(
        self, dbsession, setup_commit
    ):
        """Uploaded uploads are set to ERROR so they stop counting as 'processing'."""
        _, commit, report = setup_commit
        upload = self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, dbsession)
        state.clear_in_progress_uploads([upload.id_])

        dbsession.refresh(upload)
        assert upload.state_id == UploadState.ERROR.db_id
        assert upload.state == "error"

    def test_clear_in_progress_uploads_skips_processed(self, dbsession, setup_commit):
        """Already-processed uploads are not affected (success path in finally block)."""
        _, commit, report = setup_commit
        upload = self._create_upload(dbsession, report, UploadState.PROCESSED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, dbsession)
        state.clear_in_progress_uploads([upload.id_])

        dbsession.refresh(upload)
        assert upload.state_id == UploadState.PROCESSED.db_id

    def test_clear_in_progress_uploads_empty_list(self, dbsession, setup_commit):
        _, commit, _ = setup_commit
        state = ProcessingState(commit.repoid, commit.commitid, dbsession)
        state.clear_in_progress_uploads([])

    def test_full_lifecycle(self, dbsession, setup_commit):
        """End-to-end: UPLOADED -> PROCESSED -> MERGED with DB state."""
        _, commit, report = setup_commit
        u1 = self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)
        u2 = self._create_upload(dbsession, report, UploadState.UPLOADED.db_id)

        state = ProcessingState(commit.repoid, commit.commitid, dbsession)

        numbers = state.get_upload_numbers()
        assert numbers.processing == 2
        assert numbers.processed == 0

        state.mark_upload_as_processed(u1.id_)
        dbsession.flush()

        numbers = state.get_upload_numbers()
        assert numbers.processing == 1
        assert numbers.processed == 1
        assert not should_perform_merge(numbers)

        state.mark_upload_as_processed(u2.id_)
        dbsession.flush()

        numbers = state.get_upload_numbers()
        assert numbers.processing == 0
        assert numbers.processed == 2
        assert should_perform_merge(numbers)

        merging = state.get_uploads_for_merging()
        assert merging == {u1.id_, u2.id_}

        state.mark_uploads_as_merged(list(merging))
        dbsession.flush()

        numbers = state.get_upload_numbers()
        assert numbers.processing == 0
        assert numbers.processed == 0
        assert should_trigger_postprocessing(numbers)
