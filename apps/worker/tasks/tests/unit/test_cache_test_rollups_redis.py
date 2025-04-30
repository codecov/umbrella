import shared.storage
from shared.django_apps.core.tests.factories import RepositoryFactory
from shared.helpers.redis import get_redis_connection
from shared.storage.exceptions import BucketAlreadyExistsError
from tasks.cache_test_rollups_redis import CacheTestRollupsRedisTask


def test_cache_test_rollups(mock_storage, db):
    repo = RepositoryFactory()

    redis = get_redis_connection()
    storage_service = shared.storage.get_appropriate_storage_service(repo.repoid)
    storage_key = f"test_results/rollups/{repo.repoid}/main/1"
    try:
        storage_service.create_root_storage("archive")
    except BucketAlreadyExistsError:
        pass

    storage_service.write_file("archive", storage_key, b"hello world")

    task = CacheTestRollupsRedisTask()
    result = task.run_impl(_db_session=None, repoid=repo.repoid, branch="main")
    assert result == {"success": True}

    redis_key = f"ta_roll:{repo.repoid}:main:1"

    assert redis.get(redis_key) == storage_service.read_file("archive", storage_key)
