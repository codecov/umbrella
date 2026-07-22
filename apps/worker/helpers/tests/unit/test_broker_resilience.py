import json
from types import SimpleNamespace

import pytest
from kombu.transport import redis as kombu_redis

from helpers import broker_resilience
from helpers.broker_resilience import install_broker_resilience


@pytest.fixture
def restore_channel():
    """Snapshot and restore `Channel._brpop_read` + the idempotency flag
    so patching the shared kombu class doesn't leak across tests.
    """
    channel = kombu_redis.Channel
    orig = channel.__dict__.get("_brpop_read", channel._brpop_read)
    had_flag = "_codecov_resilient_brpop" in channel.__dict__
    broker_resilience._drops.count = 0
    yield channel
    channel._brpop_read = orig
    if not had_flag and "_codecov_resilient_brpop" in channel.__dict__:
        del channel._codecov_resilient_brpop
    broker_resilience._drops.count = 0


def test_drops_undecodable_message_and_continues(restore_channel):
    channel = restore_channel
    channel._brpop_read = lambda self, **o: (_ for _ in ()).throw(
        json.JSONDecodeError("Expecting value", "", 0)
    )
    if "_codecov_resilient_brpop" in channel.__dict__:
        del channel._codecov_resilient_brpop

    assert install_broker_resilience() is True
    # A decode failure is swallowed (returns None) instead of propagating
    # out of the consumer loop and crashing the worker.
    assert channel._brpop_read(SimpleNamespace()) is None
    assert broker_resilience._drops.count == 1


def test_passes_through_good_message(restore_channel):
    channel = restore_channel
    channel._brpop_read = lambda self, **o: "delivered"
    if "_codecov_resilient_brpop" in channel.__dict__:
        del channel._codecov_resilient_brpop

    install_broker_resilience()
    assert channel._brpop_read(SimpleNamespace()) == "delivered"
    assert broker_resilience._drops.count == 0


def test_does_not_swallow_non_decode_errors(restore_channel):
    channel = restore_channel

    class Boom(Exception):
        pass

    channel._brpop_read = lambda self, **o: (_ for _ in ()).throw(Boom())
    if "_codecov_resilient_brpop" in channel.__dict__:
        del channel._codecov_resilient_brpop

    install_broker_resilience()
    # Connection/routing errors (anything outside the decode family) must
    # keep propagating so reconnect / real failures aren't masked.
    with pytest.raises(Boom):
        channel._brpop_read(SimpleNamespace())


def test_install_is_idempotent(restore_channel):
    channel = restore_channel
    if "_codecov_resilient_brpop" in channel.__dict__:
        del channel._codecov_resilient_brpop
    assert install_broker_resilience() is True
    assert install_broker_resilience() is False
