import pytest
from django.test import TestCase

from codecov.commands.exceptions import Unauthenticated
from shared.django_apps.core.tests.factories import OwnerFactory

from ..regenerate_support_pin import RegenerateSupportPinInteractor


class RegenerateSupportPinInteractorTest(TestCase):
    def setUp(self):
        self.owner = OwnerFactory(support_pin="000000")

    def execute(self, current_owner):
        return RegenerateSupportPinInteractor(current_owner, "github").execute()

    async def test_when_unauthenticated_raise(self):
        with pytest.raises(Unauthenticated):
            await self.execute(current_owner=None)

    async def test_regenerate_support_pin(self):
        owner = await self.execute(current_owner=self.owner)

        assert owner.support_pin is not None
        assert owner.support_pin != "000000"
        assert len(owner.support_pin) == 6
        assert owner.support_pin.isdigit()

        await self.owner.arefresh_from_db()
        assert self.owner.support_pin == owner.support_pin
