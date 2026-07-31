from ariadne import UnionType

from codecov_auth.commands.owner import OwnerCommands
from graphql_api.helpers.mutation import (
    require_authenticated,
    resolve_union_error_type,
    wrap_error_handling_mutation,
)


def _camel_to_snake(name):
    import re

    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


@wrap_error_handling_mutation
@require_authenticated
async def resolve_save_terms_agreement(_, info, input=None, **kwargs):
    # Support deprecated flat argument style: saveTermsAgreement(termsAgreement: true)
    if input is None:
        input = {
            _camel_to_snake(k): v for k, v in kwargs.items() if v is not None
        }
    command: OwnerCommands = info.context["executor"].get_command("owner")
    return await command.save_terms_agreement(input)


error_save_terms_agreement = UnionType("SaveTermsAgreementError")
error_save_terms_agreement.type_resolver(resolve_union_error_type)
