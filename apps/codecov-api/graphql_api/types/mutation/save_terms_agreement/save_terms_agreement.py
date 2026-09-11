from ariadne import UnionType

from codecov_auth.commands.owner import OwnerCommands
from graphql_api.helpers.mutation import (
    require_authenticated,
    resolve_union_error_type,
    wrap_error_handling_mutation,
)


@wrap_error_handling_mutation
@require_authenticated
async def resolve_save_terms_agreement(_, info, input=None, **kwargs):
    # Support legacy call style: saveTermsAgreement(termsAgreement: true)
    if input is None:
        filtered = {k: v for k, v in kwargs.items() if v is not None}
        if not filtered:
            raise ValueError(
                "Either 'input' or 'termsAgreement' argument must be provided."
            )
        input = filtered
    command: OwnerCommands = info.context["executor"].get_command("owner")
    return await command.save_terms_agreement(input)


error_save_terms_agreement = UnionType("SaveTermsAgreementError")
error_save_terms_agreement.type_resolver(resolve_union_error_type)
