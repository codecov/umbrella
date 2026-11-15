import sentry_sdk

from graphql_api.types.enums import PathContentDisplayType
from services.path import Dir, File


@sentry_sdk.trace
def sort_path_contents(items: list[File | Dir], filters: dict = {}) -> list[File | Dir]:
    filter_parameter = filters.get("ordering", {}).get("parameter")
    filter_direction = filters.get("ordering", {}).get("direction")
    # Support deprecated flat `orderingDirection` field for backward compatibility
    if not filter_direction and filters.get("ordering_direction"):
        filter_direction = filters.get("ordering_direction")
        if not filter_parameter:
            from graphql_api.types.enums import OrderingParameter
            filter_parameter = OrderingParameter.NAME

    if filter_parameter and filter_direction:
        parameter_value = filter_parameter.value
        direction_value = filter_direction.value
        items.sort(
            key=lambda item: getattr(item, parameter_value),
            reverse=direction_value == "descending",
        )
        display_type = filters.get("display_type", {})
        if (
            parameter_value == "name"
            and display_type is not PathContentDisplayType.LIST
        ):
            items.sort(key=lambda item: isinstance(item, File))

    return items
