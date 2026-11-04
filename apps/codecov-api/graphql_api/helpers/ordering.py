from graphql_api.types.enums import OrderingDirection, RepositoryOrdering


def parse_repository_ordering(
    ordering: str | RepositoryOrdering | None,
    ordering_direction: OrderingDirection | None,
) -> tuple[RepositoryOrdering, OrderingDirection]:
    """
    Coerce the ordering argument to a (RepositoryOrdering, OrderingDirection) pair.

    Accepts:
    - A ``RepositoryOrdering`` enum instance (already resolved by Ariadne).
    - A bare enum name string, e.g. ``"NAME"``.
    - A REST-style dash-prefixed string, e.g. ``"-name"``, which implies
      descending direction.  When a dash prefix is present the caller's
      explicit ``orderingDirection`` is **ignored** in favour of DESC so that
      the combined REST convention is honoured unambiguously.
    """
    direction = ordering_direction if ordering_direction is not None else OrderingDirection.ASC

    if ordering is None:
        return RepositoryOrdering.ID, direction

    # Already resolved to the correct enum type by Ariadne (normal path).
    if isinstance(ordering, RepositoryOrdering):
        return ordering, direction

    # String input — handle REST-style dash prefix.
    assert isinstance(ordering, str)
    if ordering.startswith("-"):
        field = ordering[1:].upper()
        direction = OrderingDirection.DESC
    else:
        field = ordering.upper()

    try:
        return RepositoryOrdering[field], direction
    except KeyError:
        valid = ", ".join(e.name for e in RepositoryOrdering)
        raise ValueError(
            f"Invalid ordering value '{ordering}'. Valid values are: {valid}"
        )
