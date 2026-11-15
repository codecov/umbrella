from ariadne import ObjectType

session_bindable = ObjectType("Session")


@session_bindable.field("lastFour")
def resolve_last_four(session, _) -> str:
    return str(session.token)[-4:]


@session_bindable.field("lastSeen")
def resolve_last_seen(session, _):
    return session.lastseen


@session_bindable.field("userAgent")
def resolve_user_agent(session, _):
    return session.useragent
