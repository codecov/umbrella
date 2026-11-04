from ariadne import ObjectType

from graphql_api.helpers.ariadne import ariadne_load_local_graphql

notification = ariadne_load_local_graphql(__file__, "notification.graphql")
notification_bindable = ObjectType("Notification")


@notification_bindable.field("notificationType")
def resolve_notification_type(commit_notification, info):
    return commit_notification.notification_type


@notification_bindable.field("state")
def resolve_state(commit_notification, info):
    return commit_notification.state


@notification_bindable.field("decorationType")
def resolve_decoration_type(commit_notification, info):
    return commit_notification.decoration_type


@notification_bindable.field("createdAt")
def resolve_created_at(commit_notification, info):
    return commit_notification.created_at


@notification_bindable.field("updatedAt")
def resolve_updated_at(commit_notification, info):
    return commit_notification.updated_at