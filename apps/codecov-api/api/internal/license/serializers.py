from rest_framework import serializers


class LicenseSerializer(serializers.Serializer):
    trial = serializers.BooleanField(source="is_trial")
    url = serializers.CharField(allow_null=True)
    users = serializers.IntegerField(source="number_allowed_users", allow_null=True)
    repos = serializers.IntegerField(source="number_allowed_repos", allow_null=True)
    expires_at = serializers.DateTimeField(source="expires", allow_null=True)
    pr_billing = serializers.BooleanField(source="is_pr_billing")
