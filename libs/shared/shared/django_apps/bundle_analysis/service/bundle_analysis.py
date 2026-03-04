from shared.django_apps.bundle_analysis.models import CacheConfig


class BundleAnalysisCacheConfigService:
    @staticmethod
    def update_cache_option(repo_id: int, name: str, is_caching: bool = True) -> None:
        CacheConfig.objects.update_or_create(
            repo_id=repo_id, bundle_name=name, defaults={"is_caching": is_caching}
        )

    @staticmethod
    def create_if_not_exists(repo_id: int, name: str, is_caching: bool = True) -> None:
        CacheConfig.objects.get_or_create(
            repo_id=repo_id, bundle_name=name, defaults={"is_caching": is_caching}
        )

    @staticmethod
    def bulk_create_if_not_exists(
        repo_id: int, bundle_names: list[str], is_caching: bool = True
    ) -> None:
        """
        Efficiently creates CacheConfig records for multiple bundles at once,
        avoiding N+1 queries. Only creates records that don't already exist.
        """
        if not bundle_names:
            return

        existing_bundle_names = set(
            CacheConfig.objects.filter(
                repo_id=repo_id, bundle_name__in=bundle_names
            ).values_list("bundle_name", flat=True)
        )

        new_bundle_names = set(bundle_names) - existing_bundle_names
        if new_bundle_names:
            CacheConfig.objects.bulk_create(
                [
                    CacheConfig(
                        repo_id=repo_id, bundle_name=name, is_caching=is_caching
                    )
                    for name in new_bundle_names
                ],
                ignore_conflicts=True,
            )

    @staticmethod
    def get_cache_option(repo_id: int, name: str) -> bool:
        cache_option = CacheConfig.objects.filter(
            repo_id=repo_id, bundle_name=name
        ).first()
        return cache_option.is_caching if cache_option else False
