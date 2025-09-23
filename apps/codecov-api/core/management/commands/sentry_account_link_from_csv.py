import csv
import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from codecov_auth.models import Account
from shared.django_apps.codecov_auth.models import GithubAppInstallation, Owner, Service
from shared.plan.constants import PlanName

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Link Sentry organizations to Codecov organizations from CSV file"

    def add_arguments(self, parser):
        parser.add_argument(
            "csv_file",
            type=str,
            help="The path to the CSV file containing Sentry account linking data",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run in dry-run mode without making any database changes",
        )

    def handle(self, *args, **kwargs):
        csv_file_path = kwargs["csv_file"]
        dry_run = kwargs.get("dry_run", False)

        if dry_run:
            self.stdout.write(
                self.style.WARNING("Running in DRY-RUN mode - no changes will be made")
            )

        try:
            with open(csv_file_path, newline="") as csvfile:
                reader = csv.DictReader(csvfile)

                # Validate CSV headers
                required_columns = {
                    "sentry_org_name",
                    "sentry_org_id",
                    "slug",
                    "installation_id",
                    "service_id",
                }

                fieldnames = reader.fieldnames or []
                if not required_columns.issubset(set(fieldnames)):
                    missing_columns = required_columns - set(fieldnames)
                    self.stdout.write(
                        self.style.ERROR(
                            f"Missing required columns: {', '.join(missing_columns)}"
                        )
                    )
                    return

                self.stdout.write(
                    self.style.SUCCESS(f"Processing CSV file: {csv_file_path}")
                )

                # Group rows by sentry_org_id to handle multiple organizations per account
                sentry_accounts = {}
                for row_num, row in enumerate(reader, start=2):  # Start at 2 for header
                    try:
                        sentry_org_id = row["sentry_org_id"].strip()
                        if not sentry_org_id:
                            self.stdout.write(
                                self.style.WARNING(
                                    f"Row {row_num}: Empty sentry_org_id, skipping"
                                )
                            )
                            continue

                        if sentry_org_id not in sentry_accounts:
                            sentry_accounts[sentry_org_id] = {
                                "sentry_org_name": row["sentry_org_name"].strip(),
                                "organizations": [],
                            }

                        # Add organization data
                        org_data = {
                            "service_id": row["service_id"].strip(),
                            "slug": row["slug"].strip(),
                            "installation_id": row["installation_id"].strip(),
                            "provider": Service.GITHUB.value,  # Always GitHub as specified
                        }

                        # Validate required fields
                        if not all(
                            org_data[field]
                            for field in ["service_id", "slug", "installation_id"]
                        ):
                            self.stdout.write(
                                self.style.WARNING(
                                    f"Row {row_num}: Missing required organization data, skipping"
                                )
                            )
                            continue

                        sentry_accounts[sentry_org_id]["organizations"].append(org_data)

                    except Exception as e:
                        self.stdout.write(
                            self.style.ERROR(
                                f"Row {row_num}: Error processing row - {e}"
                            )
                        )
                        continue

                # Process each Sentry account
                total_accounts = len(sentry_accounts)
                processed_accounts = 0
                failed_accounts = 0

                for sentry_org_id, account_data in sentry_accounts.items():
                    try:
                        if dry_run:
                            self.stdout.write(
                                f"DRY-RUN: Would process Sentry org {sentry_org_id} "
                                f"with {len(account_data['organizations'])} organizations"
                            )
                            processed_accounts += 1
                            continue

                        with transaction.atomic():
                            result = self.process_sentry_account(
                                sentry_org_id, account_data
                            )
                            if result:
                                processed_accounts += 1
                                self.stdout.write(
                                    self.style.SUCCESS(
                                        f"Successfully processed Sentry org {sentry_org_id}"
                                    )
                                )
                            else:
                                failed_accounts += 1

                    except Exception as e:
                        failed_accounts += 1
                        self.stdout.write(
                            self.style.ERROR(
                                f"Failed to process Sentry org {sentry_org_id}: {e}"
                            )
                        )

                # Summary
                self.stdout.write(
                    self.style.SUCCESS(
                        f"\nProcessing complete:"
                        f"\n  Total accounts: {total_accounts}"
                        f"\n  Successfully processed: {processed_accounts}"
                        f"\n  Failed: {failed_accounts}"
                    )
                )

        except FileNotFoundError:
            self.stdout.write(self.style.ERROR(f"CSV file not found: {csv_file_path}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error processing CSV file: {e}"))

    def process_sentry_account(self, sentry_org_id, account_data):
        """
        Process a single Sentry account and its associated organizations.
        Adapted from the account_link view logic.
        """
        sentry_org_name = account_data["sentry_org_name"]
        organizations = account_data["organizations"]

        account_to_reactivate = None
        github_orgs = []

        # First pass: Check for conflicts and check for inactive account to reactivate
        for org_data in organizations:
            github_orgs.append(org_data)

            try:
                existing_owner = Owner.objects.get(
                    service_id=org_data["service_id"], service=org_data["provider"]
                )

                # If the organization is already linked to an active Sentry account, raise an error
                # If the organization is linked to an inactive Sentry account, set it to reactivate later
                if (
                    existing_owner.account
                    and existing_owner.account.plan == PlanName.SENTRY_MERGE_PLAN.value
                ):
                    if existing_owner.account.is_active:
                        self.stdout.write(
                            self.style.ERROR(
                                f"Organization {org_data['slug']} is already linked to an active Sentry account"
                            )
                        )
                        return False
                    elif account_to_reactivate is None:
                        account_to_reactivate = existing_owner.account

            except Owner.DoesNotExist:
                pass

        if not github_orgs:
            self.stdout.write(self.style.ERROR("No GitHub organizations found to link"))
            return False

        # Second pass: Account linking step
        if account_to_reactivate:
            account = account_to_reactivate
            account.sentry_org_id = sentry_org_id
            account.name = sentry_org_name
            account.plan = PlanName.SENTRY_MERGE_PLAN.value
            account.is_active = True
            account.save()
            self.stdout.write(f"  Reactivated existing account for {sentry_org_name}")
        else:
            account, created = Account.objects.get_or_create(
                sentry_org_id=sentry_org_id,
                defaults={
                    "name": sentry_org_name,
                    "plan": PlanName.SENTRY_MERGE_PLAN.value,
                    "is_active": True,
                },
            )

            if not created:
                account.name = sentry_org_name
                account.plan = PlanName.SENTRY_MERGE_PLAN.value
                account.is_active = True
                account.save()
                self.stdout.write(f"  Updated existing account for {sentry_org_name}")
            else:
                self.stdout.write(f"  Created new account for {sentry_org_name}")

        # Link organizations to the account
        for org_data in github_orgs:
            owner, owner_created = Owner.objects.get_or_create(
                service_id=org_data["service_id"],
                service=org_data["provider"],
                defaults={
                    "account": account,
                    "name": org_data["slug"],
                    "username": org_data["slug"],
                },
            )

            owner.account = account
            owner.save()

            installation_id = org_data["installation_id"]
            installation, installation_created = (
                GithubAppInstallation.objects.get_or_create(
                    installation_id=installation_id,
                    defaults={
                        "owner": owner,
                        "installation_id": installation_id,
                        "name": settings.GITHUB_SENTRY_APP_NAME,
                        "app_id": settings.GITHUB_SENTRY_APP_ID,
                    },
                )
            )

            action = "Created" if owner_created else "Updated"
            self.stdout.write(f"    {action} owner: {org_data['slug']}")

            action = "Created" if installation_created else "Updated"
            self.stdout.write(f"    {action} installation: {installation_id}")

        return True
