import argparse
import logging
from datetime import datetime

import stripe
from django.conf import settings
from django.core.management.base import BaseCommand

from billing.constants import (
    CANCELLATION_TASK_SIGNATURE,
    WEBHOOK_CANCELLATION_TASK_SIGNATURE,
)
from codecov_auth.models import Owner, Plan

log = logging.getLogger(__name__)

if settings.STRIPE_API_KEY:
    stripe.api_key = settings.STRIPE_API_KEY
    stripe.api_version = "2024-12-18.acacia"


def valid_date(date_string: str) -> datetime:
    """Validate and parse ISO format date string."""
    try:
        return datetime.fromisoformat(date_string)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: '{date_string}'. Use YYYY-MM-DD"
        )


class Command(BaseCommand):
    help = "Apply subscription schedules to subscriptions in bulk. This command is idempotent - it will skip subscriptions that already have a schedule."

    def add_arguments(self, parser):
        parser.add_argument(
            "--end-date",
            type=valid_date,
            required=True,
            help="The date to end the subscription schedule at on UTC time(format: YYYY-MM-DD), not-inclusive",
        )
        parser.add_argument(
            "--target-plan",
            type=str,
            default=None,
            help="The plan name to schedule the subscription to transition to (e.g., 'users-pr-inappy'). If omitted, uses the owner's current plan.",
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without applying them to Stripe",
        )

        # Filtering options
        parser.add_argument(
            "--renew-date-gte",
            type=valid_date,
            help="Only process owners with a renew date greater than or equal to this date (format: YYYY-MM-DD)",
        )
        parser.add_argument(
            "--renew-date-lte",
            type=valid_date,
            help="Only process owners with a renew date less than or equal to this date (format: YYYY-MM-DD)",
        )
        parser.add_argument(
            "--owner-ids",
            type=str,
            help="Comma-separated list of owner IDs to process",
        )
        parser.add_argument(
            "--current-plan",
            type=str,
            help="Only process owners currently on this plan (e.g., 'users-pr-inappy')",
        )
        parser.add_argument(
            "--exclude-owner-ids",
            type=str,
            help="Comma-separated list of owner IDs to exclude from processing",
        )

        # Pagination (cursor-based for stable batches across runs even if owners are added/removed)
        parser.add_argument(
            "--limit",
            type=int,
            default=100,
            help="Maximum number of subscriptions to process (default: 100)",
        )
        parser.add_argument(
            "--after-ownerid",
            type=int,
            default=None,
            metavar="OWNERID",
            help="Process only owners with ownerid greater than this (for next batch, use last processed ownerid from previous run)",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        target_plan_name = options["target_plan"]
        end_date = options["end_date"]
        limit = options["limit"]
        after_ownerid = options["after_ownerid"]

        if not settings.STRIPE_API_KEY:
            self.stdout.write(
                self.style.ERROR("STRIPE_API_KEY is not configured. Cannot proceed.")
            )
            return
        target_plan = None
        if target_plan_name:
            try:
                target_plan = Plan.objects.get(name=target_plan_name)
                if not target_plan.stripe_id:
                    self.stdout.write(
                        self.style.ERROR(
                            f"Plan '{target_plan_name}' does not have a Stripe price ID configured."
                        )
                    )
                    return
            except Plan.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(f"Plan '{target_plan_name}' does not exist.")
                )
                return

        if dry_run:
            self.stdout.write(
                self.style.WARNING("DRY RUN MODE - No changes will be made to Stripe")
            )

        # Build queryset of owners to process
        owners = Owner.objects.filter(
            stripe_subscription_id__isnull=False,
            stripe_customer_id__isnull=False,
        )

        # Then filter by renewal date, owner IDs, exclude owner IDs, and current plan if needed
        if options["renew_date_gte"]:
            owners = owners.filter(renew_date__gte=options["renew_date_gte"])

        if options["renew_date_lte"]:
            owners = owners.filter(renew_date__lte=options["renew_date_lte"])

        if options["owner_ids"]:
            raw = options["owner_ids"]
            owner_ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
            if not owner_ids:
                raise ValueError(
                    f"--owner-ids must be comma-separated integers, got: {raw!r}"
                )
            owners = owners.filter(ownerid__in=owner_ids)

        if options["exclude_owner_ids"]:
            exclude_ids = [
                int(x.strip()) for x in options["exclude_owner_ids"].split(",")
            ]
            owners = owners.exclude(ownerid__in=exclude_ids)

        if options["current_plan"]:
            owners = owners.filter(plan=options["current_plan"])

        owners = owners.order_by("ownerid")
        if after_ownerid is not None:
            owners = owners.filter(ownerid__gt=after_ownerid)

        total_matching = owners.count()
        if total_matching == 0:
            self.stdout.write(self.style.WARNING("No matching owners found"))
            return
        owners = list(owners[:limit])

        # Output summary
        self.stdout.write(f"Total matching owners: {total_matching}")
        self.stdout.write(
            f"Processing {len(owners)} owners (after_ownerid={after_ownerid}, limit={limit})"
        )
        if target_plan:
            self.stdout.write(
                f"Target plan: {target_plan_name} (Stripe ID: {target_plan.stripe_id})"
            )
        else:
            self.stdout.write("Target plan: (use current plan per owner)")
        if options["current_plan"]:
            self.stdout.write(f"Filtering by current plan: {options['current_plan']}")
        self.stdout.write("-" * 60)

        stats = {
            "processed": 0,
            "skipped_same_plan": 0,
            "skipped_has_schedule_with_target_date": 0,
            "skipped_no_plan": 0,
            "scheduled": 0,
            "errors": 0,
            "errored_owners": [],
        }

        for owner in owners:
            try:
                result = self.process_owner(
                    owner=owner,
                    target_plan=target_plan,
                    end_date=end_date,
                    dry_run=dry_run,
                )
                stats[result] += 1
                stats["processed"] += 1
            except stripe.StripeError as e:
                stats["errors"] += 1
                stats["errored_owners"].append(owner.ownerid)
                self.stdout.write(
                    self.style.ERROR(
                        f"  Stripe error for owner {owner.ownerid}: {e.user_message}"
                    )
                )
                log.warning(
                    f"Stripe error processing owner {owner.ownerid}",
                    extra={"error": str(e), "ownerid": owner.ownerid},
                )
            except Exception as e:
                stats["errors"] += 1
                self.stdout.write(
                    self.style.ERROR(f"  Error processing owner {owner.ownerid}: {e}")
                )
                log.exception(
                    f"Unexpected error processing owner {owner.ownerid}",
                    extra={"ownerid": owner.ownerid},
                )

        self.stdout.write("-" * 60)
        self.stdout.write(self.style.SUCCESS("Job completed!"))
        self.stdout.write(f"  Processed: {stats['processed']}")
        self.stdout.write(f"  Scheduled: {stats['scheduled']}")
        self.stdout.write(
            f"  Skipped (already on target plan): {stats['skipped_same_plan']}"
        )
        self.stdout.write(
            f"  Skipped (already has schedule with target date): {stats['skipped_has_schedule_with_target_date']}"
        )
        self.stdout.write(
            f"  Skipped (no Plan for Stripe price): {stats['skipped_no_plan']}"
        )
        self.stdout.write(f"  Error owners count: {stats['errors']}")
        self.stdout.write(
            f"  Errored owners: {', '.join(str(o) for o in stats['errored_owners'])}"
        )
        if owners and total_matching > len(owners):
            last_ownerid = owners[-1].ownerid
            self.stdout.write(
                self.style.SUCCESS(f"  Next batch: --after-ownerid={last_ownerid}")
            )

    def process_owner(
        self,
        owner: Owner,
        target_plan: Plan | None,
        end_date: datetime,
        dry_run: bool,
    ) -> str:
        """
        Process a single owner's subscription schedule.

        Returns a string indicating the result:
        - 'scheduled': A new schedule was created
        - 'skipped_has_schedule_with_target_date': Skipped because schedule already exists with target date
        - 'skipped_same_plan': Skipped because already on plan with target end date
        - 'skipped_no_plan': Skipped because no Plan record for subscription's Stripe price (when --target-plan omitted)
        """
        self.stdout.write(f"Processing owner {owner.ownerid} ({owner.username})...")

        subscription = stripe.Subscription.retrieve(owner.stripe_subscription_id)

        current_plan_id = subscription["items"]["data"][0]["plan"]["id"]
        current_quantity = subscription["items"]["data"][0]["quantity"]
        current_end_date = subscription["current_period_end"]

        self.stdout.write(
            f"  Current plan: {current_plan_id}, quantity: {current_quantity}"
        )

        # If no target plan passed, use current plan (look up Plan by Stripe price ID)
        if target_plan is None:
            target_plan = Plan.objects.filter(stripe_id=current_plan_id).first()
            if not target_plan:
                self.stdout.write(
                    self.style.WARNING(
                        f"  SKIPPED: No Plan found for Stripe price {current_plan_id}"
                    )
                )
                return "skipped_no_plan"

        # Check if already has target end date with same quantity
        if (
            current_plan_id == target_plan.stripe_id
            and current_end_date == end_date.strftime("%Y-%m-%d")
        ):
            self.stdout.write(
                self.style.WARNING(
                    "  SKIPPED: Already on target plan with same end date"
                )
            )
            return "skipped_same_plan"

        # Subscription already has a schedule, either for End or downgrade
        if subscription.schedule:
            existing_schedule = stripe.SubscriptionSchedule.retrieve(
                subscription.schedule
            )
            new_phase = {
                "start_date": existing_schedule.phases[-1]["start_date"],
                "end_date": end_date,
                "items": [
                    {
                        "plan": target_plan.stripe_id,
                        "price": target_plan.stripe_id,
                        "quantity": current_quantity,
                    }
                ],
            }

            # End schedule has already been added by this script or webhook handler
            if existing_schedule.metadata.get("task_signature") in [
                CANCELLATION_TASK_SIGNATURE,
                WEBHOOK_CANCELLATION_TASK_SIGNATURE,
            ]:
                if existing_schedule.metadata.get("end_date") == end_date.strftime(
                    "%Y-%m-%d"
                ):
                    self.stdout.write(
                        self.style.WARNING(
                            f"  SKIPPED: Subscription already has schedule {subscription.schedule} with target date"
                        )
                    )
                    return "skipped_has_schedule_with_target_date"
                else:
                    updated_phases = existing_schedule.phases.copy()
                    updated_phases = updated_phases[:-1] + [new_phase]

                    if dry_run:
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"  [DRY RUN] Would update schedule for id: {existing_schedule.id} to end on {end_date.strftime('%Y-%m-%d')} with {target_plan.name} and {current_quantity} seats"
                            )
                        )
                    else:
                        existing_schedule.modify(
                            phases=updated_phases,
                            metadata={
                                "task_signature": CANCELLATION_TASK_SIGNATURE,
                                "end_date": end_date.strftime("%Y-%m-%d"),
                                "script_version": "1.0",
                            },
                        )
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"  Updated schedule for id: {existing_schedule.id} to end on {end_date.strftime('%Y-%m-%d')} with {target_plan.name} and {current_quantity} seats"
                            )
                        )
                    return "scheduled"
            # Add an extra End phase to the existing schedule
            else:
                if dry_run:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  [DRY RUN] Would add schedule phase to id: {existing_schedule.id} to end on {end_date.strftime('%Y-%m-%d')} with {target_plan.name} and {current_quantity} seats"
                        )
                    )
                else:
                    existing_schedule.modify(
                        phases=existing_schedule.phases + [new_phase],
                        metadata={
                            "task_signature": CANCELLATION_TASK_SIGNATURE,
                            "end_date": end_date.strftime("%Y-%m-%d"),
                            "script_version": "1.0",
                        },
                    )
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  Added schedule phase to id:{existing_schedule.id} to end on {end_date.strftime('%Y-%m-%d')} with {target_plan.name} and {current_quantity} seats"
                        )
                    )
                return "scheduled"

        # Create the subscription schedule
        schedule = stripe.SubscriptionSchedule.create(
            from_subscription=owner.stripe_subscription_id,
            metadata={
                "task_signature": CANCELLATION_TASK_SIGNATURE,
                "end_date": end_date.strftime("%Y-%m-%d"),
                "script_version": "1.0",
            },
        )

        current_period_start = subscription["current_period_start"]
        current_period_end = subscription["current_period_end"]

        # Update the schedule with two phases:
        # 1. Current phase (maintains current plan until period end)
        # 2. New phase (transitions to target plan with new quantity)
        stripe.SubscriptionSchedule.modify(
            schedule.id,
            # we want the entire subscription to be cancelled at the end, not just released
            end_behavior="cancel",
            phases=[
                {
                    "start_date": current_period_start,
                    "end_date": current_period_end,
                    "items": [
                        {
                            "plan": current_plan_id,
                            "price": current_plan_id,
                            "quantity": current_quantity,
                        }
                    ],
                    "proration_behavior": "none",
                },
                {
                    "start_date": current_period_end,
                    "end_date": end_date,
                    "items": [
                        {
                            "plan": target_plan.stripe_id,
                            "price": target_plan.stripe_id,
                            # this should be updated by webhook handlers if this quantity changes after the schedule is created
                            "quantity": current_quantity,
                        }
                    ],
                    "proration_behavior": "none",
                },
            ],
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"  Created schedule {schedule.id} to end at {end_date.strftime('%Y-%m-%d')} with {target_plan.name} and {current_quantity} seats"
            )
        )

        log.info(
            "Created subscription schedule",
            extra={
                "ownerid": owner.ownerid,
                "subscription_id": owner.stripe_subscription_id,
                "schedule_id": schedule.id,
                "target_plan": target_plan.name,
                "target_quantity": current_quantity,
                "current_plan_id": current_plan_id,
                "current_quantity": current_quantity,
            },
        )

        return "scheduled"
