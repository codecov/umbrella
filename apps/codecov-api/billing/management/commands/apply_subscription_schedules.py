import logging

import stripe
from django.conf import settings
from django.core.management.base import BaseCommand

from codecov_auth.models import Owner, Plan

log = logging.getLogger(__name__)

# Initialize Stripe with credentials from settings
if settings.STRIPE_API_KEY:
    stripe.api_key = settings.STRIPE_API_KEY
    stripe.api_version = "2024-12-18.acacia"

# Offset in seconds for schedule release (matches billing.py)
SCHEDULE_RELEASE_OFFSET = 10


class Command(BaseCommand):
    help = "Apply subscription schedules to subscriptions in bulk. This command is idempotent - it will skip subscriptions that already have a schedule."

    def add_arguments(self, parser):
        # Required arguments
        parser.add_argument(
            "--target-plan",
            type=str,
            required=True,
            help="The plan name to schedule the subscription to transition to (e.g., 'users-pr-inappm')",
        )

        # Execution mode
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Preview changes without applying them to Stripe",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Force update even if a schedule already exists (will release existing schedule first)",
        )

        # Filtering options
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
            "--service",
            type=str,
            choices=["github", "gitlab", "bitbucket"],
            help="Only process owners from this service provider",
        )
        parser.add_argument(
            "--exclude-owner-ids",
            type=str,
            help="Comma-separated list of owner IDs to exclude from processing",
        )

        # Quantity options
        parser.add_argument(
            "--target-quantity",
            type=int,
            help="Set a specific quantity (seats) for the scheduled plan. If not provided, maintains current quantity.",
        )
        parser.add_argument(
            "--min-quantity",
            type=int,
            help="Only process subscriptions with at least this many seats",
        )
        parser.add_argument(
            "--max-quantity",
            type=int,
            help="Only process subscriptions with at most this many seats",
        )

        # Pagination
        parser.add_argument(
            "--limit",
            type=int,
            default=100,
            help="Maximum number of subscriptions to process (default: 100)",
        )
        parser.add_argument(
            "--offset",
            type=int,
            default=0,
            help="Number of subscriptions to skip (for pagination, default: 0)",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        target_plan_name = options["target_plan"]
        limit = options["limit"]
        offset = options["offset"]
        force = options["force"]
        target_quantity = options.get("target_quantity")

        if not settings.STRIPE_API_KEY:
            self.stdout.write(
                self.style.ERROR("STRIPE_API_KEY is not configured. Cannot proceed.")
            )
            return

        # Validate target plan exists
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

        # Apply filters
        if options["owner_ids"]:
            owner_ids = [int(x.strip()) for x in options["owner_ids"].split(",")]
            owners = owners.filter(ownerid__in=owner_ids)

        if options["exclude_owner_ids"]:
            exclude_ids = [
                int(x.strip()) for x in options["exclude_owner_ids"].split(",")
            ]
            owners = owners.exclude(ownerid__in=exclude_ids)

        if options["current_plan"]:
            owners = owners.filter(plan=options["current_plan"])

        if options["service"]:
            owners = owners.filter(service=options["service"])

        if options["min_quantity"]:
            owners = owners.filter(plan_user_count__gte=options["min_quantity"])

        if options["max_quantity"]:
            owners = owners.filter(plan_user_count__lte=options["max_quantity"])

        # Apply pagination
        total_matching = owners.count()
        owners = list(owners[offset : offset + limit])

        # Display filter summary
        self.stdout.write(f"Total matching owners: {total_matching}")
        self.stdout.write(
            f"Processing {len(owners)} owners (offset: {offset}, limit: {limit})"
        )
        self.stdout.write(
            f"Target plan: {target_plan_name} (Stripe ID: {target_plan.stripe_id})"
        )
        if target_quantity:
            self.stdout.write(f"Target quantity: {target_quantity}")
        if options["current_plan"]:
            self.stdout.write(f"Filtering by current plan: {options['current_plan']}")
        if options["service"]:
            self.stdout.write(f"Filtering by service: {options['service']}")
        self.stdout.write("-" * 60)

        stats = {
            "processed": 0,
            "skipped_has_schedule": 0,
            "skipped_same_plan": 0,
            "scheduled": 0,
            "errors": 0,
        }

        for owner in owners:
            try:
                result = self.process_owner(
                    owner=owner,
                    target_plan=target_plan,
                    target_quantity=target_quantity,
                    dry_run=dry_run,
                    force=force,
                )
                stats[result] += 1
                stats["processed"] += 1
            except stripe.StripeError as e:
                stats["errors"] += 1
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
        self.stdout.write(self.style.SUCCESS("Completed!"))
        self.stdout.write(f"  Processed: {stats['processed']}")
        self.stdout.write(f"  Scheduled: {stats['scheduled']}")
        self.stdout.write(
            f"  Skipped (already has schedule): {stats['skipped_has_schedule']}"
        )
        self.stdout.write(
            f"  Skipped (already on target plan): {stats['skipped_same_plan']}"
        )
        self.stdout.write(f"  Errors: {stats['errors']}")

    def process_owner(
        self,
        owner: Owner,
        target_plan: Plan,
        target_quantity: int | None,
        dry_run: bool,
        force: bool,
    ) -> str:
        """
        Process a single owner's subscription schedule.

        Returns a string indicating the result:
        - 'scheduled': A new schedule was created
        - 'skipped_has_schedule': Skipped because schedule already exists
        - 'skipped_same_plan': Skipped because already on target plan
        """
        self.stdout.write(f"Processing owner {owner.ownerid} ({owner.username})...")

        # Retrieve the subscription from Stripe
        subscription = stripe.Subscription.retrieve(owner.stripe_subscription_id)

        current_plan_id = subscription["items"]["data"][0]["plan"]["id"]
        current_quantity = subscription["items"]["data"][0]["quantity"]

        # Use target_quantity if specified, otherwise maintain current quantity
        new_quantity = (
            target_quantity if target_quantity is not None else current_quantity
        )

        self.stdout.write(
            f"  Current plan: {current_plan_id}, quantity: {current_quantity}"
        )
        if target_quantity is not None:
            self.stdout.write(f"  Will change quantity to: {new_quantity}")

        # Check if already on target plan with same quantity (idempotency check)
        if (
            current_plan_id == target_plan.stripe_id
            and current_quantity == new_quantity
        ):
            self.stdout.write(
                self.style.WARNING(
                    "  SKIPPED: Already on target plan with same quantity"
                )
            )
            return "skipped_same_plan"

        # Check if subscription already has a schedule (idempotency check)
        if subscription.schedule and not force:
            self.stdout.write(
                self.style.WARNING(
                    f"  SKIPPED: Subscription already has schedule {subscription.schedule}"
                )
            )
            return "skipped_has_schedule"

        if dry_run:
            self.stdout.write(
                self.style.SUCCESS(
                    f"  [DRY RUN] Would create schedule to transition to {target_plan.name} with {new_quantity} seats"
                )
            )
            return "scheduled"

        # Release existing schedule if force mode and schedule exists
        if subscription.schedule and force:
            self.stdout.write(
                f"  Releasing existing schedule {subscription.schedule}..."
            )
            stripe.SubscriptionSchedule.release(subscription.schedule)
            # Re-fetch subscription after releasing schedule
            subscription = stripe.Subscription.retrieve(owner.stripe_subscription_id)

        # Create the subscription schedule
        schedule = stripe.SubscriptionSchedule.create(
            from_subscription=owner.stripe_subscription_id
        )

        current_period_start = subscription["current_period_start"]
        current_period_end = subscription["current_period_end"]

        # Update the schedule with two phases:
        # 1. Current phase (maintains current plan until period end)
        # 2. New phase (transitions to target plan with new quantity)
        stripe.SubscriptionSchedule.modify(
            schedule.id,
            end_behavior="release",
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
                    "end_date": current_period_end + SCHEDULE_RELEASE_OFFSET,
                    "items": [
                        {
                            "plan": target_plan.stripe_id,
                            "price": target_plan.stripe_id,
                            "quantity": new_quantity,
                        }
                    ],
                    "proration_behavior": "none",
                },
            ],
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"  Created schedule {schedule.id} to transition to {target_plan.name} with {new_quantity} seats"
            )
        )

        log.info(
            "Created subscription schedule",
            extra={
                "ownerid": owner.ownerid,
                "subscription_id": owner.stripe_subscription_id,
                "schedule_id": schedule.id,
                "target_plan": target_plan.name,
                "target_quantity": new_quantity,
                "current_plan_id": current_plan_id,
                "current_quantity": current_quantity,
            },
        )

        return "scheduled"
