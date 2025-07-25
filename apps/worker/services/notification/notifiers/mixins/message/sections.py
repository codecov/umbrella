import logging
import random
from base64 import b64encode
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum, auto
from itertools import starmap
from urllib.parse import urlencode

from helpers.environment import is_enterprise
from helpers.reports import get_totals_from_file_in_reports
from services.comparison import ComparisonProxy
from services.comparison.types import ReportUploadedCount
from services.notification.notifiers.mixins.message.helpers import (
    diff_to_string,
    ellipsis,
    escape_markdown,
    get_table_header,
    get_table_layout,
    has_project_status,
    is_coverage_drop_significant,
    make_metrics,
    make_patch_only_metrics,
)
from services.urls import get_commit_url_from_commit_sha, get_pull_graph_url
from services.yaml.reader import get_components_from_yaml, round_number
from shared.helpers.yaml import walk
from shared.reports.resources import Report
from shared.validation.helpers import LayoutStructure

log = logging.getLogger(__name__)

ALL_TESTS_PASSED_MSG = ":white_check_mark: All tests successful. No failed tests found."

SectionList = list[tuple[str, type["BaseSectionWriter"]]]


@dataclass
class MessageLayout:
    upper: SectionList
    middle: SectionList
    lower: SectionList


HEADER_SECTIONS = {"header", "newheader", "condensed_header"}


def get_message_layout(
    settings, status_or_checks_helper_text: dict[str, str] | None
) -> MessageLayout:
    sections = [s.strip() for s in settings.get("layout", "").split(",")]

    # force at least the `condensed_header` if no header has been configured
    if not any(s in HEADER_SECTIONS for s in sections):
        sections.insert(0, "condensed_header")

    # append the `newfiles` section if there is a `files` or `tree` section
    if "files" in sections or "tree" in sections:
        sections.append("newfiles")

    upper: SectionList = []
    middle: SectionList = []
    lower: SectionList = []

    for section in sections:
        if section in HEADER_SECTIONS:
            upper.append((section, HeaderSectionWriter))
        elif section == "newfiles" or section == "condensed_files":
            upper.append((section, NewFilesSectionWriter))

        elif section.startswith("flag"):
            middle.append((section, FlagSectionWriter))
        elif section == "diff":
            middle.append((section, DiffSectionWriter))
        elif section.startswith(("files", "tree")):
            middle.append((section, FileSectionWriter))
        elif section == "reach":
            middle.append((section, ReachSectionWriter))
        elif section == "announcements":
            middle.append((section, AnnouncementSectionWriter))
        elif section.startswith("component"):
            middle.append((section, ComponentsSectionWriter))
        elif section == "footer":
            middle.append((section, FooterSectionWriter))

        elif section == "newfooter" or section == "condensed_footer":
            lower.append(("newfooter", NewFooterSectionWriter))

        elif section not in LayoutStructure.acceptable_objects:
            log.warning("Improper layout name", extra={"layout": section})

    # append the helper text and messages to user to the upper section
    if status_or_checks_helper_text:
        upper.append(("status_or_checks_helper_text", HelperTextSectionWriter))
    upper.append(("messages_to_user", MessagesToUserSectionWriter))
    return MessageLayout(upper, middle, lower)


class BaseSectionWriter:
    def __init__(
        self,
        repository,
        layout,
        show_complexity,
        settings,
        current_yaml,
        status_or_checks_helper_text=None,
    ):
        self.repository = repository
        self.layout = layout
        self.show_complexity = show_complexity
        self.settings = settings
        self.current_yaml = current_yaml
        self.status_or_checks_helper_text = status_or_checks_helper_text

    @property
    def name(self):
        return self.__class__.__name__

    def write_section(self, *args, **kwargs) -> list[str]:
        return list(self.do_write_section(*args, **kwargs))


class NewFooterSectionWriter(BaseSectionWriter):
    def do_write_section(self, comparison, diff, changes, links, behind_by=None):
        hide_project_coverage = self.settings.get("hide_project_coverage", False)
        if hide_project_coverage:
            yield ""
            yield ":loudspeaker: Thoughts on this report? [Let us know!]({})".format(
                "https://about.codecov.io/pull-request-comment-report/"
            )

        else:
            repo_service = comparison.repository_service.service
            yield ""
            yield "[:umbrella: View full report in Codecov by Sentry]({}?dropdown=coverage&src=pr&el=continue).   ".format(
                links["pull"]
            )
            yield ":loudspeaker: Have feedback on the report? [Share it here]({}).".format(
                "https://about.codecov.io/codecov-pr-comment-feedback/"
                if repo_service == "github"
                else "https://gitlab.com/codecov-open-source/codecov-user-feedback/-/issues/4"
            )


class HeaderSectionWriter(BaseSectionWriter):
    def _possibly_include_test_result_setup_confirmation(self, comparison):
        if ta_error_msg := comparison.test_results_error():
            yield f":warning: {ta_error_msg}"
        elif comparison.all_tests_passed():
            yield ALL_TESTS_PASSED_MSG

    def do_write_section(self, comparison, diff, changes, links, behind_by=None):
        yaml = self.current_yaml
        base_report = comparison.project_coverage_base.report
        head_report = comparison.head.report
        pull_dict = comparison.enriched_pull.provider_pull

        diff_totals = head_report.apply_diff(diff)
        if diff_totals:
            misses_and_partials = diff_totals.misses + diff_totals.partials
            patch_coverage = diff_totals.coverage
        else:
            misses_and_partials = None
            patch_coverage = None
        if misses_and_partials:
            ln_text = "lines" if misses_and_partials > 1 else "line"
            yield (
                f":x: Patch coverage is `{patch_coverage}%` with `{misses_and_partials} {ln_text}` in your changes missing coverage. Please review."
            )
        else:
            yield ":white_check_mark: All modified and coverable lines are covered by tests."

        hide_project_coverage = self.settings.get("hide_project_coverage", False)
        if hide_project_coverage:
            for msg in self._possibly_include_test_result_setup_confirmation(
                comparison
            ):
                yield msg
            return

        if base_report and head_report:
            yield (
                f":white_check_mark: Project coverage is {round_number(yaml, Decimal(head_report.totals.coverage))}%. Comparing base ([`{comparison.project_coverage_base.commit.commitid[:7]}`]({links['base']}?dropdown=coverage&el=desc)) to head ([`{comparison.head.commit.commitid[:7]}`]({links['head']}?dropdown=coverage&el=desc))."
            )
        else:
            # This doesn't actually emit a message if the _head_ report is missing
            # Because we don't notify if the _head_ report is missing
            # But it's still used if the base report is missing.
            # Why didn't you change the condition and the code then? Idk... maybe I'm wrong in my assumptions :P
            what = "BASE" if not base_report else "HEAD"
            branch = pull_dict["base" if not base_report else "head"]["branch"]
            commit = pull_dict["base" if not base_report else "head"]["commitid"][:7]
            yield (
                f":warning: Please [upload](https://docs.codecov.com/docs/codecov-uploader) report for {what} (`{branch}@{commit}`). [Learn more](https://docs.codecov.io/docs/error-reference#section-missing-{what.lower()}-commit) about missing {what} report."
            )

        if behind_by:
            yield (
                f":warning: Report is {behind_by} commits behind head on {pull_dict['base']['branch']}."
            )
        if (
            comparison.enriched_pull.provider_pull is not None
            and comparison.head.commit.commitid
            != comparison.enriched_pull.provider_pull["head"]["commitid"]
        ):
            # Temporary log so we understand when this happens
            log.info(
                "Notifying user that current head and pull head differ",
                extra={
                    "repoid": comparison.head.commit.repoid,
                    "commit": comparison.head.commit.commitid,
                    "pull_head": comparison.enriched_pull.provider_pull["head"][
                        "commitid"
                    ],
                },
            )
            yield ""
            pull_head = comparison.enriched_pull.provider_pull["head"]["commitid"][:7]
            current_head = comparison.head.commit.commitid[:7]
            yield (
                f":warning: **Current head {current_head} differs from pull request most recent head {pull_head}**"
            )
            yield ""
            yield f"Please [upload](https://docs.codecov.com/docs/codecov-uploader) reports for the commit {pull_head} to get more accurate results."

        for msg in self._possibly_include_test_result_setup_confirmation(comparison):
            yield msg


class AnnouncementSectionWriter(BaseSectionWriter):
    current_active_messages = [
        "Codecov offers a browser extension for seamless coverage viewing on GitHub. Try it in [Chrome](https://chrome.google.com/webstore/detail/codecov/gedikamndpbemklijjkncpnolildpbgo) or [Firefox](https://addons.mozilla.org/en-US/firefox/addon/codecov/) today!"
        #   "Codecov can now indicate which changes are the most critical in Pull Requests. [Learn more](https://about.codecov.io/product/feature/runtime-insights/)"  # This is disabled as of CODE-1885. But we might bring it back later.
    ]

    def do_write_section(self, comparison: ComparisonProxy, *args, **kwargs):
        # This allows us to shift through active messages while respecting the annoucement limit.
        message_to_display = random.choice(
            AnnouncementSectionWriter.current_active_messages
        )

        yield f":mega: {message_to_display}"


class FooterSectionWriter(BaseSectionWriter):
    def do_write_section(self, comparison, diff, changes, links, behind_by=None):
        pull_dict = comparison.enriched_pull.provider_pull
        yield ""
        yield "------"
        yield ""
        yield (
            "[Continue to review full report in Codecov by Sentry]({}?dropdown=coverage&src=pr&el=continue).".format(
                links["pull"]
            )
        )
        yield "> **Legend** - [Click here to learn more](https://docs.codecov.io/docs/codecov-delta)"
        yield "> `\u0394 = absolute <relative> (impact)`, `\xf8 = not affected`, `? = missing data`"

        yield (
            "> Powered by [Codecov]({pull}?dropdown=coverage&src=pr&el=footer). Last update [{base}...{head}]({pull}?dropdown=coverage&src=pr&el=lastupdated). Read the [comment docs]({comment}).".format(
                pull=links["pull"],
                base=pull_dict["base"]["commitid"][:7],
                head=pull_dict["head"]["commitid"][:7],
                comment="https://docs.codecov.io/docs/pull-request-comments",
            )
        )


class ReachSectionWriter(BaseSectionWriter):
    def do_write_section(self, comparison, diff, changes, links, behind_by=None):
        pull = comparison.enriched_pull.database_pull
        yield ""
        yield (
            "[![Impacted file tree graph]({})]({}?src=pr&el=tree)".format(
                get_pull_graph_url(
                    pull,
                    "tree.svg",
                    width=650,
                    height=150,
                    src="pr",
                    token=pull.repository.image_token,
                ),
                links["pull"],
            )
        )


class DiffSectionWriter(BaseSectionWriter):
    def do_write_section(self, comparison, diff, changes, links, behind_by=None):
        base_report = comparison.project_coverage_base.report
        head_report = comparison.head.report
        if base_report is None:
            base_report = Report()
        pull_dict = comparison.enriched_pull.provider_pull
        pull = comparison.enriched_pull.database_pull
        yield ""
        yield "```diff"
        lines = diff_to_string(
            self.current_yaml,
            pull_dict["base"]["branch"],  # important because base may be null
            base_report.totals if base_report else None,
            f"#{pull.pullid}",
            head_report.totals,
        )
        yield from lines
        yield "```"


def _get_tree_cell(typ, path, metrics, compare, is_critical):
    return "| {rm}[{path}]({compare}?src=pr&el=tree&{path_as_query_param}#diff-{hash}){rm}{file_tags} {metrics}".format(
        rm="~~" if typ == "deleted" else "",
        path=escape_markdown(ellipsis(path, 50, False)),
        path_as_query_param=urlencode({"filepath": path}),
        compare=compare,
        hash=b64encode(path.encode()).decode(),
        metrics=metrics,
        file_tags=" **Critical**" if is_critical else "",
    )


class NewFilesSectionWriter(BaseSectionWriter):
    def do_write_section(self, comparison, diff, changes, links, behind_by=None):
        # create list of files changed in diff
        base_report = comparison.project_coverage_base.report
        head_report = comparison.head.report
        if base_report is None:
            base_report = Report()
        files_in_diff = [
            (
                _diff["type"],
                path,
                make_patch_only_metrics(
                    get_totals_from_file_in_reports(base_report, path) or False,
                    get_totals_from_file_in_reports(head_report, path) or False,
                    _diff["totals"],
                    self.show_complexity,
                    self.current_yaml,
                    links["pull"],
                ),
                int(_diff["totals"].misses + _diff["totals"].partials),
            )
            for path, _diff in (diff["files"] if diff else {}).items()
            if _diff.get("totals")
        ]

        all_files = {f[1] for f in files_in_diff or []} | {
            c.path for c in changes or []
        }
        if files_in_diff:
            table_header = "| Patch % | Lines |"
            table_layout = "|---|---|---|"

            # get limit of results to show
            limit = int(self.layout.split(":")[1] if ":" in self.layout else 10)
            mentioned = []
            files_in_critical = set()

            def tree_cell(typ, path, metrics, _=None):
                if path not in mentioned:
                    # mentioned: for files that are in diff and changes
                    mentioned.append(path)
                    return _get_tree_cell(
                        typ=typ,
                        path=path,
                        metrics=metrics,
                        compare=links["pull"],
                        is_critical=path in files_in_critical,
                    )

            remaining_files = 0
            printed_files = 0
            changed_files = sorted(
                files_in_diff, key=lambda a: a[3] or Decimal("0"), reverse=True
            )
            changed_files_with_missing_lines = [f for f in changed_files if f[3] > 0]
            if changed_files_with_missing_lines:
                yield ""
                yield (
                    "| [Files with missing lines]({}?dropdown=coverage&src=pr&el=tree) {}".format(
                        links["pull"], table_header
                    )
                )
                yield table_layout
            for file in changed_files_with_missing_lines:
                if printed_files == limit:
                    remaining_files += 1
                else:
                    printed_files += 1
                    yield tree_cell(file[0], file[1], file[2])
            if remaining_files:
                yield (
                    "| ... and [{n} more]({href}?src=pr&el=tree-more) | |".format(
                        n=remaining_files, href=links["pull"]
                    )
                )


class FileSectionWriter(BaseSectionWriter):
    def do_write_section(self, comparison, diff, changes, links, behind_by=None):
        # create list of files changed in diff
        base_report = comparison.project_coverage_base.report
        head_report = comparison.head.report
        if base_report is None:
            base_report = Report()
        files_in_diff = [
            (
                _diff["type"],
                path,
                make_metrics(
                    get_totals_from_file_in_reports(base_report, path) or False,
                    get_totals_from_file_in_reports(head_report, path) or False,
                    _diff["totals"],
                    self.show_complexity,
                    self.current_yaml,
                    links["pull"],
                ),
                int(_diff["totals"].misses + _diff["totals"].partials),
            )
            for path, _diff in (diff["files"] if diff else {}).items()
            if _diff.get("totals")
        ]

        all_files = {f[1] for f in files_in_diff or []} | {
            c.path for c in changes or []
        }
        if files_in_diff:
            table_header = get_table_header(self.show_complexity)
            table_layout = get_table_layout(self.show_complexity)

            # get limit of results to show
            limit = int(self.layout.split(":")[1] if ":" in self.layout else 10)
            mentioned = []
            files_in_critical = set()

            def tree_cell(typ, path, metrics, _=None):
                if path not in mentioned:
                    # mentioned: for files that are in diff and changes
                    mentioned.append(path)
                    return _get_tree_cell(
                        typ=typ,
                        path=path,
                        metrics=metrics,
                        compare=links["pull"],
                        is_critical=path in files_in_critical,
                    )

            yield ""
            yield (
                "| [Files with missing lines]({}?dropdown=coverage&src=pr&el=tree) {}".format(
                    links["pull"], table_header
                )
            )
            yield table_layout
            yield from starmap(
                tree_cell,
                sorted(files_in_diff, key=lambda a: a[3] or Decimal("0"))[:limit],
            )
            remaining = len(files_in_diff) - limit
            if remaining > 0:
                yield (
                    "| ... and [{n} more]({href}?src=pr&el=tree-more) | |".format(
                        n=remaining, href=links["pull"]
                    )
                )

        if changes:
            len_changes_not_in_diff = len(all_files or []) - len(files_in_diff or [])
            if files_in_diff and len_changes_not_in_diff > 0:
                yield ""
                yield (
                    "... and [{n} file{s} with indirect coverage changes]({href}/indirect-changes?src=pr&el=tree-more)".format(
                        n=len_changes_not_in_diff,
                        href=links["pull"],
                        s="s" if len_changes_not_in_diff > 1 else "",
                    )
                )
            elif len_changes_not_in_diff > 0:
                yield (
                    "[see {n} file{s} with indirect coverage changes]({href}/indirect-changes?src=pr&el=tree-more)".format(
                        n=len_changes_not_in_diff,
                        href=links["pull"],
                        s="s" if len_changes_not_in_diff > 1 else "",
                    )
                )


class FlagSectionWriter(BaseSectionWriter):
    def do_write_section(self, comparison, diff, changes, links, behind_by=None):
        # flags
        base_report = comparison.project_coverage_base.report
        head_report = comparison.head.report
        if base_report is None:
            base_report = Report()
        base_flags = base_report.flags if base_report else {}
        head_flags = head_report.flags if head_report else {}
        missing_flags = set(base_flags.keys()) - set(head_flags.keys())
        flags = []

        show_carriedforward_flags = self.settings.get("show_carryforward_flags", False)
        for name, flag in head_flags.items():
            if (show_carriedforward_flags is True) or (  # Include all flags
                show_carriedforward_flags is False
                and flag.carriedforward
                is False  # Only include flags without carriedforward coverage
            ):
                flags.append(
                    {
                        "name": name,
                        "before": get_totals_from_file_in_reports(base_flags, name),
                        "after": flag.totals,
                        "diff": (
                            flag.apply_diff(diff) if walk(diff, ("files",)) else None
                        ),
                        "carriedforward": flag.carriedforward,
                        "carriedforward_from": flag.carriedforward_from,
                    }
                )

        flags.extend(
            {
                "name": flag,
                "before": base_flags[flag],
                "after": None,
                "diff": None,
                "carriedforward": False,
                "carriedforward_from": None,
            }
            for flag in missing_flags
        )

        # TODO: get icons working
        # flag_icon_url = ""
        # carriedforward_flag_icon_url = ""

        if flags:
            # Even if "show_carryforward_flags" is true, we don't want to show that column if there isn't actually carriedforward coverage,
            # so figure out if we actually have any carriedforward coverage to show
            has_carriedforward_flags = any(
                flag["carriedforward"] is True
                for flag in flags  # If "show_carryforward_flags" yaml setting is set to false there won't be any flags in this list with carriedforward coverage.
            )

            table_header = (
                "| Coverage \u0394 |"
                + (" Complexity \u0394 |" if self.show_complexity else "")
                + " |"
                + (" *Carryforward flag |" if has_carriedforward_flags else "")
            )
            table_layout = (
                "|---|---|---|"
                + ("---|" if self.show_complexity else "")
                + ("---|" if has_carriedforward_flags else "")
            )

            yield ""
            yield (
                "| [Flag]({href}/flags?src=pr&el=flags) ".format(href=links["pull"])
                + table_header
            )
            yield table_layout
            for flag in sorted(flags, key=lambda f: f["name"]):
                carriedforward, carriedforward_from = (
                    flag["carriedforward"],
                    flag["carriedforward_from"],
                )
                # Format the message for the "carriedforward" column, if the flag was carried forward
                if carriedforward is True:
                    # The "from <link to parent commit>" text will only appear if we actually know which commit we carried forward from
                    carriedforward_from_url = (
                        get_commit_url_from_commit_sha(
                            self.repository, carriedforward_from
                        )
                        if carriedforward_from
                        else ""
                    )

                    carriedforward_message = (
                        " Carriedforward"
                        + (
                            f" from [{carriedforward_from[:7]}]({carriedforward_from_url})"
                            if carriedforward_from and carriedforward_from_url
                            else ""
                        )
                        + " |"
                    )
                else:
                    carriedforward_message = " |" if has_carriedforward_flags else ""

                yield (
                    "| {name} {metrics}{cf}".format(
                        name="[{flag_name}]({href}/flags?src=pr&el=flag)".format(
                            flag_name=flag["name"],
                            href=links["pull"],
                        ),
                        metrics=make_metrics(
                            flag["before"],
                            flag["after"],
                            flag["diff"],
                            self.show_complexity,
                            self.current_yaml,
                        ),
                        cf=carriedforward_message,
                    )
                )

            if has_carriedforward_flags and show_carriedforward_flags:
                yield ""
                yield (
                    "*This pull request uses carry forward flags. [Click here](https://docs.codecov.io/docs/carryforward-flags) to find out more."
                )
            elif not show_carriedforward_flags:
                yield ""
                yield (
                    "Flags with carried forward coverage won't be shown. [Click here](https://docs.codecov.io/docs/carryforward-flags#carryforward-flags-in-the-pull-request-comment) to find out more."
                )


class ComponentsSectionWriter(BaseSectionWriter):
    def _get_table_data_for_components(
        self, all_components, comparison: ComparisonProxy
    ) -> list[dict]:
        component_data = []
        for component in all_components:
            flags = component.get_matching_flags(comparison.head.report.flags.keys())
            filtered_comparison = comparison.get_filtered_comparison(
                flags, component.paths
            )
            diff = filtered_comparison.get_diff()
            component_data.append(
                {
                    "name": component.get_display_name(),
                    "before": (
                        filtered_comparison.project_coverage_base.report.totals
                        if filtered_comparison.project_coverage_base.report is not None
                        else None
                    ),
                    "after": filtered_comparison.head.report.totals,
                    "diff": filtered_comparison.head.report.apply_diff(
                        diff, _save=False
                    ),
                }
            )
        return component_data

    def do_write_section(
        self, comparison: ComparisonProxy, diff, changes, links, behind_by=None
    ):
        all_components = get_components_from_yaml(self.current_yaml)
        if all_components == []:
            return  # fast return if there's noting to process

        component_data_to_show = self._get_table_data_for_components(
            all_components, comparison
        )

        yield ""
        # Table header and layout
        yield "| [Components]({href}/components?src=pr&el=components) | Coverage \u0394 | |".format(
            href=links["pull"],
        )
        yield "|---|---|---|"
        # The interesting part
        for component_data in component_data_to_show:
            yield (
                "| {name} {metrics}".format(
                    name="[{component_name}]({href}/components?src=pr&el=component)".format(
                        component_name=component_data["name"],
                        href=links["pull"],
                    ),
                    metrics=make_metrics(
                        component_data["before"],
                        component_data["after"],
                        component_data["diff"],
                        show_complexity=False,
                        yaml=self.current_yaml,
                    ),
                )
            )


class HelperTextSectionWriter(BaseSectionWriter):
    def do_write_section(self, comparison: ComparisonProxy, *args, **kwargs):
        helper_template = ":x: {helper_text}"
        sorted_helper_text_keys = sorted(self.status_or_checks_helper_text.keys())
        if sorted_helper_text_keys:
            yield ""
        for helper_text_key in sorted_helper_text_keys:
            yield helper_template.format(
                helper_text=self.status_or_checks_helper_text[helper_text_key]
            )


class MessagesToUserSectionWriter(BaseSectionWriter):
    class Messages(Enum):
        INSTALL_GITHUB_APP_WARNING = auto()
        DIFFERENT_UPLOAD_COUNT_WARNING = auto()

    def _write_different_upload_count_warning(self, comparison: ComparisonProxy) -> str:
        upload_diff = comparison.get_reports_uploaded_count_per_flag_diff()
        is_commit_complete = comparison.head.commit.state == "complete"
        if (
            is_commit_complete
            and upload_diff
            and has_project_status(self.current_yaml)
            and is_coverage_drop_significant(comparison)
        ):
            template = (
                "> :exclamation:  There is a different number of reports uploaded between BASE ({base_short_sha}) and HEAD ({head_short_sha}). Click for more details."
                + "\n> "
                + "\n> <details><summary>HEAD has {aggregated_upload_diff} upload{plural} {more_or_less} than BASE</summary>"
                # There needs to be a padding line between the <summary> and the table or it won't be rendered correctly
                + "\n>"
                + "\n>| Flag | BASE ({base_short_sha}) | HEAD ({head_short_sha}) |"
                + "\n>|------|------|------|"
                + "{per_flag_diff_lines}"
                + "\n></details>"
            )

            def get_line_for_flag_diff(info: ReportUploadedCount) -> str:
                line_template = "\n>|{flag_name}|{base_count}|{head_count}|"
                return line_template.format(
                    flag_name=info["flag"],
                    base_count=info["base_count"],
                    head_count=info["head_count"],
                )

            aggregated_upload_diff = sum(
                diff["head_count"] - diff["base_count"] for diff in upload_diff
            )
            context = {
                "aggregated_upload_diff": abs(aggregated_upload_diff),
                "more_or_less": "more" if aggregated_upload_diff > 0 else "less",
                "plural": "s" if abs(aggregated_upload_diff) > 1 else "",
                "base_short_sha": comparison.project_coverage_base.commit.commitid[:7],
                "head_short_sha": comparison.head.commit.commitid[:7],
                "per_flag_diff_lines": "".join(
                    [get_line_for_flag_diff(info) for info in upload_diff]
                ),
            }

            return template.format(**context)
        return ""

    def _write_install_github_app_warning(self, comparison: ComparisonProxy) -> str:
        """Writes a warning message to GitHub owners that have not yet installed the Codecov App to their account."""
        repo = comparison.head.commit.repository
        owner = repo.owner
        is_user_in_github = owner.service == "github"
        owner_is_using_app = (
            owner.integration_id is not None or owner.github_app_installations != []
        )
        if is_user_in_github and not is_enterprise() and not owner_is_using_app:
            return ":exclamation: Your organization needs to install the [Codecov GitHub app](https://github.com/apps/codecov/installations/select_target) to enable full functionality."
        return ""

    def do_write_section(self, comparison: ComparisonProxy, *args, **kwargs):
        messages_ordering = [
            self.Messages.INSTALL_GITHUB_APP_WARNING,
            self.Messages.DIFFERENT_UPLOAD_COUNT_WARNING,
        ]
        messages_content = {
            self.Messages.INSTALL_GITHUB_APP_WARNING: self._write_install_github_app_warning(
                comparison
            ),
            self.Messages.DIFFERENT_UPLOAD_COUNT_WARNING: self._write_different_upload_count_warning(
                comparison
            ),
        }
        for message in messages_ordering:
            message_content = messages_content[message]
            if message_content != "":
                yield message_content
