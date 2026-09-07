class Flag:
    def __init__(
        self, report, name, totals=None, carriedforward=False, carriedforward_from=None
    ):
        self._report = report
        self.name = name
        # TODO cache by storing in database
        self._totals = totals
        self.carriedforward = carriedforward
        self.carriedforward_from = carriedforward_from
        self._filtered_report = None

    @property
    def report(self):
        """returns the report filtered by this flag"""
        if self._filtered_report is None:
            self._filtered_report = self._report.filter(paths=None, flags=[self.name])
        return self._filtered_report

    @property
    def totals(self):
        if not self._totals:
            self._totals = self.report.totals
        return self._totals

    def apply_diff(self, diff):
        return self.report.apply_diff(diff, _save=False)
