import logging
from decimal import Decimal
from functools import cached_property

from sqlalchemy import Column, ForeignKey, Table, types
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import backref, relationship

from database.base import CodecovBaseModel, MixinBaseClass
from database.models.core import Commit, CompareCommit, Repository
from helpers.number import precise_round

log = logging.getLogger(__name__)


class RepositoryFlag(CodecovBaseModel, MixinBaseClass):
    __tablename__ = "reports_repositoryflag"
    repository_id = Column(types.Integer, ForeignKey("repos.repoid"))
    repository = relationship(Repository, backref=backref("flags"))
    flag_name = Column(types.String(1024), nullable=False)
    deleted = Column(types.Boolean, nullable=True)


class CommitReport(CodecovBaseModel, MixinBaseClass):
    __tablename__ = "reports_commitreport"
    commit_id = Column(types.BigInteger, ForeignKey("commits.id"))
    code = Column(types.String(100), nullable=True)
    report_type = Column(types.String(100), nullable=True)
    commit: Commit = relationship(
        "Commit",
        foreign_keys=[commit_id],
        back_populates="reports_list",
        cascade="all, delete",
    )
    totals = relationship(
        "ReportLevelTotals",
        back_populates="report",
        uselist=False,
        cascade="all, delete",
        passive_deletes=True,
    )
    uploads = relationship(
        "Upload", back_populates="report", cascade="all, delete", passive_deletes=True
    )


uploadflagmembership = Table(
    "reports_uploadflagmembership",
    CodecovBaseModel.metadata,
    Column("upload_id", types.BigInteger, ForeignKey("reports_upload.id")),
    Column("flag_id", types.BigInteger, ForeignKey("reports_repositoryflag.id")),
)


class Upload(CodecovBaseModel, MixinBaseClass):
    __tablename__ = "reports_upload"
    build_code = Column(types.Text)
    build_url = Column(types.Text)
    env = Column(postgresql.JSON)
    job_code = Column(types.Text)
    name = Column(types.String(100))
    provider = Column(types.String(50))
    report_id = Column(types.BigInteger, ForeignKey("reports_commitreport.id"))
    report: CommitReport = relationship(
        "CommitReport", foreign_keys=[report_id], back_populates="uploads"
    )
    state = Column(types.String(100), nullable=False)
    storage_path = Column(types.Text, nullable=False)
    order_number = Column(types.Integer)
    flags = relationship(RepositoryFlag, secondary=uploadflagmembership)
    totals = relationship(
        "UploadLevelTotals",
        back_populates="upload",
        uselist=False,
        cascade="all, delete",
        passive_deletes=True,
    )
    upload_extras = Column(postgresql.JSON, nullable=False)
    upload_type = Column(types.String(100), nullable=False)
    state_id = Column(types.Integer)
    upload_type_id = Column(types.Integer)

    @cached_property
    def flag_names(self) -> list[str]:
        return [f.flag_name for f in self.flags]


class UploadError(CodecovBaseModel, MixinBaseClass):
    __tablename__ = "reports_uploaderror"
    report_upload = relationship(Upload, backref="errors")
    upload_id = Column("upload_id", types.BigInteger, ForeignKey("reports_upload.id"))
    error_code = Column(types.String(100), nullable=False)
    error_params = Column(postgresql.JSON, default=dict)


class AbstractTotals(MixinBaseClass):
    branches = Column(types.Integer)
    coverage = Column(types.Numeric(precision=8, scale=5))
    hits = Column(types.Integer)
    lines = Column(types.Integer)
    methods = Column(types.Integer)
    misses = Column(types.Integer)
    partials = Column(types.Integer)
    files = Column(types.Integer)

    def update_from_totals(self, totals, precision=2, rounding="down"):
        self.branches = totals.branches
        if totals.coverage is not None:
            coverage: Decimal = Decimal(totals.coverage)
            self.coverage = precise_round(
                coverage, precision=precision, rounding=rounding
            )
        # Temporary until the table starts accepting NULLs
        else:
            self.coverage = 0
        self.hits = totals.hits
        self.lines = totals.lines
        self.methods = totals.methods
        self.misses = totals.misses
        self.partials = totals.partials
        self.files = totals.files

    class Meta:
        abstract = True


class ReportLevelTotals(CodecovBaseModel, AbstractTotals):
    __tablename__ = "reports_reportleveltotals"
    report_id = Column(types.BigInteger, ForeignKey("reports_commitreport.id"))
    report = relationship("CommitReport", foreign_keys=[report_id])


class UploadLevelTotals(CodecovBaseModel, AbstractTotals):
    __tablename__ = "reports_uploadleveltotals"
    upload_id = Column("upload_id", types.BigInteger, ForeignKey("reports_upload.id"))
    upload = relationship("Upload", foreign_keys=[upload_id])


class CompareFlag(MixinBaseClass, CodecovBaseModel):
    __tablename__ = "compare_flagcomparison"

    commit_comparison_id = Column(
        types.BigInteger, ForeignKey("compare_commitcomparison.id")
    )
    repositoryflag_id = Column(
        types.BigInteger, ForeignKey("reports_repositoryflag.id")
    )
    head_totals = Column(postgresql.JSON)
    base_totals = Column(postgresql.JSON)
    patch_totals = Column(postgresql.JSON)

    commit_comparison = relationship(CompareCommit, foreign_keys=[commit_comparison_id])
    repositoryflag = relationship(RepositoryFlag, foreign_keys=[repositoryflag_id])


class CompareComponent(MixinBaseClass, CodecovBaseModel):
    __tablename__ = "compare_componentcomparison"

    commit_comparison_id = Column(
        types.BigInteger, ForeignKey("compare_commitcomparison.id")
    )
    component_id = Column(types.String(100), nullable=False)
    head_totals = Column(postgresql.JSON)
    base_totals = Column(postgresql.JSON)
    patch_totals = Column(postgresql.JSON)

    commit_comparison = relationship(CompareCommit, foreign_keys=[commit_comparison_id])
