"""Filesystem locations owned by the API runtime."""

from pathlib import Path


class LocalPath:
    """Resolve files relative to the API project root."""

    @staticmethod
    def repo_root() -> Path:
        return Path(__file__).resolve().parent.parent

    @staticmethod
    def workspace_root() -> Path:
        return LocalPath.repo_root().joinpath("workspace")

    @staticmethod
    def workspace_pipeline_root() -> Path:
        return LocalPath.workspace_root().joinpath("pipelines")
