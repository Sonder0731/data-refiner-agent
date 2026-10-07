from subprocess import CompletedProcess

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from api import main


def test_add_package(monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return CompletedProcess(command, 0, "added", "")

    monkeypatch.setattr(main.subprocess, "run", run)

    assert main.add_package(main.PackageRequest(package_name="requests")) == {
        "status": "succeeded",
        "exit_code": 0,
        "stdout": "added",
        "stderr": "",
    }
    assert calls[0][0] == ["uv", "add", "requests"]
    assert calls[0][1]["timeout"] == 600

    with pytest.raises(ValidationError):
        main.PackageRequest(package_name="--dev")
    with pytest.raises(ValidationError):
        main.PackageRequest(package_name="pypinyin", version="latest stable")


def test_add_package_pins_exact_version(monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        return CompletedProcess(command, 0, "added", "")

    monkeypatch.setattr(main.subprocess, "run", run)

    result = main.add_package(
        main.PackageRequest(package_name="pypinyin", version="0.55.0")
    )

    assert result["status"] == "succeeded"
    assert calls == [["uv", "add", "pypinyin==0.55.0"]]


def test_add_package_reports_uv_failure(monkeypatch):
    monkeypatch.setattr(
        main.subprocess,
        "run",
        lambda command, **kwargs: CompletedProcess(command, 1, "", "not found"),
    )

    with pytest.raises(HTTPException, match="uv add failed"):
        main.add_package(main.PackageRequest(package_name="missing"))
