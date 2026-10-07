from pathlib import Path
from subprocess import CompletedProcess

import pytest
from fastapi import HTTPException

from api import main


def test_write_and_run_pytest(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "WORKSPACE", tmp_path)

    assert main.write_file(main.FileRequest(path="tests/test_ok.py", content="def test_ok(): pass")) == {
        "path": "tests/test_ok.py"
    }
    assert (tmp_path / "tests/test_ok.py").is_file()

    monkeypatch.setattr(
        main.subprocess,
        "run",
        lambda *args, **kwargs: CompletedProcess(args[0], 0, "1 passed", ""),
    )
    assert main.run_pytest(main.PytestRequest(path="tests/test_ok.py"))["exit_code"] == 0

    with pytest.raises(HTTPException):
        main.workspace_path("../outside.txt")
