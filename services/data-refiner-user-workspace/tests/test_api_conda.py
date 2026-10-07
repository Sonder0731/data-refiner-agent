import asyncio
import json
from pathlib import Path
from subprocess import CompletedProcess

from api import main


def test_startup_syncs_conda_then_workspace(monkeypatch):
    calls = []
    monkeypatch.setattr(main, "package_conda_env", lambda: calls.append("conda"))
    monkeypatch.setattr(main, "sync_workspace", lambda: calls.append("workspace"))
    monkeypatch.setattr(
        main,
        "sync_runtime_resources",
        lambda: calls.append("runtime-resources"),
    )

    async def run_lifespan():
        async with main.lifespan(main.app):
            calls.append("ready")

    asyncio.run(run_lifespan())
    assert calls == ["conda", "workspace", "runtime-resources", "ready"]


def test_sync_runtime_resources_uploads_archive(tmp_path, monkeypatch):
    archive = tmp_path / "data-refiner-runtime-resources.zip"
    archive.touch()
    monkeypatch.setattr(main, "RUNTIME_RESOURCES", archive)
    monkeypatch.setenv("USER_NAME", "alice")
    uploads = []

    class Storage:
        def __init__(self, **kwargs):
            pass

        def upload_file(self, local_path, hdfs_path, overwrite=False):
            uploads.append((Path(local_path), hdfs_path, overwrite))

    monkeypatch.setattr(main, "WebHDFSStorage", Storage)

    result = main.sync_runtime_resources()

    assert uploads == [
        (archive, "/workspaces/alice/data-refiner-runtime-resources.zip", True)
    ]
    assert result["hdfs_path"] == uploads[0][1]


def test_run_pytest_syncs_conda_only_after_success(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    test_file = workspace / "tests" / "test_operator.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_operator(): pass", encoding="utf-8")
    monkeypatch.setattr(main, "WORKSPACE", workspace)

    exit_code = 0
    monkeypatch.setattr(
        main.subprocess,
        "run",
        lambda command, **kwargs: CompletedProcess(command, exit_code, "", ""),
    )
    sync_calls = []
    monkeypatch.setattr(
        main,
        "sync_conda_dependencies",
        lambda: sync_calls.append(True) or {"exit_code": 0},
    )

    assert main.run_pytest(main.PytestRequest(path=str(test_file)))["conda_sync"] == {
        "exit_code": 0
    }
    exit_code = 1
    assert main.run_pytest(main.PytestRequest(path=str(test_file)))["conda_sync"] is None
    assert sync_calls == [True]


def test_package_conda_env_uses_conda_pack(tmp_path, monkeypatch):
    env_path = tmp_path / main.CONDA_ENV
    env_path.mkdir()
    monkeypatch.setenv("USER_NAME", "alice")
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        if command[:3] == ["conda", "info", "--envs"]:
            return CompletedProcess(
                command, 0, json.dumps({"envs": [str(env_path)]}), ""
            )
        Path(command[command.index("--output") + 1]).touch()
        return CompletedProcess(command, 0, "", "")

    uploads = []

    class Storage:
        def __init__(self, **kwargs):
            pass

        def upload_file(self, local_path, hdfs_path, overwrite=False):
            uploads.append((Path(local_path).name, hdfs_path, overwrite))

    monkeypatch.setattr(main.subprocess, "run", run)
    monkeypatch.setattr(main, "WebHDFSStorage", Storage)

    result = main.package_conda_env()
    pack_command = commands[1]
    assert pack_command[0] == str(main.CONDA_PACK)
    assert pack_command[pack_command.index("--format") + 1] == "tar.gz"
    assert pack_command[pack_command.index("--n-threads") + 1] == "-1"
    assert result["hdfs_path"].endswith("/data_refiner_env.tar.gz")
    assert uploads == [("data_refiner_env.tar.gz", result["hdfs_path"], True)]
