from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

from hdfs import HdfsError, InsecureClient


class WebHDFSStorage:
    """基于 WebHDFS 的 HDFS 文件操作封装。"""

    def __init__(
        self,
        namenode_url: str = "http://master:9870",
        user: str = "jupyter",
        timeout: int = 300,
        root_dir: str = "/",
    ) -> None:
        self.namenode_url = namenode_url.rstrip("/")
        self.user = user
        self.timeout = timeout
        self.root_dir = self._normalize_absolute_path(root_dir)

        self.client = InsecureClient(
            url=self.namenode_url,
            user=self.user,
            timeout=self.timeout,
        )

    @staticmethod
    def _normalize_absolute_path(path: str) -> str:
        """
        规范化为 HDFS 绝对路径，并阻止明显的路径穿越。
        """
        if not path:
            raise ValueError("HDFS path cannot be empty")

        normalized = str(PurePosixPath("/", path))

        if ".." in PurePosixPath(path).parts:
            raise ValueError(f"Path traversal is not allowed: {path}")

        return normalized

    def _resolve_path(self, path: str) -> str:
        """
        将传入路径限制在 root_dir 下。
        """
        if path.startswith("/"):
            normalized = self._normalize_absolute_path(path)
        else:
            normalized = self._normalize_absolute_path(
                f"{self.root_dir.rstrip('/')}/{path}"
            )

        if self.root_dir != "/":
            root_prefix = self.root_dir.rstrip("/") + "/"
            if normalized != self.root_dir and not normalized.startswith(root_prefix):
                raise ValueError(
                    f"Path must stay inside root directory: {self.root_dir}"
                )

        return normalized

    # -------------------------
    # 查询操作
    # -------------------------

    def exists(self, hdfs_path: str) -> bool:
        path = self._resolve_path(hdfs_path)
        return self.client.status(path, strict=False) is not None

    def status(self, hdfs_path: str) -> dict[str, Any] | None:
        path = self._resolve_path(hdfs_path)
        return self.client.status(path, strict=False)

    def list_dir(
        self,
        hdfs_path: str = "/",
        include_status: bool = False,
    ) -> list[Any]:
        path = self._resolve_path(hdfs_path)
        return self.client.list(path, status=include_status)

    # -------------------------
    # 目录操作
    # -------------------------

    def mkdir(
        self,
        hdfs_path: str,
        permission: int | str = 755,
    ) -> None:
        path = self._resolve_path(hdfs_path)
        self.client.makedirs(path, permission=permission)

    # -------------------------
    # 本地文件上传、下载
    # -------------------------

    def upload_file(
        self,
        local_path: str | Path,
        hdfs_path: str,
        overwrite: bool = False,
    ) -> str:
        local = Path(local_path)

        if not local.exists():
            raise FileNotFoundError(f"Local file does not exist: {local}")

        if not local.is_file():
            raise ValueError(f"Local path is not a file: {local}")

        target = self._resolve_path(hdfs_path)

        parent = str(PurePosixPath(target).parent)
        self.client.makedirs(parent)

        result = self.client.upload(
            hdfs_path=target,
            local_path=str(local),
            overwrite=overwrite,
            n_threads=1,
        )

        return str(result)

    def upload_directory(
        self,
        local_dir: str | Path,
        hdfs_dir: str,
        overwrite: bool = False,
        n_threads: int = 4,
    ) -> str:
        local = Path(local_dir)

        if not local.exists():
            raise FileNotFoundError(f"Local directory does not exist: {local}")

        if not local.is_dir():
            raise ValueError(f"Local path is not a directory: {local}")

        target = self._resolve_path(hdfs_dir)
        self.client.makedirs(target)

        result = self.client.upload(
            hdfs_path=target,
            local_path=str(local),
            overwrite=overwrite,
            n_threads=n_threads,
        )

        return str(result)

    def download_file(
        self,
        hdfs_path: str,
        local_path: str | Path,
        overwrite: bool = False,
    ) -> str:
        source = self._resolve_path(hdfs_path)

        result = self.client.download(
            hdfs_path=source,
            local_path=str(local_path),
            overwrite=overwrite,
        )

        return str(result)

    # -------------------------
    # 字符串、字节写入
    # -------------------------

    def write_text(
        self,
        hdfs_path: str,
        content: str,
        encoding: str = "utf-8",
        overwrite: bool = False,
        permission: int | str = 644,
    ) -> None:
        path = self._resolve_path(hdfs_path)
        parent = str(PurePosixPath(path).parent)
        self.client.makedirs(parent)

        self.client.write(
            hdfs_path=path,
            data=content,
            encoding=encoding,
            overwrite=overwrite,
            permission=permission,
        )

    def write_bytes(
        self,
        hdfs_path: str,
        content: bytes,
        overwrite: bool = False,
        permission: int | str = 644,
    ) -> None:
        path = self._resolve_path(hdfs_path)
        parent = str(PurePosixPath(path).parent)
        self.client.makedirs(parent)

        self.client.write(
            hdfs_path=path,
            data=content,
            overwrite=overwrite,
            permission=permission,
        )

    def write_json(
        self,
        hdfs_path: str,
        data: Any,
        overwrite: bool = False,
        ensure_ascii: bool = False,
        indent: int | None = 2,
    ) -> None:
        content = json.dumps(
            data,
            ensure_ascii=ensure_ascii,
            indent=indent,
        )

        self.write_text(
            hdfs_path=hdfs_path,
            content=content,
            encoding="utf-8",
            overwrite=overwrite,
        )

    # -------------------------
    # 字符串、字节读取
    # -------------------------

    def read_text(
        self,
        hdfs_path: str,
        encoding: str = "utf-8",
    ) -> str:
        """
        将整个 HDFS 文件读入内存并返回字符串。

        只适合小文件，例如配置、JSON、日志片段、SQL、YAML。
        """
        path = self._resolve_path(hdfs_path)

        with self.client.read(path, encoding=encoding) as reader:
            return reader.read()

    def read_bytes(self, hdfs_path: str) -> bytes:
        path = self._resolve_path(hdfs_path)

        with self.client.read(path) as reader:
            return reader.read()

    def read_json(
        self,
        hdfs_path: str,
        encoding: str = "utf-8",
    ) -> Any:
        content = self.read_text(
            hdfs_path=hdfs_path,
            encoding=encoding,
        )
        return json.loads(content)

    def read_text_range(
        self,
        hdfs_path: str,
        offset: int = 0,
        length: int | None = None,
        encoding: str = "utf-8",
    ) -> str:
        path = self._resolve_path(hdfs_path)

        with self.client.read(
            path,
            offset=offset,
            length=length,
        ) as reader:
            data = reader.read()

        return data.decode(encoding)

    def iter_bytes(
        self,
        hdfs_path: str,
        chunk_size: int = 1024 * 1024,
    ) -> Iterator[bytes]:
        """
        分块读取大文件，默认每块 1 MiB。
        """
        path = self._resolve_path(hdfs_path)

        with self.client.read(
            path,
            chunk_size=chunk_size,
        ) as chunks:
            yield from chunks

    def iter_lines(
        self,
        hdfs_path: str,
        encoding: str = "utf-8",
    ) -> Iterator[str]:
        """
        逐行读取文本文件，避免一次性加载整个文件。
        """
        path = self._resolve_path(hdfs_path)

        with self.client.read(
            path,
            encoding=encoding,
            delimiter="\n",
        ) as lines:
            for line in lines:
                yield line

    # -------------------------
    # 修改操作
    # -------------------------

    def overwrite_text(
        self,
        hdfs_path: str,
        content: str,
        encoding: str = "utf-8",
    ) -> None:
        self.write_text(
            hdfs_path=hdfs_path,
            content=content,
            encoding=encoding,
            overwrite=True,
        )

    def append_text(
        self,
        hdfs_path: str,
        content: str,
        encoding: str = "utf-8",
    ) -> None:
        path = self._resolve_path(hdfs_path)

        self.client.write(
            hdfs_path=path,
            data=content,
            encoding=encoding,
            append=True,
        )

    def update_text_safely(
        self,
        hdfs_path: str,
        update_function,
        encoding: str = "utf-8",
    ) -> str:
        """
        读取旧文本，调用 update_function 修改，然后整体覆盖。

        仅适合小文件，不适合大数据文件。
        """
        old_content = self.read_text(
            hdfs_path=hdfs_path,
            encoding=encoding,
        )

        new_content = update_function(old_content)

        if not isinstance(new_content, str):
            raise TypeError("update_function must return str")

        self.overwrite_text(
            hdfs_path=hdfs_path,
            content=new_content,
            encoding=encoding,
        )

        return new_content

    def rename(
        self,
        source_path: str,
        destination_path: str,
    ) -> None:
        source = self._resolve_path(source_path)
        destination = self._resolve_path(destination_path)

        destination_parent = str(PurePosixPath(destination).parent)
        self.client.makedirs(destination_parent)

        self.client.rename(source, destination)

    # -------------------------
    # 删除操作
    # -------------------------

    def delete(
        self,
        hdfs_path: str,
        recursive: bool = False,
        skip_trash: bool = False,
    ) -> bool:
        path = self._resolve_path(hdfs_path)

        return self.client.delete(
            hdfs_path=path,
            recursive=recursive,
            skip_trash=skip_trash,
        )


def main() -> None:
    storage = WebHDFSStorage(
        namenode_url="http://master:9870",
        user="jupyter",
        root_dir="/",
    )
    # storage.upload_file("dist/data_refiner_user_workspace-0.0.1-py3-none-any.whl",f"/workspaces/@sonder/",True)

    # storage.upload_directory("./workspace","/test")
    storage.delete("/workspaces", recursive=True)


if __name__ == "__main__":
    try:
        main()
    except HdfsError as exc:
        raise RuntimeError(f"WebHDFS operation failed: {exc}") from exc
