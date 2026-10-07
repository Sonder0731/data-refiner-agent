from pathlib import Path

class LocalPath:
    @staticmethod
    def repo_root() -> Path:
        # deal with add .whl to --py-files lead to wrong repo_root
        repo_root = Path(__file__).parent
        return repo_root

    @staticmethod
    def workspace_root() -> Path:
        return LocalPath.repo_root().joinpath("workspace")

    @staticmethod
    def workspace_ops_root() -> Path:
        return LocalPath.workspace_root().joinpath("ops")

    @staticmethod
    def workspace_data_root() -> Path:
        return LocalPath.workspace_root().joinpath("test_data")

    @staticmethod
    def workspace_mapper_ops_root() -> Path:
        return LocalPath.workspace_ops_root().joinpath("mapper")

    @staticmethod
    def workspace_filter_ops_root() -> Path:
        return LocalPath.workspace_ops_root().joinpath("filter")

    @staticmethod
    def workspace_deduplicator_ops_root() -> Path:
        return LocalPath.workspace_ops_root().joinpath("deduplicator")

    @staticmethod
    def workspace_reducer_ops_root() -> Path:
        return LocalPath.workspace_ops_root().joinpath("reducer")

    @staticmethod
    def workspace_sampler_ops_root() -> Path:
        return LocalPath.workspace_ops_root().joinpath("sampler")

    @staticmethod
    def workspace_reader_ops_root() -> Path:
        return LocalPath.workspace_ops_root().joinpath("reader")

    @staticmethod
    def workspace_writer_root() -> Path:
        return LocalPath.workspace_ops_root().joinpath("writer")

    @staticmethod
    def workspace_test_root() -> Path:
        return LocalPath.workspace_root().joinpath("tests")

    @staticmethod
    def workspace_test_operator_root() -> Path:
        return LocalPath.workspace_test_root().joinpath("ops")

    @staticmethod
    def workspace_test_pipeline_root() -> Path:
        return LocalPath.workspace_test_root().joinpath("pipeline")

    @staticmethod
    def workspace_test_pipeline_conf_root() -> Path:
        return LocalPath.workspace_test_pipeline_root().joinpath("pipeline_cfg_files")

    @staticmethod
    def workspace_test_mapper_root() -> Path:
        return LocalPath.workspace_test_operator_root().joinpath("mapper")

    @staticmethod
    def workspace_test_filter_root() -> Path:
        return LocalPath.workspace_test_operator_root().joinpath("filter")

    @staticmethod
    def workspace_test_deduplicator_root() -> Path:
        return LocalPath.workspace_test_operator_root().joinpath("deduplicator")

    @staticmethod
    def workspace_test_reducer_root() -> Path:
        return LocalPath.workspace_test_operator_root().joinpath("reducer")

    @staticmethod
    def workspace_test_sampler_root() -> Path:
        return LocalPath.workspace_test_operator_root().joinpath("sampler")

    @staticmethod
    def workspace_test_reader_root() -> Path:
        return LocalPath.workspace_test_operator_root().joinpath("reader")

    @staticmethod
    def workspace_test_writer_root() -> Path:
        return LocalPath.workspace_test_operator_root().joinpath("writer")

    @staticmethod
    def workspace_operator_docs_root() -> Path:
        return LocalPath.workspace_root().joinpath("docs")

    @staticmethod
    def workspace_builtin_ops_doc_root() -> Path:
        return LocalPath.workspace_operator_docs_root().joinpath("builtin")

    @staticmethod
    def workspace_deduplicator_ops_doc_root() -> Path:
        return LocalPath.workspace_operator_docs_root().joinpath("deduplicator")

    @staticmethod
    def workspace_filter_ops_doc_root() -> Path:
        return LocalPath.workspace_operator_docs_root().joinpath("filter")

    @staticmethod
    def workspace_mapper_ops_doc_root() -> Path:
        return LocalPath.workspace_operator_docs_root().joinpath("mapper")

    @staticmethod
    def workspace_other_operator_doc_root() -> Path:
        return LocalPath.workspace_operator_docs_root().joinpath("other")

    @staticmethod
    def workspace_reader_ops_doc_root() -> Path:
        return LocalPath.workspace_operator_docs_root().joinpath("reader")

    @staticmethod
    def workspace_reducer_ops_doc_root() -> Path:
        return LocalPath.workspace_operator_docs_root().joinpath("reducer")

    @staticmethod
    def workspace_sampler_ops_doc_root() -> Path:
        return LocalPath.workspace_operator_docs_root().joinpath("sampler")

    @staticmethod
    def workspace_writer_ops_doc_root() -> Path:
        return LocalPath.workspace_operator_docs_root().joinpath("writer")

    @staticmethod
    def workspace_jar_root() -> Path:
        return LocalPath.workspace_root().joinpath("jar")

    @staticmethod
    def workspace_checkpoint_root() -> Path:
        return LocalPath.workspace_root().joinpath("checkpoint")

    @staticmethod
    def workspace_pipeline_root() -> Path:
        return LocalPath.workspace_root().joinpath("pipelines")

    @staticmethod
    def temp_dir() -> Path:
        return LocalPath.repo_root().joinpath("temp")