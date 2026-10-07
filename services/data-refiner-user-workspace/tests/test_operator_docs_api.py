import pytest

from api import main


def test_operator_docs_endpoints(tmp_path, monkeypatch):
    docs = tmp_path / "docs"
    (docs / "filter").mkdir(parents=True)
    (docs / "ops_market.md").write_text("market", encoding="utf-8")
    (docs / "filter" / "length_filter.md").write_text("details", encoding="utf-8")
    (tmp_path / "ops" / "filter").mkdir(parents=True)
    (tmp_path / "ops" / "filter" / "length_filter.py").write_text(
        "operator code", encoding="utf-8"
    )
    (tmp_path / "tests" / "filter").mkdir(parents=True)
    (tmp_path / "tests" / "filter" / "test_length_filter.py").write_text(
        "test code", encoding="utf-8"
    )
    monkeypatch.setattr(main, "WORKSPACE", tmp_path)

    assert main.get_ops_market().result == "market"
    assert (
        main.get_processing_operator_docs(main.OperatorType.filter, "length_filter").result
        == "details"
    )
    assert main.get_operator_code(main.OperatorType.filter, "length_filter").result == "operator code"
    assert (
        main.get_operator_test_code(main.OperatorType.filter, "length_filter").result
        == "test code"
    )
    assert main.get_processing_operator_docs_by_name("length_filter").result == "details"
    assert main.get_processing_operator_code_by_name("length_filter").result == "operator code"
    assert (
        main.get_processing_operator_test_code_by_name("length_filter").result
        == "test code"
    )
    assert {route.path for route in main.app.routes} >= {
        "/ops_market",
        "/processing_operator/{operator_name}/docs",
        "/processing_operator/{operator_name}/code",
        "/processing_operator/{operator_name}/test-code",
        "/{operator_type}/{operator_name}/docs",
        "/{operator_type}/{operator_name}/code",
        "/{operator_type}/{operator_name}/test-code",
    }

    with pytest.raises(main.HTTPException) as exc_info:
        main.get_processing_operator_code_by_name("missing_operator")

    assert exc_info.value.status_code == 404
