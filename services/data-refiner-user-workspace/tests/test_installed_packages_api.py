from api import main


def test_get_installed_packages(monkeypatch):
    class Distribution:
        def __init__(self, name, version):
            self.name = name
            self.version = version

    monkeypatch.setattr(
        main,
        "distributions",
        lambda: [Distribution("Zeta", "2.0"), Distribution("alpha", "1.0")],
    )

    result = main.get_installed_packages()

    assert result.python == main.sys.executable
    assert [package.model_dump() for package in result.packages] == [
        {"name": "alpha", "version": "1.0"},
        {"name": "Zeta", "version": "2.0"},
    ]
