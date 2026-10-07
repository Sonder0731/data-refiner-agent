from importlib.resources import files


def load_prompt(agent_name: str) -> str:
    return files(__package__).joinpath(f"{agent_name}.md").read_text(encoding="utf-8")


__all__ = ["load_prompt"]
