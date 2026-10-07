import os
from typing import Any, TypeVar

from langchain.chat_models import init_chat_model
from langchain_core.runnables import RunnableConfig
from pydantic import BaseModel

Schema = TypeVar("Schema", bound=BaseModel)


def resolve_model(model: str | Any | None = None) -> Any:
    model = model or os.getenv("DATA_REFINER_AGENT_MODEL", "openai:gpt-5.6-luna")
    if not isinstance(model, str):
        return model
    if ":" not in model:
        raise ValueError("DATA_REFINER_AGENT_MODEL must use provider:model format")
    return init_chat_model(model)


def invoke_structured(
    model: Any,
    schema: type[Schema],
    prompt: str,
    config: RunnableConfig,
) -> Schema:
    result = model.with_structured_output(schema).invoke(prompt, config=config)
    return schema.model_validate(result)


__all__ = ["invoke_structured", "resolve_model"]
