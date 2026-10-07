"""ASGI and direct script entry point for the API service."""

import uvicorn

from data_refiner_api.main import app, create_app

__all__ = ["app", "create_app", "main"]


def main() -> None:
    """Start the API service when this file is executed directly."""
    uvicorn.run(
        "data_refiner_api.main:app",
        host="0.0.0.0",
        port=8000,
    )


if __name__ == "__main__":
    main()
