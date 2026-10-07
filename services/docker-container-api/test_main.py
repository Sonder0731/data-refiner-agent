import os
import unittest
from unittest.mock import MagicMock, patch

from docker.errors import NotFound
from fastapi import HTTPException
from starlette.responses import Response

os.environ.setdefault("CONTAINER_API_KEY", "test-key")

import main


class CreateContainerTest(unittest.TestCase):
    def test_auth_and_docker_options(self):
        request = main.CreateContainerRequest(
            image="data-refiner-user-workspace:0.0.1",
            name="data-refiner-test",
            command=["sleep", "60"],
            environment={"MODE": "test"},
            ports={8000: 18000},
            volumes={"refiner-data": {"target": "/workspace"}},
            restart_policy="unless-stopped",
            memory="512m",
        )

        with self.assertRaises(HTTPException) as error:
            main.create_container(request, Response(), "wrong-key")
        self.assertEqual(error.exception.status_code, 401)

        container = MagicMock()
        container.id = "container-id"
        container.name = "data-refiner-test"
        container.status = "running"
        client = MagicMock()
        client.containers.get.side_effect = NotFound("not found")
        client.containers.run.return_value = container
        with patch.object(main.docker, "from_env", return_value=client):
            response = main.create_container(request, Response(), main.API_KEY)

        self.assertEqual(response.id, "container-id")
        options = client.containers.run.call_args.kwargs
        self.assertEqual(options["ports"], {8000: 18000})
        self.assertEqual(options["volumes"]["refiner-data"]["bind"], "/workspace")
        client.close.assert_called_once()

    def test_existing_container_returns_200(self):
        request = main.CreateContainerRequest(image="workspace:latest", name="existing")
        container = MagicMock()
        container.id = "container-id"
        container.name = "existing"
        container.status = "running"
        client = MagicMock()
        client.containers.get.return_value = container
        http_response = Response(status_code=201)

        with patch.object(main.docker, "from_env", return_value=client):
            response = main.create_container(request, http_response, main.API_KEY)

        self.assertEqual(http_response.status_code, 200)
        self.assertEqual((response.name, response.status), ("existing", "running"))
        client.containers.run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
