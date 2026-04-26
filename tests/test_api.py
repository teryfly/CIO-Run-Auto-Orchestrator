"""
test_api.py — Unit tests for src/api.py

Coverage targets:
- POST /tasks: creates new task
- POST /tasks: dedup by find_incomplete
- POST /tasks: dedup by find_by_digest
- POST /tasks: missing project_name → 422
- POST /tasks: empty project_name → 422
- POST /tasks: requirement_file_content field → 422
- POST /tasks: empty requirement accepted (pure resume)
- GET /tasks/{task_id}: found → 200
- GET /tasks/{task_id}: not found → 404
- GET /health → 200
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from src.api import app
from src.models import Task, TaskMode, TaskStatus


def make_app_client(
    task_store=None,
    task_queue=None,
    event_bus=None,
):
    """Build a TestClient with mocked app state."""
    store = task_store or MagicMock()
    queue = task_queue or MagicMock()
    bus = event_bus or MagicMock()

    app.state.task_store = store
    app.state.task_queue = queue
    app.state.event_bus = bus

    return TestClient(app, raise_server_exceptions=False), store, queue, bus


class TestCreateTask:
    def test_creates_new_task(self):
        store = MagicMock()
        store.find_incomplete = AsyncMock(return_value=None)
        store.find_by_digest = AsyncMock(return_value=None)
        store.save = AsyncMock()

        queue = MagicMock()
        queue.enqueue = AsyncMock()

        client, _, _, _ = make_app_client(task_store=store, task_queue=queue)

        resp = client.post("/tasks", json={
            "project_name": "my-proj",
            "requirement": "Build it",
        })

        assert resp.status_code == 202
        data = resp.json()
        assert data["project_name"] == "my-proj"
        assert data["requirement"] == "Build it"
        assert data["status"] == "pending"
        store.save.assert_awaited_once()
        queue.enqueue.assert_awaited_once()

    def test_dedup_by_find_incomplete(self):
        existing = Task(project_name="proj", requirement="req")
        store = MagicMock()
        store.find_incomplete = AsyncMock(return_value=existing)

        client, _, _, _ = make_app_client(task_store=store)

        resp = client.post("/tasks", json={"project_name": "proj", "requirement": "req"})

        assert resp.status_code == 202
        assert resp.json()["task_id"] == existing.task_id

    def test_dedup_by_find_by_digest(self):
        existing = Task(project_name="proj", requirement="req")
        store = MagicMock()
        store.find_incomplete = AsyncMock(return_value=None)
        store.find_by_digest = AsyncMock(return_value=existing)

        client, _, _, _ = make_app_client(task_store=store)

        resp = client.post("/tasks", json={"project_name": "proj", "requirement": "req"})

        assert resp.status_code == 202
        assert resp.json()["task_id"] == existing.task_id

    def test_missing_project_name_returns_422(self):
        client, _, _, _ = make_app_client()
        resp = client.post("/tasks", json={"requirement": "do it"})
        assert resp.status_code == 422

    def test_empty_project_name_returns_422(self):
        client, _, _, _ = make_app_client()
        resp = client.post("/tasks", json={"project_name": "", "requirement": "do it"})
        assert resp.status_code == 422

    def test_requirement_file_content_returns_422(self):
        """CTD change: requirement_file_content field is no longer accepted."""
        client, _, _, _ = make_app_client()
        resp = client.post("/tasks", json={
            "project_name": "proj",
            "requirement": "do it",
            "requirement_file_content": "some content",
        })
        assert resp.status_code == 422

    def test_empty_requirement_accepted(self):
        """Empty requirement should be accepted (pure resume scenario)."""
        store = MagicMock()
        store.find_incomplete = AsyncMock(return_value=None)
        store.find_by_digest = AsyncMock(return_value=None)
        store.save = AsyncMock()

        queue = MagicMock()
        queue.enqueue = AsyncMock()

        client, _, _, _ = make_app_client(task_store=store, task_queue=queue)

        resp = client.post("/tasks", json={"project_name": "proj"})

        assert resp.status_code == 202
        assert resp.json()["requirement"] == ""

    def test_no_requirement_field_accepted(self):
        """Omitting requirement entirely should work (defaults to "")."""
        store = MagicMock()
        store.find_incomplete = AsyncMock(return_value=None)
        store.find_by_digest = AsyncMock(return_value=None)
        store.save = AsyncMock()

        queue = MagicMock()
        queue.enqueue = AsyncMock()

        client, _, _, _ = make_app_client(task_store=store, task_queue=queue)
        resp = client.post("/tasks", json={"project_name": "proj"})
        assert resp.status_code == 202


class TestGetTask:
    def test_returns_task_when_found(self):
        task = Task(project_name="proj", requirement="req")
        store = MagicMock()
        store.get = AsyncMock(return_value=task)

        client, _, _, _ = make_app_client(task_store=store)
        resp = client.get(f"/tasks/{task.task_id}")

        assert resp.status_code == 200
        assert resp.json()["task_id"] == task.task_id

    def test_returns_404_when_not_found(self):
        store = MagicMock()
        store.get = AsyncMock(return_value=None)

        client, _, _, _ = make_app_client(task_store=store)
        resp = client.get("/tasks/nonexistent")

        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    def test_task_fields_in_response(self):
        task = Task(project_name="proj", requirement="req")
        task.transition(TaskStatus.RUNNING, detail="mode=new")
        object.__setattr__(task, "mode", TaskMode.NEW)

        store = MagicMock()
        store.get = AsyncMock(return_value=task)

        client, _, _, _ = make_app_client(task_store=store)
        resp = client.get(f"/tasks/{task.task_id}")

        data = resp.json()
        assert data["status"] == "running"
        assert data["mode"] == "new"
        assert data["status_detail"] == "mode=new"


class TestHealth:
    def test_health_returns_ok(self):
        client, _, _, _ = make_app_client()
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}
