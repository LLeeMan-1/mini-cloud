from datetime import timedelta

import pytest

from test_replicas import setup, deployment, reconcile, worker, claim


def test_empty_discovery_and_legacy_list(client):
    assert client.get("/deployments/page").json() == {"items": [], "snapshot_max_id": 0, "next_after_id": None}
    assert client.get("/deployments").json() == []


def test_keyset_pages_keep_upper_bound_during_creation(setup):
    client = setup[0]
    first_id, second_id = deployment(setup), deployment(setup)
    old_list = client.get("/deployments").json()
    first = client.get("/deployments/page?limit=1").json()
    assert first == {"items": [{"id": first_id}], "snapshot_max_id": second_id, "next_after_id": first_id}
    third_id = deployment(setup)
    final = client.get("/deployments/page", params={"limit": 1, "after_id": first_id, "through_id": second_id}).json()
    assert final == {"items": [{"id": second_id}], "snapshot_max_id": second_id, "next_after_id": None}
    assert client.get("/deployments").json()[:2] == old_list
    assert client.get("/deployments/page").json()["items"] == [{"id": value} for value in (first_id, second_id, third_id)]
    assert "specification" not in str(first)


@pytest.mark.parametrize("query", ["limit=0", "limit=101", "after_id=-1", "through_id=-1",
                                  "after_id=1", "after_id=2&through_id=1", "limit=true"])
def test_discovery_rejects_invalid_parameters(client, query):
    assert client.get("/deployments/page?" + query).status_code == 422


@pytest.mark.parametrize("flag,recovered", [(None, True), ("true", True), ("false", False)])
def test_schedule_recovery_default_preserved_and_can_be_disabled(setup, flag, recovered):
    client, now, _ = setup
    dep = deployment(setup, 1)
    item = reconcile(client, dep)["created"][0]
    worker_id = worker(client)
    client.post("/scheduling/run")
    claim(client, worker_id, item)
    now[0] += timedelta(seconds=30)
    response = client.post("/scheduling/run", params={} if flag is None else {"recover_expired": flag})
    assert response.status_code == 200
    assert client.get(f"/jobs/{item['start_job_id']}").json()["state"] == ("queued" if recovered else "claimed")
    assert client.get(f"/instances/{item['instance_id']}").json()["state"] == ("failed" if recovered else "starting")
