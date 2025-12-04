from concurrent.futures import ThreadPoolExecutor

import allure
import pytest

from db.services.user_repository import UserRepository


@allure.feature("Concurrency")
@allure.story("Parallel user creation")
@pytest.mark.usefixtures("clean_database")
def test_parallel_user_creation(session_factory, fake):
    repo = UserRepository(session_factory)

    with allure.step("Creating users"):
        with ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(lambda _: repo.create_user(fake.email()), range(50)))
        assert all(results), "Some users failed to be created"

    with allure.step("Verify created users"):
        users = repo.get_all_users()
        allure.attach("\n".join(user.email for user in users), name="User emails",
                      attachment_type=allure.attachment_type.TEXT)


@allure.feature("Concurrency")
@allure.story("Parallel reads and writes")
@pytest.mark.usefixtures("clean_database")
def test_parallel_read_write(session_factory, fake):
    repo = UserRepository(session_factory)

    with allure.step("Parallel write and read operations"):
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = []
            for _ in range(5):
                futures.append(pool.submit(lambda: repo.create_user(fake.email())))
                futures.append(pool.submit(lambda: repo.get_user_count()))

            results = [f.result() for f in futures]

    with allure.step("Verify reads returned counts"):
        assert any(isinstance(r, int) for r in results), "No reads returned integer count"


@allure.feature("Concurrency")
@allure.story("Atomic operations")
@pytest.mark.usefixtures("clean_database")
def test_concurrent_updates(session_factory):
    repo = UserRepository(session_factory)

    with allure.step("Create initial user"):
        repo.create_user("test@example.com")

    emails = ["a@test.com", "b@test.com", "c@test.com"]

    with allure.step("Perform concurrent email updates"):
        def update_email(new_email):
            test_users = repo.get_all_users()
            if not test_users:
                return False
            repo.update_email(test_users[0].id, new_email)
            return True
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = (list(pool.map(update_email, emails)))
        assert all(results), f"Some users failed to be updated: {emails}"

    with allure.step("Verify final email"):
        users = repo.get_all_users()
        allure.attach(users[0].email, name="Final email", attachment_type=allure.attachment_type.TEXT)
        assert users[0].email in emails, "Final email not in expected emails"


@allure.feature("Concurrency")
@allure.story("Stress test")
@pytest.mark.parametrize(("workers", "items"), [(5, 50), (10, 200)])
@pytest.mark.usefixtures("clean_database")
def test_stress_concurrent_inserts(session_factory, fake, workers, items):
    repo = UserRepository(session_factory)

    with allure.step(f"Create {items} users in parallel with {workers} workers"):
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(lambda _: repo.create_user(fake.email()), range(items)))

    with allure.step("Verify number of users"):
        count = repo.get_user_count()
        assert count == items, "Not enough users created under stress"
        allure.attach(str(count), name="User count", attachment_type=allure.attachment_type.TEXT)
