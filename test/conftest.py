import pytest
from faker import Faker
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.models import Base


@pytest.fixture(scope="session")
def engine():
    engine = create_engine("sqlite:///test.db", echo=False)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def session_factory(engine):
    return sessionmaker(bind=engine)


@pytest.fixture
def fake():
    return Faker()


@pytest.fixture(autouse=True)
def clean_database(session_factory):
    session = session_factory()
    for table in reversed(Base.metadata.sorted_tables):
        session.execute(table.delete())
    session.commit()
    session.close()
