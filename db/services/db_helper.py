from contextlib import contextmanager
from sqlalchemy.exc import IntegrityError

@contextmanager
def managed_session(session_factory):
    session = session_factory()
    try:
        yield session
        session.commit()
    except IntegrityError:
        session.rollback()
        yield False
    finally:
        session.close()


@contextmanager
def managed_session_read(session_factory):
    session = session_factory()
    try:
        yield session
    finally:
        session.close()