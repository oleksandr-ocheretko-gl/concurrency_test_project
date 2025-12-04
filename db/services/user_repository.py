from typing import List
from db.models import User
from db.services.db_helper import managed_session, managed_session_read

class UserRepository:

    def __init__(self, session_factory):
        self.session_factory = session_factory

    def create_user(self, email: str) -> bool:
        with managed_session(self.session_factory) as session:
            if session is False:
                return False
            user = User(email=email)
            session.add(user)
            return True

    def get_all_users(self) -> List[User]:
        with managed_session_read(self.session_factory) as session:
            return session.query(User).all()

    def get_user_count(self) -> int:
        with managed_session_read(self.session_factory) as session:
            return session.query(User).count()

    def update_email(self, user_id: int, new_email: str) -> bool:
        with managed_session(self.session_factory) as session:
            if session is False:
                return False
            user = session.query(User).filter_by(id=user_id).first()
            if not user:
                return False
            user.email = new_email
            return True
