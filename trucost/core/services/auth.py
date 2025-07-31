from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

import jwt
from passlib.context import CryptContext

from trucost.core.services.base import BaseService


class JWTAuthService(BaseService):
    """JWT service"""

    _pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    def __init__(
        self,
        secret_key: str,
        algorithm: str = "HS256",
        access_token_expire_minutes: int = 30,
    ):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.token_expire_time = timedelta(minutes=access_token_expire_minutes)

    def get_password_hash(self, password: str) -> str:
        """Get a password hash"""
        return self._pwd_context.hash(password)

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password"""
        return self._pwd_context.verify(plain_password, hashed_password)

    def create_token(self, data: dict) -> str:
        """Create an access token"""
        to_encode = data.copy()

        expire = datetime.now(timezone.utc) + self.token_expire_time

        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)

        return encoded_jwt

    def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify a token"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            raise ValueError("Token has expired")
        except jwt.InvalidTokenError:
            raise ValueError("Invalid token")
