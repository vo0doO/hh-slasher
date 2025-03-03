import datetime
import json

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential


class TokenMiddleware:
    def __init__(self, token):
        self.token = token

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.spider.settings
        token = cls._read_json(settings.get("HH_API_TOKEN_FILE_PATH"))

        if not cls._is_can_update(token["timestamp"], 5):
            return cls(token=token)

        token = cls._regenerate_token(settings, token)
        cls._write_json(settings.get("HH_API_TOKEN_FILE_PATH"), token)
        return cls(token=token)

    def process_request(self, spider, request):
        request.headers["Authorization"] = f"Bearer {self.token['access_token']}"

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    @staticmethod
    def _regenerate_token(settings, token: dict) -> dict:
        url = "https://api.hh.ru/token"
        headers = {
            "HH-User-Agent": settings.get("HH_API_USER_AGENT"),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        payload = {
            "client_id": settings.get("HH_API_CLIENT_ID"),
            "client_secret": settings.get("HH_API_CLIENT_SECRET"),
            "grant_type": "client_credentials",
        }
        response_ = httpx.post(url=url, data=payload, headers=headers)
        data = response_.json()

        response_.raise_for_status()

        if 'access_token' in data:
            token["access_token"] = data["access_token"]
            token["timestamp"] = datetime.datetime.now(
                datetime.timezone.utc
            ).timestamp()

        return token

    @staticmethod
    def _is_can_update(token_create_timestamp: float, timedelta: int) -> bool:
        utc = datetime.timezone.utc
        now = datetime.datetime.now(utc)
        create = datetime.datetime.fromtimestamp(token_create_timestamp, utc)
        update = create + datetime.timedelta(minutes=timedelta)

        if now >= update:
            return True
        else:
            return False

    @staticmethod
    def _read_json(path: str) -> dict:
        with open(path, "r") as f:
            return json.load(f)

    @staticmethod
    def _write_json(path: str, data: dict) -> None:
        with open(path, "w") as f:
            return json.dump(data, f)
