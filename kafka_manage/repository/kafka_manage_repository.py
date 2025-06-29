from abc import ABC, abstractmethod


class KafkaManageRepository(ABC):
    @abstractmethod
    def create_topic(self, topic_name: str) -> dict:
        pass

    @abstractmethod
    async def send_message(self, topic: str, message: dict) -> None:
        pass
    