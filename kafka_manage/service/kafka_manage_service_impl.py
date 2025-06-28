from kafka_manage.repository.kafka_manage_repository_impl import KafkaManageRepositoryImpl
from kafka_manage.service.kafka_manage_service import KafkaManageService


class KafkaManageServiceImpl(KafkaManageService):
    def __init__(self):
        self.repository = KafkaManageRepositoryImpl()

    def create_topic(self, topic_name: str) -> dict:
        return self.repository.create_topic(topic_name)