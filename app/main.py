import os
import json
import asyncio
import warnings
from dotenv import load_dotenv

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
import aiomysql

from async_db.database import getMySqlPool, createTableIfNeccessary
from vector_db.database import getMongoDBPool

# Kafka Topic 생성
async def create_kafka_topics():
    adminClient = AIOKafkaAdminClient(
        bootstrap_servers='localhost:9092',
        loop=asyncio.get_running_loop()
    )

    try:
        await adminClient.start()

        topics = [
            NewTopic("test-topic", num_partitions=1, replication_factor=1),
            NewTopic("completion-topic", num_partitions=1, replication_factor=1),
        ]

        for topic in topics:
            try:
                await adminClient.create_topics([topic])
            except TopicAlreadyExistsError:
                print(f"Topic '{topic.name}' already exists, skipping creation")

    except Exception as e:
        print(f"카프카 토픽 생성 실패: {e}")
    finally:
        await adminClient.close()

# WebSocket으로 클라이언트에게 메시지 전달
async def testTopicConsume(app: FastAPI):
    consumer = app.state.kafka_test_topic_consumer

    while not app.state.stop_event.is_set():
        try:
            msg = await consumer.getone()
            print(f"Kafka message: {msg}")
            data = json.loads(msg.value.decode("utf-8"))
            print(f"Decoded data: {data}")

            # 여기서 비즈니스 로직 처리
            await asyncio.sleep(60)

            for connection in app.state.connections:
                await connection.send_json({
                    'message': 'Processing completed.',
                    'data': data,
                    'title': "Kafka Test"
                })

        except asyncio.CancelledError:
            print("소비자 태스크 종료")
            break
        except Exception as e:
            print(f"소비 중 에러 발생: {e}")

# FastAPI lifespan 설정 (앱 시작 및 종료 관리)
async def lifespan(app: FastAPI):
    # Startup
    app.state.dbPool = await getMySqlPool()
    await createTableIfNeccessary(app.state.dbPool)

    app.state.vectorDBPool = await getMongoDBPool()
    app.state.stop_event = asyncio.Event()

    app.state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        client_id='fastapi-kafka-producer'
    )
    app.state.kafka_consumer = AIOKafkaConsumer(
        'completion_topic',
        bootstrap_servers='localhost:9092',
        group_id="my_group",
        client_id='fastapi-kafka-consumer'
    )
    app.state.kafka_test_topic_consumer = AIOKafkaConsumer(
        'test-topic',
        bootstrap_servers='localhost:9092',
        group_id="another_group",
        client_id='fastapi-kafka-consumer'
    )

    await app.state.kafka_producer.start()
    await app.state.kafka_consumer.start()
    await app.state.kafka_test_topic_consumer.start()

    asyncio.create_task(testTopicConsume(app))

    try:
        yield
    finally:
        # Shutdown
        app.state.dbPool.close()
        await app.state.dbPool.wait_closed()

        app.state.vectorDBPool.close()
        await app.state.vectorDBPool.wait_closed()

        app.state.stop_event.set()

        await app.state.kafka_producer.stop()
        await app.state.kafka_consumer.stop()
        await app.state.kafka_test_topic_consumer.stop()

# FastAPI 애플리케이션 정의
warnings.filterwarnings("ignore", category=aiomysql.Warning)

load_dotenv()
origins = os.getenv("ALLOWED_ORIGINS", "").split(",")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.state.connections = set()

# Kafka 전송용 엔드포인트
class KafkaRequest(BaseModel):
    message: str

@app.post("/kafka-endpoint")
async def kafka_endpoint(request: KafkaRequest):
    eventData = request.dict()
    await app.state.kafka_producer.send_and_wait("test-topic", json.dumps(eventData).encode())
    return {"status": "processing"}

# WebSocket 엔드포인트
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    app.state.connections.add(websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        app.state.connections.remove(websocket)

if __name__ == "__main__":
    import uvicorn

    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", 8000))

    asyncio.run(create_kafka_topics())
    uvicorn.run(app, host=host, port=port)
