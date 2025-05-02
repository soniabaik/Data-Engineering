import asyncio
import json
import os

import aiomysql
import nltk
# from aiokafka.admin import AIOKafkaAdminClient, NewTopic
# from aiokafka.errors import TopicAlreadyExistsError
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from pydantic import BaseModel


async def create_kafka_topics():
    adminClient = AIOKafkaAdminClient(
        bootstrap_servers='localhost:9092',
        loop=asyncio.get_running_loop()
    )

    try:
        await adminClient.start()

        topics = [
            NewTopic(
                "test-topic",
                num_partitions=1,
                replication_factor=1,
            ),
            NewTopic(
                "completion-topic",
                num_partitions=1,
                replication_factor=1,
            ),
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

# 현재는 deprecated 라고 나타나지만 lifespan 이란 것을 대신 사용하라고 나타나고 있음
# 완전히 배제되지는 않았는데 애플리케이션이 시작할 때 실행될 함수를 지정함
# 고로 애플리케이션 시작 시 비동기 처리가 가능한 DB를 구성한다 보면 됨
@app.on_event("startup")
async def startup_event():
    app.state.db_pool = await getMySqlPool()
    await createTableIfNeccessary(app.state.db_pool)


# 위의 것이 킬 때 였으니 이건 반대라 보면 됨
@app.on_event("shutdown")
async def shutdown_event():
    app.state.db_pool.close()
    await app.state.db_pool.wait_closed()

import warnings

warnings.filterwarnings("ignore", category=aiomysql.Warning)

async def lifespan(app: FastAPI):
    # Startup
    app.state.dbPool = await getMySqlPool()
    await createTableIfNeccessary(app.state.dbPool)

    app.state.vectorDBPool = await getMongoDBPool()

    # 비동기 I/O 정지 이벤트 감지
    app.state.stop_event = asyncio.Event()

    # Kafka Producer (생산자) 구성
    app.state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        client_id='fastapi-kafka-producer'
    )

    # Kafka Consumer (소비자) 구성
    app.state.kafka_consumer = AIOKafkaConsumer(
        'completion_topic',
        bootstrap_servers='localhost:9092',
        group_id="my_group",
        client_id='fastapi-kafka-consumer'
    )

    # 자동 생성했던 test-topic 관련 소비자
    app.state.kafka_test_topic_consumer = AIOKafkaConsumer(
        'test-topic',
        bootstrap_servers='localhost:9092',
        group_id="another_group",
        client_id='fastapi-kafka-consumer'
    )

    await app.state.kafka_producer.start()
    await app.state.kafka_consumer.start()
    await app.state.kafka_test_topic_consumer.start()

    # asyncio.create_task(consume(app))
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


app = FastAPI(lifespan=lifespan)

async def testTopicConsume(app: FastAPI):
    consumer = app.state.kafka_test_topic_consumer

    while not app.state.stop_event.is_set():
        try:
            msg = await consumer.getone()
            print(f"msg: {msg}")
            data = json.loads(msg.value.decode("utf-8"))
            print(f"request data: {data}")
            
            # 실제로 여기서 뭔가 요청을 하던 뭘 하던 지지고 볶으면 됨
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

load_dotenv()

origins = os.getenv("ALLOWED_ORIGINS", "").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.state.connections = set()

class KafkaRequest(BaseModel):
    message: str

@app.post("/kafka-endpoint")
async def kafka_endpoint(request: KafkaRequest):
    eventData = request.dict()
    await app.state.kafka_producer.send_and_wait("test-topic", json.dumps(eventData).encode())

    return {"status": "processing"}

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
    asyncio.run(create_kafka_topics())
    uvicorn.run(app, host="0.0.0.0", port=33333)
