import os
import json
import threading
import time
from typing import Dict, Any
from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

app = FastAPI(title="CinemaAbyss Events Service")

# Kafka конфигурация
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
PORT = int(os.getenv("PORT", 8082))

# Инициализация Kafka Producer
producer = None

def get_producer():
    """Получить или создать Kafka producer"""
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"Kafka Producer подключен к {KAFKA_BROKERS}")
        except Exception as e:
            print(f"Не удалось подключиться к Kafka: {e}")
            producer = None
    return producer


# Модели данных
class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: int


class UserEvent(BaseModel):
    user_id: int
    username: str
    action: str
    timestamp: str


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: str
    method_type: str


def consume_events(topic: str, group_id: str):
    """Consumer который читает события из Kafka и логирует их"""
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            print(f"Consumer для топика '{topic}' запущен")
            
            for message in consumer:
                event_data = message.value
                print(f"[{topic}] Получено событие: {json.dumps(event_data, indent=2)}")
                
        except Exception as e:
            print(f"Ошибка в consumer '{topic}': {e}")
            time.sleep(5)  # Ждем перед повторной попыткой


# Запуск consumers в фоновых потоках
def start_consumers():
    """Запустить все consumers в отдельных потоках"""
    topics = [
        ("movie-events", "events-service-movies"),
        ("user-events", "events-service-users"),
        ("payment-events", "events-service-payments")
    ]
    
    for topic, group_id in topics:
        thread = threading.Thread(
            target=consume_events,
            args=(topic, group_id),
            daemon=True
        )
        thread.start()
        print(f"🚀 Запущен consumer для топика '{topic}'")


@app.on_event("startup")
async def startup_event():
    """Инициализация при старте приложения"""
    # Подключение producer
    get_producer()
    
    # Запуск consumers
    start_consumers()
    print("Events Service запущен")


@app.get("/health")
@app.get("/api/events/health")
async def health_check():
    """Хэлс чек events-сервиса"""
    return {"status": True}


async def publish_event(topic: str, event_data: Dict[str, Any]) -> Dict[str, str]:
    """Публикация события в Kafka"""
    prod = get_producer()
    
    if prod is None:
        print(f"Producer не доступен, событие не отправлено: {event_data}")
        return {"status": "success", "note": "Kafka недоступен, событие не отправлено"}
    
    try:
        future = prod.send(topic, value=event_data)
        # Ждем подтверждения отправки
        record_metadata = future.get(timeout=10)
        print(f"Событие отправлено в топик '{topic}': partition={record_metadata.partition}, offset={record_metadata.offset}")
        return {"status": "success"}
    except KafkaError as e:
        print(f"Ошибка отправки в Kafka: {e}")
        return {"status": "success", "note": f"Ошибка Kafka: {str(e)}"}


@app.post("/api/events/movie", status_code=201)
async def create_movie_event(event: MovieEvent):
    """Создать событие о фильме"""
    event_data = event.dict()
    print(f"Получено событие о фильме: {event_data}")
    result = await publish_event("movie-events", event_data)
    return result


@app.post("/api/events/user", status_code=201)
async def create_user_event(event: UserEvent):
    """Создать событие о пользователе"""
    event_data = event.dict()
    print(f"Получено событие о пользователе: {event_data}")
    result = await publish_event("user-events", event_data)
    return result


@app.post("/api/events/payment", status_code=201)
async def create_payment_event(event: PaymentEvent):
    """Создать событие о платеже"""
    event_data = event.dict()
    print(f"Получено событие о платеже: {event_data}")
    result = await publish_event("payment-events", event_data)
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
