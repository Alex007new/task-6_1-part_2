# lifecycle_test.py
from s3_client import S3Client
import os

ENDPOINT = "https://s3.ru-3.storage.selcloud.ru"
BUCKET = "data-engineer-practice-alex"

if __name__ == "__main__":
    s3c = S3Client(
        endpoint=ENDPOINT,
        access_key=os.getenv("S3_ACCESS_KEY"),
        secret_key=os.getenv("S3_SECRET_KEY"),
        bucket=BUCKET
    )

    print("🚀 НАСТРОЙКА LIFECYCLE POLICY")
    s3c.set_lifecycle_policy()  # Создать правило

    print("\n🔍 ПРОВЕРКА НАСТРОЕК")
    s3c.check_lifecycle()  # Показать результат
