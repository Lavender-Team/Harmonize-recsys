from kafka import KafkaConsumer

import json
from database import close_mysql_connection
from content_based_filtering import content_based_filtering
from custom_logger import info

consumer = KafkaConsumer(
    'musicRecSys', # 토픽명
    bootstrap_servers=['localhost:9092'],                           # 카프카 브로커 주소 리스트
    auto_offset_reset='latest',                                     # 오프셋 위치(earliest: 가장 처음, latest: 가장 최근)
    enable_auto_commit=True,                                        # 오프셋 자동 커밋 여부
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),     # 메시지 값의 역직렬화
    consumer_timeout_ms=1000                                        # 데이터를 기다리는 최대 시간
)

info('[Start] consumer started')

try:
    while True:
        # 메시지를 폴링, 타임아웃을 설정하여 주기적으로 메시지 확인
        messages = consumer.poll(timeout_ms=3000)

        if messages:
            for topic_partition, msgs in messages.items():
                for message in msgs:
                    # print(f'Topic : {message.topic}, Partition : {message.partition}, Offset : {message.offset}, Key : {message.key}, value : {message.value}')

                    request = message.value

                    if request['command'] == 'content-based':
                        # 콘텐츠 기반 추천 결과 업데이트
                        content_based_filtering()

        # else:
        #    print("메시지 없음, 계속 대기 중...")

except PermissionError:
    info('[PermissionError] File I/O error occurred')
except KeyboardInterrupt:
    info('[Interrupt] consumer stopping')
finally:
    consumer.close()
    close_mysql_connection()
    info('[End] consumer stopped')
