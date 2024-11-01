import threading
from concurrent.futures import ThreadPoolExecutor

from kafka import KafkaConsumer
from kafka import KafkaProducer

import json
from datetime import datetime, timedelta
from collaborative_filtering import collaborative_filtering, update_predicted_ratings, save_recommendations
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

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

info('[Start] consumer and producer started')

# 한 회원만 업데이트할 때 음악과 로그 정보 전체 조회를 피하기 위한 저장 변수
df_svd_preds = None
music_data = None
user_index_dict = None
update_time = None
lock = threading.Lock()

def update_global_vars(preds, music, user, uptime):
    global df_svd_preds, music_data, user_index_dict, update_time

    with lock:
        df_svd_preds = preds
        music_data = music
        user_index_dict = user
        update_time = uptime


def process_request(message):
    global df_svd_preds, music_data, user_index_dict, update_time

    request = message.value

    if request['command'] == 'content-based':
        # 콘텐츠 기반 추천 결과 업데이트
        content_based_filtering()

    elif request['command'] == 'collaborative_all':
        # 응답을 위한 correlationId 확인
        correlation_id = None
        for header in message.headers:
            if header[0] == 'kafka_correlationId':
                correlation_id = header[1]
                break

        if correlation_id is None:
            info(" [Kafka] No correlation id found")
            return

        # 전체 회원에 대해 추천 정보를 업데이트
        preds, music, user, uptime = collaborative_filtering()
        update_global_vars(preds, music, user, uptime)

        # 완료 응답
        headers = [('kafka_correlationId', correlation_id)]

        producer.send('musicRecSysReply', value=b'all', headers=headers)
        info("[Kafka] collaborative_all request replied")

    elif request['command'] == 'collaborative_one':
        # 응답을 위한 correlationId 확인
        correlation_id = None
        for header in message.headers:
            if header[0] == 'kafka_correlationId':
                correlation_id = header[1]
                break

        if correlation_id is None:
            info(" [Kafka] No correlation id found")
            return

        # 갖고 있는 예측 평점 정보가 오래되었으면 업데이트
        current_time = datetime.now()
        if update_time == None or current_time - update_time >= timedelta(minutes=2):
            preds, music, user, uptime = update_predicted_ratings()
            update_global_vars(preds, music, user, uptime)

        # user_id 회원에 대해 추천 결과 업데이트
        try:
            save_recommendations(int(request['user_id']), df_svd_preds, music_data, user_index_dict)
        except KeyError:
            # 새로운 회원이라 df_svd_preds에 존재하지 않으면
            preds, music, user, uptime = collaborative_filtering()
            update_global_vars(preds, music, user, uptime)

        # 완료 응답
        headers = [('kafka_correlationId', correlation_id)]

        producer.send('musicRecSysReply', value=bytes(request['user_id']), headers=headers)
        info("[Kafka] collaborative_one request replied")


try:
    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            # 메시지를 폴링, 타임아웃을 설정하여 주기적으로 메시지 확인
            messages = consumer.poll(timeout_ms=3000)

            if messages:
                for topic_partition, msgs in messages.items():
                    for message in msgs:
                        # print(f'Topic : {message.topic}, Partition : {message.partition}, Offset : {message.offset}, Key : {message.key}, value : {message.value}')

                        # 쓰레드풀에 제출하여 병렬로 요청 처리
                        executor.submit(process_request, message)

            # else:
            #    print("메시지 없음, 계속 대기 중...")

except PermissionError:
    info('[PermissionError] File I/O error occurred')
except KeyboardInterrupt:
    info('[Interrupt] consumer stopping')
finally:
    consumer.close()
    producer.close()
    info('[End] consumer and producer stopped')
