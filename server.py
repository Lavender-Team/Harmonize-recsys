from kafka import KafkaConsumer

import json
from datetime import datetime, timedelta
from database import close_mysql_connection
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

info('[Start] consumer started')

try:
    while True:
        # 메시지를 폴링, 타임아웃을 설정하여 주기적으로 메시지 확인
        messages = consumer.poll(timeout_ms=3000)

        # 한 회원만 업데이트할 때 음악과 로그 정보 전체 조회를 피하기 위한 저장 변수
        df_svd_preds = None
        music_data = None
        user_index_dict = None
        update_time = None

        if messages:
            for topic_partition, msgs in messages.items():
                for message in msgs:
                    # print(f'Topic : {message.topic}, Partition : {message.partition}, Offset : {message.offset}, Key : {message.key}, value : {message.value}')

                    request = message.value

                    if request['command'] == 'content-based':
                        # 콘텐츠 기반 추천 결과 업데이트
                        content_based_filtering()

                    elif request['command'] == 'collaborative_all':
                        # 전체 회원에 대해 추천 정보를 업데이트
                        df_svd_preds, music_data, user_index_dict, update_time = collaborative_filtering()

                    elif request['command'] == 'collaborative_one':
                        # 갖고 있는 예측 평점 정보가 오래되었으면 업데이트
                        current_time = datetime.now()
                        if update_time == None or current_time - update_time >= timedelta(minutes=2):
                            df_svd_preds, music_data, user_index_dict, update_time = update_predicted_ratings()

                        # user_id 회원에 대해 추천 결과 업데이트
                        save_recommendations(int(request['user_id']), df_svd_preds, music_data, user_index_dict)

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
