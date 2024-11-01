import pandas as pd
import numpy as np
import ast
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

from scipy.sparse.linalg import svds
from concurrent.futures import ThreadPoolExecutor

from database import (fetch_data_music_and_groups_extended, fetch_log_from_all_user, fetch_artist, fetch_bookmark,
                      fetch_user, save_recom_musics)
from pitch_converter import PitchConverter
from key_lock_dict import KeyLockDict
from custom_logger import info


# 로그 데이터로부터 평점을 계산하고 행렬분해 후 되돌려 예측 평점을 계산하는 함수
def update_predicted_ratings():
    # 음악 및 음악 분석 결과 가져오기
    music_data = fetch_data_music_and_groups_extended()
    column_names = ['music_id', 'title', 'genre', 'lyrics', 'release_date', 'likes', 'view',
                    'highest_pitch', 'lowest_pitch', 'high_pitch_ratio', 'low_pitch_ratio',
                    'pitch_average', 'pitch_stat', 'group_id', 'group_name', 'group_type']
    music_data.columns = column_names

    music_data['pitch_stat'] = music_data['pitch_stat'].apply(ast.literal_eval)

    info("[Process] music data fetched")

    # 가수 정보 가져오기
    artist_dict = fetch_artist_dict()

    # 노래 성별(남자, 여자, 혼성, 그외) 구분 추가
    music_data['gender'] = music_data.apply(
        lambda row: classify_gender([at.gender for at in artist_dict[row.group_id]]), axis=1
    )

    # 로그 정보 가져오기
    log_data = fetch_log_from_all_user()
    column_names = ['event_id', 'created_at', 'event', 'target_id', 'user_id']
    log_data.columns = column_names

    log_data.drop('created_at', axis=1, inplace=True)

    info("[Process] log data fetched")

    # 로그로 부터 병렬 처리로 rating을 계산할 때 안전성을 보장하기 위한 KeyLockDict 생성
    shared_dict = KeyLockDict()

    # ThreadPoolExecutor를 사용한 병렬 처리로 사용자별 음악에 대한 평점 계산
    with ThreadPoolExecutor() as executor:
        executor.map(lambda args: process_row(*args), [(row, shared_dict) for _, row in log_data.iterrows()])

    # 평점 정보를 DataFrame으로 변환
    rating_data = pd.DataFrame(list(shared_dict.data.items()), columns=['Key', 'rating'])

    rating_data[['music_id', 'user_id']] = pd.DataFrame(rating_data['Key'].tolist(), index=rating_data.index)
    rating_data = rating_data.drop('Key', axis=1)
    # 열 순서 정렬
    rating_data = rating_data[['music_id', 'user_id', 'rating']]

    info("[Process] rating data processed")

    # 음악과 평점 정보를 merge
    music_rating_data = pd.merge(rating_data, music_data, on='music_id')

    # user_id가 행, music_id가 열인 pivot table 만들기
    user_music_rating = music_rating_data.pivot_table('rating', index='user_id', columns='music_id').fillna(3)

    info("[Process] pivot table created")

    # 나중에 user_id를 이용해 index로 접근하기 위해 저장
    user_index_dict = dict()
    for idx, uid in enumerate(user_music_rating.index):
        user_index_dict[uid] = idx

    # 로그에 나타나지 않은 음악 추가
    all_music_id = music_data['music_id'].tolist()
    missing_titles = [music_id for music_id in all_music_id if music_id not in user_music_rating.columns.tolist()]

    user_music_rating = user_music_rating.reindex(
        index=list(user_music_rating.index),
        columns=list(user_music_rating.columns) + missing_titles,
        fill_value=3
    )

    # pivot_table 값을 numpy matrix로 변환해 matrix로 저장
    matrix = user_music_rating.values

    # user_ratings_mean은 사용자의 평균 평점
    user_ratings_mean = np.mean(matrix, axis=1)

    # R_user_mean : 사용자-음악에 대해 사용자 평균 평점을 뺀 것.
    matrix_user_mean = matrix - user_ratings_mean.reshape(-1, 1)

    # 행렬 분해
    U, sigma, Vt = svds(matrix_user_mean, k=6)
    sigma = np.diag(sigma)  #

    # U, Sigma, Vt의 내적을 수행하여, 다시 원본 행렬을 복구, 예측 평점을 계산
    # 사용자별 평점 평균을 다시 더해 줌
    svd_user_predicted_ratings = np.dot(np.dot(U, sigma), Vt) + user_ratings_mean.reshape(-1, 1)
    df_svd_preds = pd.DataFrame(svd_user_predicted_ratings, columns=user_music_rating.columns)

    return (df_svd_preds, music_data, user_index_dict, datetime.now())


# 전체 회원에 대해 추천 정보를 업데이트하는 함수
def collaborative_filtering():
    df_svd_preds, music_data, user_index_dict, update_time = update_predicted_ratings()

    # ThreadPoolExecutor를 사용한 병렬 처리로 모든 사용자의 추천 정보 업데이트
    with ThreadPoolExecutor() as executor:
        executor.map(
            lambda args: save_recommendations(*args),
            [(user_id, df_svd_preds, music_data, user_index_dict) for user_id in user_index_dict.keys()]
        )

    return (df_svd_preds, music_data, user_index_dict, update_time)


# 한 회원에 대한 추천 정보를 업데이트하는 함수
def save_recommendations(user_id, df_svd_preds, music_data, user_index_dict):

    # 전체 회원을 상대로 로그 기반으로 예측한 평점에서 user_id번 사용자 데이터 추출
    try:
        recommendations = df_svd_preds.iloc[user_index_dict[user_id]]
    except KeyError as e:
        # 새로운 회원이라 df_svd_preds에 존재하지 않으면 KeyError 던짐
        raise


    # 사용자가 북마크한 음악 조회
    bookmark_data = fetch_bookmark(user_id)
    if len(bookmark_data.columns) >= 2:
        bookmark_data.columns = ['music_id', 'title']

        # 사용자가 북마크한 곡은 추천에서 제외
        recommendations = recommendations.drop(bookmark_data.music_id.tolist())

    # DataFrame 변환
    recommendations_data = pd.DataFrame(recommendations)
    recommendations_data.columns = ['pred']
    # 추천 결과에 기존 음악 정보 추가
    recommendations_data = recommendations_data.merge(music_data, on='music_id')

    # 사용자 정보 조회
    user_data = fetch_user(user_id)

    # 사용자의 선호 장르와 나이대에 따른 가산점 계산
    recommendations_data = recommendations_data.apply(update_rating_by_user_data, axis=1, user_data=user_data)

    # 음역대에 따른 가산점 계산
    recommendations_data = recommendations_data.apply(update_rating_by_pitch_coverage, axis=1, user_data=user_data)

    # 예측 평점이 높은 순으로 정렬
    recommendations_data.sort_values(by='pred', ascending=False, inplace=True)
    recommendations_data.reset_index(drop=True, inplace=True)

    # 상위 100개만 선택
    recommendations_data = recommendations_data[:20]
    recommendations_data = recommendations_data[['music_id', 'pred']]

    # 추천 시점 저장
    now = datetime.now()
    version = now.strftime("%Y-%m-%d %H:%M:%S")

    save_recom_musics(user_id, recommendations_data, version)

    info("[Process] recommendations for user {0} completed".format(user_id))


def fetch_artist_dict():
    artists = fetch_artist()
    column_names = ['group_id', 'artist_id', 'artist_name', 'gender', 'nation']
    artists.columns = column_names

    # 가수 정보 group_id를 키로 dictionary 만들기
    artist_dict = {}
    for at in artists.itertuples():
        if at.group_id in artist_dict:
            artist_dict[at.group_id].append(at)
        else:
            artist_dict[at.group_id] = [at]

    return artist_dict


def classify_gender(gender_list):
    # 'OTHER'이 하나라도 있으면 '기타'
    if 'OTHER' in gender_list:
        return 'OTHER'

    # 'FEMALE'과 'MALE'이 섞여 있으면 '혼성'
    if 'FEMALE' in gender_list and 'MALE' in gender_list:
        return 'MIX'

    # 'FEMALE'만 있으면 '여성'
    if all(g == 'FEMALE' for g in gender_list):
        return 'FEMALE'

    # 'MALE'만 있으면 '남성'
    if all(g == 'MALE' for g in gender_list):
        return 'MALE'

    # 예외 처리를 위해 기본 반환값 (조건에 맞지 않는 경우)
    return 'OTHER'


# 데이터 처리 함수 (Thread-safe)
def process_row(row, shared_dict):
    key = (row['target_id'], row['user_id'])
    event = row['event']

    if event == 'viewMusicDetail':
        shared_dict.add(key, 0.1)
    elif event == 'closeMusicDetail':
        shared_dict.add(key, -0.5)
    elif event == 'bookmarkMusic':
        shared_dict.add(key, 1)
    elif event == 'unbookmarkMusic':
        shared_dict.add(key, -1)
    elif event == 'feedbackPositive':
        shared_dict.add(key, 1.5)
    elif event == 'feedbackNegative':
        shared_dict.add(key, -2)


# 사용자의 선호 장르와 나이대에 따른 가산점 계산
def update_rating_by_user_data(row, user_data):
    # 나이
    if user_data.get('age') is not None:
        age_prefix = user_data['age'] // 10
        release = row['release_date'].year
        if age_prefix == 1 and 2018 <= release:
            row['pred'] += 0.3
        elif age_prefix == 2 and 2010 <= release <= 2024:
            row['pred'] += 0.3
        elif age_prefix == 3 and 1995 <= release <= 2015:
            row['pred'] += 0.3
        elif age_prefix == 4 and 1985 <= release <= 2005:
            row['pred'] += 0.3
        elif age_prefix == 5 and 1975 <= release <= 1995:
            row['pred'] += 0.3
        elif age_prefix == 6 and 1965 <= release <= 1985:
            row['pred'] += 0.3

    # 성별
    if user_data.get('gender') is not None:
        if row['gender'] == user_data['gender']:
            row['pred'] += 0.4

    # 장르
    if user_data.get('genre') is not None:
        if row['genre'] in user_data['genre']:
            row['pred'] += 0.4

    return row


# 음역대에 따른 가산점 계산
def update_rating_by_pitch_coverage(row, user_data):
    if user_data.get('highest_pitch') is None or user_data.get('lowest_pitch') is None:
        return row

    coverage = 0.0

    highestPitch = PitchConverter.freq_to_pitch(user_data['highest_pitch'])
    lowestPitch = PitchConverter.freq_to_pitch(user_data['lowest_pitch'])

    lowest_pitch_met = False
    for pitch, pct in row['pitch_stat'].items():
        if lowestPitch == pitch:
            lowest_pitch_met = True
            coverage += pct
        elif highestPitch == pitch:
            coverage += pct
            break
        elif lowest_pitch_met:
            coverage += pct

    row['pred'] += coverage
    return row
