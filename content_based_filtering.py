import numpy as np
import pandas as pd
from datetime import datetime

from konlpy.tag import Okt
from collections import Counter
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from database import fetch_data_music_and_groups, fetch_artist, fetch_theme, save_similar_music, close_mysql_connection
from custom_logger import info

okt = Okt()


def content_based_filtering():
    # 음악 및 음악 분석 결과 가져오기
    musics = fetch_data_music_and_groups()
    column_names = ['music_id', 'title', 'genre', 'lyrics', 'release_date', 'likes', 'view',
                    'highest_pitch', 'lowest_pitch', 'high_pitch_ratio', 'low_pitch_ratio',
                    'group_id', 'group_name', 'group_type']
    musics.columns = column_names

    # 가사 형태소 분석 후 2글자 이상 명사, 최고 많이 등장한 10개 단어만 남기기
    musics['lyrics'] = musics.apply(lambda row: remove_lyric_stop_words(row.lyrics), axis=1)

    # 가수 정보 가져오기
    artist_dict = fetch_artist_dict()

    # musics에 가수 이름 열 추가
    musics['artist_names'] = musics.apply(lambda row: '|'.join(at.artist_name for at in artist_dict[row.group_id]), axis=1)
    # musics에 노래 성별(남자, 여자, 혼성, 그외) 구분 추가
    musics['gender'] = musics.apply(lambda row: classify_gender([at.gender for at in artist_dict[row.group_id]]), axis=1)
    # musics에 국가 열 추가
    musics['nation'] = musics.apply(lambda row: classify_nation([at.nation for at in artist_dict[row.group_id]]), axis=1)

    # 테마 정보 가져오기
    theme_dict = fetch_theme_dict()

    # musics에 테마 열 추가
    musics['themes'] = musics.apply(lambda row: theme_dict.get(row.music_id, ''), axis=1)

    # 음악 데이터를 tfidf_vector로 처리하기 위해 문자열 전환
    musics_str = preprocessing(musics)

    # TfidfVectorizer를 이용 숫자 벡터로 전환
    tfidf_vector = TfidfVectorizer(token_pattern=r"\b\S+\b", tokenizer=None, preprocessor=custom_preprocessor)
    tfidf_matrix = tfidf_vector.fit_transform(musics_str).toarray()
    tfidf_matrix_feature = tfidf_vector.get_feature_names_out()

    tfidf_matrix = pd.DataFrame(tfidf_matrix, columns=tfidf_matrix_feature, index=musics.title)

    # 코사인 유사도 구하기
    cosine_sim = cosine_similarity(tfidf_matrix)
    cosine_sim_df = pd.DataFrame(cosine_sim, index=musics.music_id, columns=musics.music_id)

    # 실행 일시를 version으로 데이터베이스에 저장
    now = datetime.now()
    version = now.strftime("%Y-%m-%d %H:%M:%S")

    # 각 음악에 대해서 가장 유사한 음악 10개 추출 후 데이터베이스에 저장
    for music_id in cosine_sim_df.index:
        recommendations = get_recommendations(music_id, cosine_sim_df, musics)
        save_similar_music(music_id, recommendations, version)

    info("[SQL] " + "Similar music INSERT completed")
    close_mysql_connection()


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


def fetch_theme_dict():
    themes = fetch_theme()
    column_names = ['music_id', 'theme_name']
    themes.columns = column_names

    theme_dict = {}
    for tm in themes.itertuples():
        if tm.music_id in theme_dict:
            theme_dict[tm.music_id] = theme_dict[tm.music_id] + '|' + tm.theme_name
        else:
            theme_dict[tm.music_id] = tm.theme_name

    return theme_dict


def classify_gender(gender_list):
    # 'OTHER'이 하나라도 있으면 '기타'
    if 'OTHER' in gender_list:
        return '기타'

    # 'FEMALE'과 'MALE'이 섞여 있으면 '혼성'
    if 'FEMALE' in gender_list and 'MALE' in gender_list:
        return '혼성'

    # 'FEMALE'만 있으면 '여성'
    if all(g == 'FEMALE' for g in gender_list):
        return '여성'

    # 'MALE'만 있으면 '남성'
    if all(g == 'MALE' for g in gender_list):
        return '남성'

    # 예외 처리를 위해 기본 반환값 (조건에 맞지 않는 경우)
    return '기타'


def classify_nation(nation_list):
    # '대한민국' 또는 '한국'이 하나라도 있으면 '대한민국' 반환
    if '대한민국' in nation_list or '한국' in nation_list:
        return '대한민국'

    # 중복 제거한 국가 목록
    unique_nations = set(nation_list)

    # 하나의 국가만 있으면 그 국가를 반환
    if len(unique_nations) == 1:
        return unique_nations.pop()

    # 여러 국가가 섞여 있으면 '다국가' 반환
    return '다국가'


def remove_lyric_stop_words(lyrics):
    if lyrics:
        tags = okt.pos(lyrics, norm=True, stem=True)

        # 조사 제거
        tags = list(filter(lambda t: t[1] == 'Noun', tags))
        # 줄바꿈 문자 제거
        tags = list(filter(lambda t: not t[0].startswith('\n'), tags))

        # 단어만 추출하고, 2글자 이상인 단어 필터링
        words = [word for word, pos in tags if len(word) >= 2]

        # 단어 개수 세기
        word_counts = Counter(words)

        # 개수가 가장 많은 10개의 단어 추출
        most_common_words = word_counts.most_common(10)

        return ' '.join([tag[0] for tag in most_common_words])

    else:  # 가사 없는 경우
        return ''


def preprocessing(musics):
    # 음악 데이터를 tfidf_vector로 처리하기 위해 문자열 전환

    musics_str = 'genre:' + musics.genre
    musics_str = musics_str + ' group_id:' + musics.group_id.astype(str)
    musics_str = musics_str + ' group_type:' + musics.group_type

    musics_str = musics_str + ' ' + musics.artist_names.apply(
        lambda x: ' '.join(['artist:' + artist.replace(' ', '_') for artist in x.split('|')])
    )

    musics_str = musics_str + ' gender:' + musics.gender
    musics_str = musics_str + ' nation:' + musics.nation

    musics_str = musics_str + ' ' + musics.themes.apply(
        lambda x: ' '.join(['theme:' + theme.replace(' ', '_') if theme else '' for theme in x.split('|')])
    )

    musics_str = musics_str + ' ' + ' hPitch:' + musics.highest_pitch.apply(lambda x: str(round(x / 100) * 100))
    musics_str = musics_str + ' ' + ' lPitch:' + musics.lowest_pitch.apply(lambda x: str(round(x / 100) * 100))
    musics_str = musics_str + ' ' + ' hRatio:' + musics.high_pitch_ratio.apply(
        lambda x: str(round(round(x * 100) / 10) * 10))
    musics_str = musics_str + ' ' + ' lRatio:' + musics.low_pitch_ratio.apply(
        lambda x: str(round(round(x * 100) / 10) * 10))

    musics_str = musics_str + ' ' + musics.lyrics

    return musics_str


def custom_tokenizer(text):
    # TfidfVectorizer에서 사용하는 tokenizer : 중요 정보에 가중치를 줌
    tokens = text.split()
    weighted_tokens = []
    for token in tokens:
        if token.startswith("genre:"):
            weighted_tokens.append((token, 20))  # 장르 가중치 20
        elif token.startswith("group_id:"):
            weighted_tokens.append((token, 20))  # 그룹 가중치 20
        elif token.startswith("group_type:"):
            weighted_tokens.append((token, 10))  # 그룹 형태 가중치 10
        elif token.startswith("artist:"):
            weighted_tokens.append((token, 30))  # 가수 명 가중치 30
        elif token.startswith("gender:"):
            weighted_tokens.append((token, 60))  # 성별 가중치 60
        elif token.startswith("nation:"):
            weighted_tokens.append((token, 40))  # 국가 가중치 40
        elif token.startswith("theme:"):
            weighted_tokens.append((token, 10))  # 테마 가중치 10
        elif token.startswith("hPitch:"):
            weighted_tokens.append((token, 10))  # 최고음 가중치 10
        elif token.startswith("lPitch:"):
            weighted_tokens.append((token, 10))  # 최저음 가중치 10
        elif token.startswith("hRatio:"):
            weighted_tokens.append((token, 10))  # 고음 비율 가중치 10
        elif token.startswith("lRatio:"):
            weighted_tokens.append((token, 10))  # 저음 비율 가중치 10
        else:
            weighted_tokens.append((token, 1))  # 가사 가중치 1
    return weighted_tokens    


def custom_preprocessor(text):
    # TfidfVectorizer에서 사용하는 preprocessor : 가중치만큼 token을 복제해서 expanded_token을 만듦
    expanded_tokens = [" ".join([token] * weight) for token, weight in custom_tokenizer(text)]
    return " ".join(expanded_tokens)


def get_recommendations(target_id, matrix, items, k=10):
    # 각 음악에 대해서 유사한 곡 목록을 추출하는 함수
    recom_idx = matrix.loc[:, target_id].values.reshape(1, -1).argsort()[:, ::-1].flatten()[1:k + 1]
    recom_score = np.sort(matrix.loc[:, target_id].values.reshape(1, -1).flatten())[::-1][1:k + 1]
    recom_id = items.iloc[recom_idx, :].music_id.values
    target_title_list = np.full(len(range(k)), target_id)
    d = {
        'target_id': target_title_list,
        'recom_id': recom_id,
        'score': recom_score
    }
    return pd.DataFrame(d)
