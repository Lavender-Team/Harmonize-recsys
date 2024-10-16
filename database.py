import pandas as pd
import pymysql
from custom_logger import info


def get_mysql_password(file_path):
    with open(file_path, 'r') as file:
        password = file.read().strip()
    return password


connection = pymysql.connect(
    host='127.0.0.1',  # 데이터베이스 호스트 이름 또는 IP 주소
    user='root',  # 데이터베이스 사용자 이름
    password=get_mysql_password('mysql_password.txt'),  # 데이터베이스 사용자 비밀번호
    database='harmonize'  # 연결할 데이터베이스 이름
)


def close_mysql_connection():
    global connection
    if connection:
        connection.close()


def fetch_data_music_and_groups():
    with connection.cursor() as cursor:
        sql = (f"SELECT m.music_id, m.title, m.genre, m.lyrics, m.release_date, m.likes, m.`view`, "
               f"ma.highest_pitch, ma.lowest_pitch, ma.high_pitch_ratio, ma.low_pitch_ratio, "
               f"g.group_id, g.group_name, g.group_type "
               f"FROM (music m NATURAL JOIN music_analysis ma) NATURAL JOIN groups g")
        info("[SQL] " + sql)
        cursor.execute(sql)
        result = cursor.fetchall()

        df = pd.DataFrame(result)

        return df


def fetch_artist():
    with connection.cursor() as cursor:
        sql = (f"SELECT gai.group_id, a.artist_id, a.artist_name, a.gender, a.nation "
               f"FROM (SELECT g.group_id, gm.artist_id FROM groups g NATURAL JOIN group_member gm) gai "
               f"   INNER JOIN artist a ON gai.artist_id = a.artist_id")
        info("[SQL] " + sql)
        cursor.execute(sql)
        result = cursor.fetchall()

        df = pd.DataFrame(result)

        return df


def fetch_theme():
    with connection.cursor() as cursor:
        sql = (f"SELECT m.music_id, t.theme_name "
               f"FROM music m INNER JOIN theme t ON m.music_id = t.music_id")
        info("[SQL] " + sql)
        cursor.execute(sql)
        result = cursor.fetchall()

        df = pd.DataFrame(result)

        return df


def save_similar_music(music_id, similar_musics, version):
    try:
        with connection.cursor() as cursor:
            connection.begin()

            delete_query = "DELETE FROM similar_music WHERE target_id = %s"
            cursor.execute(delete_query, (music_id,))

            insert_query = ("INSERT INTO similar_music(target_id, recom_id, score, `version`, `rank`) "
                            "VALUES (%s, %s, '%s', %s, %s)")

            similar_musics['version'] = version
            similar_musics['rank'] = similar_musics.index + 1

            cursor.executemany(insert_query, similar_musics.itertuples(index=False, name=None))

            connection.commit()

    except Exception as e:
        connection.rollback()
