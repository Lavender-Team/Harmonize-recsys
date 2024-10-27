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


def get_own_connection():
    return pymysql.connect(
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
               f"FROM (music m NATURAL JOIN music_analysis ma) NATURAL JOIN groups g "
               f"WHERE ma.`status` = 'COMPLETE'")
        info("[SQL] " + sql)
        cursor.execute(sql)
        result = cursor.fetchall()

        df = pd.DataFrame(result)

        return df

def fetch_data_music_and_groups_extended():
    with connection.cursor() as cursor:
        sql = (f"SELECT m.music_id, m.title, m.genre, m.lyrics, m.release_date, m.likes, m.`view`, "
               f"ma.highest_pitch, ma.lowest_pitch, ma.high_pitch_ratio, ma.low_pitch_ratio, "
               f"ma.pitch_average, ma.pitch_stat, g.group_id, g.group_name, g.group_type "
               f"FROM (music m NATURAL JOIN music_analysis ma) NATURAL JOIN groups g "
               f"WHERE ma.`status` = 'COMPLETE'")
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


def fetch_log_from_all_user():
    with connection.cursor() as cursor:
        connection.begin()

        query = "SELECT * FROM log WHERE created_at >= DATE_SUB(NOW(), INTERVAL 3 MONTH)"

        info("[SQL] " + query)
        cursor.execute(query)
        result = cursor.fetchall()

        df = pd.DataFrame(result)

        return df


def fetch_bookmark(own_connection, user_id):
    with own_connection.cursor() as cursor:
        own_connection.begin()

        query = f"SELECT music_id, title FROM bookmark NATURAL JOIN music WHERE user_id = {user_id}"

        info("[SQL] " + query)
        cursor.execute(query)
        result = cursor.fetchall()

        df = pd.DataFrame(result)

        own_connection.close()
        return df


def fetch_user(own_connection, user_id):
    with own_connection.cursor() as cursor:
        own_connection.begin()
        user_data = dict()

        query = f"SELECT age, gender FROM user WHERE user_id = {user_id}"
        cursor.execute(query)
        result = cursor.fetchone()

        if result is not None:
            user_data['age'] = result[0]
            user_data['gender'] = result[1]

        query = f"SELECT genre FROM user_genre WHERE user_user_id = {user_id}"
        cursor.execute(query)
        result = cursor.fetchall()

        if len(result) == 3:
            user_data['genre'] = [result[0][0], result[1][0], result[2][0]]

        query = f"SELECT highest_pitch, lowest_pitch FROM user_analysis WHERE user_id = 1 ORDER BY analysis_date DESC LIMIT 1"
        cursor.execute(query)
        result = cursor.fetchone()

        if result is not None:
            user_data['highest_pitch'] = result[0]
            user_data['lowest_pitch'] = result[1]

        own_connection.close()
        return user_data


def save_recom_musics(own_connection, user_id, recommendations_data, version):
    try:
        with own_connection.cursor() as cursor:
            own_connection.begin()

            delete_query = f"DELETE FROM recom_music WHERE target_user = {user_id}"
            cursor.execute(delete_query)

            insert_query = ("INSERT INTO recom_music(recom_music, score, `version`, `rank`, target_user) "
                            "VALUES (%s, %s, %s, %s, %s)")

            recommendations_data['version'] = version
            recommendations_data['rank'] = recommendations_data.index + 1
            recommendations_data['target_user'] = user_id

            cursor.executemany(insert_query, recommendations_data.itertuples(index=False, name=None))

            own_connection.commit()

    except Exception as e:
        print(e)
        own_connection.rollback()

    own_connection.close()

