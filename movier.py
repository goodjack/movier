import os
from datetime import datetime
from os.path import dirname, join
from time import sleep

import pymysql.cursors
import requests
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path, override=True)

MOVIER_API = os.environ.get("MOVIER_API")


def get_mysql_connection():
    MYSQL_HOST = os.environ.get("MYSQL_HOST")
    MYSQL_USER = os.environ.get("MYSQL_USER")
    MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")
    MYSQL_DB = os.environ.get("MYSQL_DB")
    MYSQL_CHARSET = os.environ.get("MYSQL_CHARSET")

    connection = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        charset=MYSQL_CHARSET,
        cursorclass=pymysql.cursors.DictCursor)

    return connection

def insert_data(movie_data):
    connection = get_mysql_connection()
    MYSQL_TABLE = os.environ.get("MYSQL_TABLE")

    if (movie_data.get('MOID')):
        mo_date_create = movie_data['MO_DATE_CREATE'] if movie_data['MO_DATE_CREATE'] != "0000-00-00 00:00:00" else None
        mo_date_update = movie_data['MO_DATE_UPDATE'] if movie_data['MO_DATE_UPDATE'] != "0000-00-00 00:00:00" else None

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = ("INSERT INTO `" + MYSQL_TABLE + "` (`id`, `name`, `imdb`, `roto`, `ptt`, `ptt_base`, `ptt_title`, `ptt_desc`, `mo_date_create`, `mo_date_update`, `created_at`, `updated_at`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

                cursor.execute(
                    sql,
                    (movie_data['MOID'], movie_data['MONAMECH'],
                     movie_data['IMDB'], movie_data['ROTO'], movie_data['PTT'],
                     movie_data['PTT_BASE'], movie_data['title'],
                     movie_data['desc'], mo_date_create, mo_date_update,
                     datetime.now(), datetime.now()))
            connection.commit()
        finally:
            connection.close()


def main():
    MOVIE_ID_MAX = int(os.environ.get("MOVIE_ID_MAX"))

    for moid in range(MOVIE_ID_MAX, 0, -1):
        sleep(0.05)
        payload = {'moid': moid}

        try:
            movie = requests.get(MOVIER_API, params=payload)
        except:
            print("Unexpected error: ", moid)
        else:
            insert_data(movie.json()['moviedata'])


if __name__ == "__main__":
    main()
