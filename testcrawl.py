from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from googleapiclient.discovery import build
from airflow.utils.dates import days_ago
import os

# Fungsi untuk mengambil komentar YouTube
def fetch_youtube_comments():
    # Gantilah dengan kunci API YouTube Anda
    api_key = 'AIzaSyDd2g0RQD6QKadRkRmPUxOqrZcnbwhWYMU'

    # Buat koneksi ke API YouTube
    youtube = build('youtube', 'v3', developerKey=api_key)

    # ID video yang ingin Anda ambil komentarnya
    video_id = '47crIZmSNNc'

    # Maksimal jumlah komentar yang ingin diambil
    max_results = 50

    # Panggil API untuk mendapatkan komentar
    comments = []
    next_page_token = None

    while True:
        response = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            textFormat='plainText',
            maxResults=max_results,
            pageToken=next_page_token
        ).execute()

        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            comments.append(comment)

        if 'nextPageToken' in response:
            next_page_token = response['nextPageToken']
        else:
            break

    # Simpan komentar ke dalam file SQL
    output_sql_file = 'output_file.sql'  # Ganti dengan path file SQL yang sesuai
    with open(output_sql_file, 'w') as f:
        for comment_text in comments:
            f.write(f"INSERT INTO youtube_comments (comment) VALUES ('{comment_text}');\n")

# Buat DAG
dag = DAG(
    'youtube_comment_crawler',
    default_args={'owner': 'airflow'},
    schedule_interval=None,  # Atur jadwal eksekusi sesuai kebutuhan
    start_date=days_ago(1),  # Atur tanggal mulai sesuai kebutuhan
    catchup=False,
)

# Buat Python Operator untuk menjalankan kode
fetch_comments_task = PythonOperator(
    task_id='fetch_youtube_comments',
    python_callable=fetch_youtube_comments,
    dag=dag,
)

fetch_comments_task

if __name__ == "__main__":
    dag.cli()
