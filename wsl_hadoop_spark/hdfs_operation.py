from hdfs import InsecureClient
import os

HDFS_URL = 'http://localhost:9870'
HDFS_USER = 'hadoop-hdfs-user'  # 사용자명

client = InsecureClient(HDFS_URL, user=HDFS_USER)

# 공유 디렉토리 경로
local_file_path = '/opt/shared_data/input.txt'
download_path = '/opt/shared_data/downloaded_input.txt'

# 파일 생성
os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
with open(local_file_path, 'w') as f:
    f.write("HDFS shared access test.\n")

print("[INFO] 로컬 파일 생성 완료")

# 업로드
client.upload(f'/user/{HDFS_USER}/input.txt', local_file_path, overwrite=True)
print("[INFO] HDFS 업로드 완료")

# 다운로드
client.download(f'/user/{HDFS_USER}/input.txt', download_path, overwrite=True)
print("[INFO] HDFS 다운로드 완료")
