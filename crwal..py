import os
import boto3
import tarfile
import re
from os import sched_getaffinity
from datetime import datetime
from multiprocessing import Pool
import uuid
import gzip
# 配置AWS访问信息
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
MAX_FILE_SIZE = 700 * 1024  # 700 KB
AWS_REGION = 'us-east-1'  # arXiv S3 bucket所在区域

# S3桶和目录
S3_BUCKET = 'arxiv'  # arXiv的S3公共桶名称
#S3_PREFIX = 'src/arxiv'  # 源文件路径
S3_PREFIX = 'src/'

# 本地保存图片文件的目录
IMAGE_SAVE_DIR = './arxiv_images'

# 定义支持的图片格式
SUPPORTED_IMAGE_FORMATS = ['.png', '.jpg', '.jpeg', '.gif']



s3_resource = boto3.resource(
            "s3",  # the AWS resource we want to use
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name="us-east-1",  # same region arxiv bucket is in
        )
def extract_images_from_gz(gz_path, save_dir):
    """
    从 .gz 文件中解压并提取图片文件
    """
    try:
        # 解压 .gz 文件
        with gzip.open(gz_path, 'rb') as gz_file:
            with tarfile.open(fileobj=gz_file, mode='r:*') as tar:
                extract_images_from_archive(tar, save_dir)
    except Exception as e:
        print(f"Error extracting {gz_path}: {e}")

def extract_from_tar(tar_path, save_dir):
    """
    从 arXiv.tar 中提取所有子目录中的 .gz 文件并处理
    """
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # 解压 tar 文件
    with tarfile.open(tar_path, 'r:*') as archive:
        for member in archive.getmembers():
            # 如果是 .gz 文件，解压并处理
            if member.isfile() and member.name.lower().endswith('.gz'):
                # 先将 .gz 文件解压到本地
                gz_file_path = os.path.join(save_dir, os.path.basename(member.name))
                with open(gz_file_path, 'wb') as out_file:
                    out_file.write(archive.extractfile(member).read())
                extract_images_from_gz(gz_file_path, save_dir)
                os.remove(gz_file_path)  # 删除临时解压的 .gz 文件

def download_and_extract_images(s3_key, save_dir):
    """
    下载 arXiv 源文件，解压到临时目录，提取符合条件的图片文件，不保留目录结构。
    最后删除压缩包和临时目录。
    """
    try:
        # 构造本地文件路径（不保留嵌套目录结构）
        local_path = os.path.join(save_dir, os.path.basename(s3_key))

        # 确保保存目录存在
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        # 下载 S3 对象到本地
        s3_resource.meta.client.download_file(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Filename=local_path,
            ExtraArgs={'RequestPayer': 'requester'}
        )
        print(f"Downloaded: {s3_key}")

        # 确保下载的是 .tar 文件并调用 extract_from_tar 进行解压
        if local_path.endswith('.tar') or local_path.endswith('.tar.gz'):
            extract_from_tar(local_path, save_dir)
        else:
            print(f"Unsupported file format: {local_path}")
            return

        # 删除本地的 tar 文件
        if os.path.exists(local_path):
            os.remove(local_path)
            print(f"Deleted archive: {local_path}")

    except Exception as e:
        print(f"Error downloading and extracting {s3_key}: {e}")

def generate_unique_filename(save_dir, original_name):
    """
    生成唯一的文件名
    :param save_dir: 保存目录
    :param original_name: 原始文件名
    :return: 唯一的文件路径
    """
    unique_id = uuid.uuid4()
    base_name, ext = os.path.splitext(original_name)
    unique_filename = f"{base_name}_{unique_id}{ext}"
    return os.path.join(save_dir, unique_filename)

def extract_images_from_archive(archive, save_dir):
    """
    从 tar 文件对象中提取图片文件
    """
    for member in archive.getmembers():
        # 递归处理嵌套的 tar 文件（如果有）
        if member.isfile() and any(member.name.lower().endswith(ext) for ext in SUPPORTED_IMAGE_FORMATS):
            # 保存图片到目标目录（去掉目录结构），并生成唯一的文件名
            file_size = member.size
            if file_size <= MAX_FILE_SIZE:
                save_path = generate_unique_filename(save_dir, os.path.basename(member.name))
                
                with archive.extractfile(member) as file:
                    with open(save_path, 'wb') as out_file:
                        out_file.write(file.read())
                print(f"Extracted image: {save_path}")

        elif member.isfile() and member.name.lower().endswith('.gz'):
            # 如果文件是 .gz 压缩包，解压并递归提取
            gz_file_path = generate_unique_filename(save_dir, os.path.basename(member.name))
            with open(gz_file_path, 'wb') as out_file:
                out_file.write(archive.extractfile(member).read())
            with tarfile.open(gz_file_path, 'r:gz') as nested_tar:
                extract_images_from_archive(nested_tar, save_dir)
            os.remove(gz_file_path)  # 删除临时解压的 .gz 文件

def process_arxiv_files(cutoff_datetime):
    """
    处理指定年份的arXiv文件，下载并提取图片
    """
    # 创建本地保存目录
    if not os.path.exists(IMAGE_SAVE_DIR):
        os.makedirs(IMAGE_SAVE_DIR)

    # 根据前缀列出S3中的文件
    paginator = s3_resource.meta.client.get_paginator("list_objects_v2")
    iterator = paginator.paginate(
        Bucket=S3_BUCKET, 
        RequestPayer="requester", 
        Prefix='src'
    )

    for result in iterator:
        if 'Contents' in result:
            for item in result['Contents']:
                # exit()
                s3_key = item['Key']
                try: 
                    file_datetime = datetime.strptime(s3_key.strip('.tar'), "src/arXiv_src_%y%m_%f")
                except:
                    continue
                # 只处理在 cutoff_date 之后的文件
                if datetime(2023,9,19)<=file_datetime <= cutoff_datetime:
                    # 匹配压缩文件
                    if re.search(r'(\.tar\.gz|\.tar|\.zip)$', s3_key):
                        print(f"Processing file: {s3_key}")
                        download_and_extract_images(s3_key, IMAGE_SAVE_DIR)
        else:
            print(f"No files found for year {cutoff_datetime}")


if __name__ == '__main__':
    # 处理近一年的文件 (假设从2023年到2024年)
    print(AWS_ACCESS_KEY)
    print(AWS_SECRET_KEY)
    cutoff_datetime = datetime(2024,1 , 1)
    
    process_arxiv_files(cutoff_datetime)
