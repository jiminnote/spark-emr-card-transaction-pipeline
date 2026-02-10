"""S3 출력 확인 스크립트"""
import boto3

s3 = boto3.client('s3', endpoint_url='http://localhost:4566',
    aws_access_key_id='test', aws_secret_access_key='test', region_name='ap-northeast-2')

paginator = s3.get_paginator('list_objects_v2')
total_size = 0
dirs = set()
file_count = 0

for page in paginator.paginate(Bucket='card-pipeline-output'):
    for obj in page.get('Contents', []):
        parts = obj['Key'].split('/')
        if len(parts) > 1:
            dirs.add(parts[0])
        total_size += obj['Size']
        file_count += 1

print('=== S3 Output Bucket (card-pipeline-output) ===')
for d in sorted(dirs):
    dir_files = 0
    dir_size = 0
    for page in paginator.paginate(Bucket='card-pipeline-output', Prefix=d + '/'):
        for obj in page.get('Contents', []):
            dir_files += 1
            dir_size += obj['Size']
    mb = dir_size / 1024 / 1024
    print(f'  s3://card-pipeline-output/{d}/  ({dir_files} files, {mb:.1f} MB)')

mb_total = total_size / 1024 / 1024
print(f'\n  Total: {file_count} files, {mb_total:.1f} MB')
