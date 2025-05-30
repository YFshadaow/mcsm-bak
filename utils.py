import hashlib
import os
import re
from pathlib import Path

from config import exclusions


# 获取文件的最后修改时间（mtime）
def get_file_mtime(file_path) -> float:
    return os.path.getmtime(file_path)


# 获取文件的大小
def get_file_size(file_path) -> int:
    return os.path.getsize(file_path)


# 获取文件的 SHA-256 哈希值
def get_file_sha256(file_path) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as f:
        # 读取文件内容，按块计算哈希值
        while chunk := f.read(8192):  # 逐块读取文件，8192字节为块大小
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def normalize(relative_path: str) -> str:
    # 使用Path处理路径，并返回标准化的相对路径
    return str(Path(relative_path).relative_to(Path('.')))


def is_excluded(file_path: str) -> bool:
    # 排除缓存文件
    if re.match(r'.*^\.mcsm_bak\..+\.json$', file_path):
        return True
    for pattern in exclusions:
        if re.match(pattern, file_path):
            return True
    return False