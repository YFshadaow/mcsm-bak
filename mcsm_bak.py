import json
import logging
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterator

from config import target_path, exclusions, instances, logging_level
from mcsm_api import get_cwd, get_status, Status, disable_auto_save, enable_auto_save
from utils import get_file_mtime, get_file_size, get_file_sha256


def normalize(relative_path: str) -> str:
    # 使用Path处理路径，并返回标准化的相对路径
    return str(Path(relative_path).relative_to(Path('.')))


def backup_file(file_path: str, label: str, instance: str) -> bool:
    normalized_path = normalize(file_path)
    final_target_path = Path(target_path) / label / instance / Path(normalized_path).parent
    result = subprocess.run(
        args=[
            '/root/BaiduPCS-Go/BaiduPCS-Go',
            'upload',
            normalized_path,
            final_target_path
        ],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        return True
    return False


def walk_files(base : str, current: str = None) -> Iterator[str]:
    base_path = Path(base)
    current_path = base_path if current is None else Path(current)

    for entry in sorted(current_path.iterdir()):
        # 如果是文件，返回相对于 base 的路径
        if entry.is_file():
            yield str(entry.relative_to(base_path))  # 返回字符串形式的相对路径
        elif entry.is_dir():
            # 如果是目录，递归调用并返回子目录中的文件
            yield from walk_files(base, str(entry))



def is_excluded(file_path: str) -> bool:
    # 排除缓存文件
    if re.match(r'.+^\.mcsm_bak\..+\.json$', file_path):
        return True
    for pattern in exclusions:
        if re.match(pattern, file_path):
            return True
    return False


def should_backup(file_path: str, cache: dict) -> bool:
    normalized_path = normalize(file_path)
    if normalized_path not in cache:
        return True
    mtime = get_file_mtime(normalized_path)
    size = get_file_size(normalized_path)
    if cache[normalized_path]['mtime'] == mtime and cache[normalized_path]['size'] == size:
        return False
    sha256 = get_file_sha256(normalized_path)
    if cache[normalized_path]['sha256'] == sha256:
        return False
    return True


def update_cache(file_path: str, cache: dict):
    normalized_path = normalize(file_path)
    mtime = get_file_mtime(normalized_path)
    size = get_file_size(normalized_path)
    sha256 = get_file_sha256(normalized_path)
    cache[normalized_path] = {
        'mtime': mtime,
        'size': size,
        'sha256': sha256
    }


def pre_backup(instance: str) -> bool:

    try:
        status = get_status(instance)
        logging.info(f'获取到实例 {instance} 的状态: {status}')
        if status == Status.RUNNING:
            logging.info(f'实例 {instance} 正在运行，尝试关闭自动保存')
            try:
                disable_auto_save(instance)
                logging.info(f'实例 {instance} 的自动保存已关闭，继续备份')
                return True
            except Exception as e:
                logging.warning(f'实例 {instance} 的自动保存关闭失败: {e}，停止备份')
                return False
        else:
            logging.info(f'实例 {instance} 未在运行，继续备份')
            return True
    except Exception as e:
        logging.warning(f'获取实例 {instance} 的状态失败: {e}，默认其未处于运行状态，继续备份')
        return True


def post_backup(instance: str):
    try:
        status = get_status(instance)
        logging.info(f'获取到实例 {instance} 的状态: {status}')
        if status == Status.RUNNING:
            logging.info(f'实例 {instance} 正在运行，尝试恢复自动保存')
            try:
                enable_auto_save(instance)
                logging.info(f'实例 {instance} 的自动保存已恢复')
            except Exception as e:
                logging.warning(f'实例 {instance} 的自动保存恢复失败: {e}')
    except Exception as e:
        logging.warning(f'获取实例 {instance} 的状态失败: {e}，无法恢复自动保存')


def load_cache(label: str) -> dict:
    cache_file = Path(f'.mcsm_bak.{label}.json')
    if cache_file.exists():
        with open(cache_file, 'r') as f:
            logging.info(f'读取缓存文件 {cache_file}')
            return json.load(f)
    else:
        logging.info(f'未找到缓存文件 {cache_file}，将创建新的缓存')
        return {}


def save_cache(label: str, cache: dict):
    cache_file = Path(f'.mcsm_bak.{label}.json')
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=4)
        logging.info(f'保存缓存文件 {cache_file}')


def backup_instance(instance: str, label: str):
    try:
        cwd = get_cwd(instance)
        logging.info(f'获取实例 {instance} 的当前工作目录: {cwd}')
    except Exception as e:
        logging.warning(f'获取实例 {instance} 的当前工作目录失败: {e}，停止备份')
        return
    os.chdir(cwd)

    if not pre_backup(instance):
        return

    cache = load_cache(label)

    try:
        for file in walk_files('.'):
            if is_excluded(file):
                logging.debug(f'跳过排除文件 {file}')
                continue
            if not should_backup(file, cache):
                logging.debug(f'跳过未修改文件 {file}')
                continue
            if backup_file(file, label, instance):
                update_cache(file, cache)
                logging.debug(f'成功上传文件 {file}')
            else:
                logging.warning(f'上传文件失败 {file}')
    finally:
        save_cache(label, cache)
        post_backup(instance)


def config_logging():
    # 配置日志
    logging.basicConfig(
        level=logging_level,
        format='[%(levelname)s][%(asctime)s] %(message)s',  # 日志格式：日志级别 + 时间 + 信息
        datefmt='%Y-%m-%d %H:%M:%S',  # 时间格式：年-月-日 时:分:秒
        handlers=[
            logging.StreamHandler()  # 输出到控制台
        ]
    )


def main():
    if len(sys.argv) < 2:
        logging.warning("使用方法: python(3) mcsm_bak.py <备份标签(如daily, weekly, monthly)>")
        sys.exit(1)
    label = sys.argv[1]

    for instance in instances.keys():
        logging.info(f'开始备份实例 {instance}，标签 {label}')
        backup_instance(instance, label)
        logging.info(f'实例 {instance} 的备份完成')


if __name__ == '__main__':
    main()

