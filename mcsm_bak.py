import json
import os
import re
import subprocess
import sys
from pathlib import Path

from config import target_path, exclusions, instances
from mcsm_api import get_cwd, get_status, Status, disable_auto_save, enable_auto_save
from utils import get_file_mtime, get_file_size, get_file_sha256


def normalize(relative_path: str) -> str:
    # 使用Path处理路径，并返回标准化的相对路径
    return str(Path(relative_path).relative_to(Path('.')).resolve())


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


def is_excluded(file_path: str) -> bool:
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
        print(f'获取到实例 {instance} 的状态: {status}')
        if status == Status.RUNNING:
            print(f'实例 {instance} 正在运行，尝试关闭自动保存')
            try:
                disable_auto_save(instance)
                print(f'实例 {instance} 的自动保存已关闭，继续备份')
                return True
            except Exception as e:
                print(f'实例 {instance} 的自动保存关闭失败: {e}，停止备份')
                return False
        else:
            print(f'实例 {instance} 未在运行，继续备份')
            return True
    except Exception as e:
        print(f'获取实例 {instance} 的状态失败: {e}，默认其未处于运行状态，继续备份')
        return True


def post_backup(instance: str):
    try:
        status = get_status(instance)
        print(f'获取到实例 {instance} 的状态: {status}')
        if status == Status.RUNNING:
            print(f'实例 {instance} 正在运行，尝试恢复自动保存')
            try:
                enable_auto_save(instance)
                print(f'实例 {instance} 的自动保存已恢复')
            except Exception as e:
                print(f'实例 {instance} 的自动保存恢复失败: {e}')
    except Exception as e:
        print(f'获取实例 {instance} 的状态失败: {e}，无法恢复自动保存')


def load_cache(label: str) -> dict:
    cache_file = Path(f'.mcsm_bak.{label}.json')
    if cache_file.exists():
        with open(cache_file, 'r') as f:
            print(f'读取缓存文件 {cache_file}')
            return json.load(f)
    else:
        print(f'未找到缓存文件 {cache_file}，将创建新的缓存')
        return {}


def save_cache(label: str, cache: dict):
    cache_file = Path(f'.mcsm_bak.{label}.json')
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=4)
        print(f'保存缓存文件 {cache_file}')


def backup_instance(instance: str, label: str):
    try:
        cwd = get_cwd(instance)
        print(f'获取实例 {instance} 的当前工作目录: {cwd}')
    except Exception as e:
        print(f'获取实例 {instance} 的当前工作目录失败: {e}，停止备份')
        return
    os.chdir(cwd)

    if not pre_backup(instance):
        return

    cache = load_cache(label)

    base_dir = Path('.')  # 当前目录
    files = [str(path.relative_to(base_dir)) for path in base_dir.rglob('*') if path.is_file()]

    try:
        for file in files:
            if is_excluded(file):
                print(f'跳过排除文件 {file}')
                continue
            if not should_backup(file, cache):
                print(f'跳过未修改文件 {file}')
                continue
            if backup_file(file, label, instance):
                update_cache(file, cache)
                print(f'成功上传文件 {file}')
            else:
                print(f'上传文件失败 {file}')
    finally:
        save_cache(label, cache)
        post_backup(instance)


def main():
    if len(sys.argv) < 2:
        print("使用方法: python(3) mcsm_bak.py <备份标签(如daily, weekly, monthly)>")
        sys.exit(1)
    label = sys.argv[1]

    for instance in instances.keys():
        print(f'开始备份实例 {instance}，标签 {label}')
        backup_instance(instance, label)
        print(f'实例 {instance} 的备份完成')


if __name__ == '__main__':
    main()

