import json
import logging
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path
from queue import Queue, Full
from typing import Iterator

from config import target_path, instances, logging_level, max_upload_threads
from mcsm_api import get_cwd, get_status, Status, disable_auto_save, enable_auto_save
from utils import get_file_mtime, get_file_size, get_file_sha256, normalize, is_excluded


def walk_files(base : str, current: str = None) -> Iterator[str]:
    base_path = Path(base)
    current_path = base_path if current is None else Path(current)

    for entry in sorted(current_path.iterdir()):
        try:
            if is_excluded(str(entry)):
                continue
            # 如果是文件，返回相对于 base 的路径
            if entry.is_file():
                yield str(entry.relative_to(base_path))  # 返回字符串形式的相对路径
            elif entry.is_dir():
                # 如果是目录，递归调用并返回子目录中的文件
                yield from walk_files(base, str(entry))
        except (FileNotFoundError, PermissionError) as e:
            logging.warning(f'访问文件或目录失败: {e}')
            continue


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


def should_backup(file_path: str, cache: dict) -> bool:
    normalized_path = normalize(file_path)
    if normalized_path not in cache:
        return True
    try:
        mtime = get_file_mtime(normalized_path)
        size = get_file_size(normalized_path)
    except (FileNotFoundError, PermissionError) as e:
        logging.warning(f'访问文件失败: {e}')
        return False
    if cache[normalized_path]['mtime'] == mtime and cache[normalized_path]['size'] == size:
        return False
    try:
        sha256 = get_file_sha256(normalized_path)
    except (FileNotFoundError, PermissionError) as e:
        logging.warning(f'访问文件失败: {e}')
        return False
    if cache[normalized_path]['sha256'] == sha256:
        return False
    return True


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
    logging.info(f'正在保存缓存文件 {cache_file} ...')
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=4)
        logging.info(f'保存缓存文件 {cache_file}')


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


stop_event = threading.Event()

def producer(file_queue: Queue, cache: dict, uploader_count: int):
    for file in walk_files('.'):
        if is_excluded(file):
            logging.debug(f'跳过排除文件 {file}')
            continue
        if not should_backup(file, cache):
            logging.debug(f'跳过未修改/无效文件 {file}')
            continue
        while True:
            if stop_event.is_set():
                logging.info('接收到停止信号，停止添加文件')
                return
            try:
                file_queue.put(file, timeout=1)
                break
            except Full:
                continue
    for _ in range(uploader_count):
        file_queue.put(None)


def uploader(file_queue: Queue, update_queue: Queue, label: str, instance: str):
    while True:
        if stop_event.is_set():
            logging.info('接收到停止信号，停止上传文件')
            update_queue.put(None)
            return
        file = file_queue.get()
        if file is None:
            logging.info('没有更多文件，当前线程停止上传')
            update_queue.put(None)
            return
        if backup_file(file, label, instance):
            update_queue.put(file)
            logging.debug(f'成功上传文件 {file}')
        else:
            logging.warning(f'上传文件失败 {file}')


def updater(update_queue: Queue, cache: dict, uploader_count: int):
    finished_uploader_count = 0
    while True:
        file = update_queue.get()
        if file is None:
            finished_uploader_count += 1
            if finished_uploader_count == uploader_count:
                logging.info('所有上传已完成，停止更新缓存')
                return
            continue
        try:
            update_cache(file, cache)
        except (FileNotFoundError, PermissionError) as e:
            logging.warning(f'文件 {file} 更新缓存失败: {e}')


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

    stop_event.clear()
    file_queue = Queue(maxsize=100)
    update_queue = Queue()
    cache = load_cache(label)

    threads = []
    producer_thread = threading.Thread(target=producer, args=(file_queue, cache, max_upload_threads))
    threads.append(producer_thread)
    producer_thread.start()
    for _ in range(max_upload_threads):
        uploader_thread = threading.Thread(target=uploader, args=(file_queue, update_queue, label, instance))
        threads.append(uploader_thread)
        uploader_thread.start()
    updater_thread = threading.Thread(target=updater, args=(update_queue, cache, max_upload_threads))
    threads.append(updater_thread)
    updater_thread.start()

    try:
        for thread in threads:
            thread.join()
        logging.info('所有线程已完成')
    except (KeyboardInterrupt, SystemExit):
        logging.info('接收到停止信号，正在停止所有线程...')
        stop_event.set()
        for thread in threads:
            thread.join()
        logging.info('所有线程已停止')
    finally:
        save_cache(label, cache)
        post_backup(instance)


def config_logging():
    # 配置日志
    logging.basicConfig(
        level=logging_level,
        format='[%(levelname)s] [%(asctime)s] %(message)s',  # 日志格式：日志级别 + 时间 + 信息
        datefmt='%Y-%m-%d %H:%M:%S',  # 时间格式：年-月-日 时:分:秒
        handlers=[
            logging.StreamHandler()  # 输出到控制台
        ]
    )


def handle_sigterm(signum, frame):
    logging.warning('接收到 SIGTERM 信号，正在退出...')
    raise SystemExit('程序被终止')


def main():
    signal.signal(signal.SIGTERM, handle_sigterm)
    config_logging()

    if len(sys.argv) < 2:
        logging.warning("使用方法: python(3) mcsm_bak.py <备份标签(如daily, weekly, monthly)>")
        sys.exit(1)
    label = sys.argv[1]

    for instance in instances.keys():
        if stop_event.is_set():
            return
        logging.info(f'开始备份实例 {instance}，标签 {label}')
        backup_instance(instance, label)
        logging.info(f'实例 {instance} 的备份结束')


if __name__ == '__main__':
    main()

