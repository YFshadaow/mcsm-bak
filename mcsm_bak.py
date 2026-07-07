import json
import logging
import os
import signal
import sys
import threading
from pathlib import Path
from queue import Queue, Full, Empty
from typing import Iterator

from config import target_path, instances, logging_level, max_upload_threads, baidu_client_id, baidu_client_secret
from mcsm_api import get_cwd, get_status, Status, disable_auto_save, enable_auto_save
from utils import get_file_mtime, get_file_size, get_file_sha256, normalize, is_excluded
from baidu_pcs import create_client
from cache_db import open_db, load_cache, write_entry, DB_DIR


pcs_client = None


def walk_files(base: str, current: str = None) -> Iterator[str]:
    base_path = Path(base)
    current_path = base_path if current is None else Path(current)

    for entry in sorted(current_path.iterdir()):
        try:
            if is_excluded(str(entry)):
                continue
            # Yield relative path as string if entry is a file
            if entry.is_file():
                yield str(entry.relative_to(base_path))
            elif entry.is_dir():
                # Recursively yield files from subdirectories if entry is a directory
                yield from walk_files(base, str(entry))
        except (FileNotFoundError, PermissionError) as e:
            logging.warning(f'访问文件或目录失败: {e}')
            continue


def backup_file(file_path: str, label: str, instance: str) -> bool:
    normalized_path = normalize(file_path)
    remote_path = f'{target_path}/{label}/{instance}/{normalized_path}'
    return pcs_client.upload(normalized_path, remote_path)


def should_backup(file_path: str, cache: dict) -> tuple:
    normalized_path = normalize(file_path)
    
    try:
        mtime = get_file_mtime(normalized_path)
        size = get_file_size(normalized_path)
    except (FileNotFoundError, PermissionError) as e:
        logging.warning(f'访问文件失败: {e}')
        return False, {}
        
    is_new_or_changed = False
    if normalized_path not in cache:
        is_new_or_changed = True
    elif cache[normalized_path]['mtime'] != mtime or cache[normalized_path]['size'] != size:
        is_new_or_changed = True
        
    if is_new_or_changed:
        try:
            sha256 = get_file_sha256(normalized_path)
        except (FileNotFoundError, PermissionError) as e:
            logging.warning(f'访问文件失败: {e}')
            return False, {}
        return True, {'mtime': mtime, 'size': size, 'sha256': sha256}
        
    return False, {}


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


def update_cache(file_path: str, file_meta: dict, cache: dict):
    normalized_path = normalize(file_path)
    cache[normalized_path] = file_meta


stop_event = threading.Event()

def producer(file_queue: Queue, cache: dict, uploader_count: int):
    for file in walk_files('.'):
        if is_excluded(file):
            logging.debug(f'跳过排除文件 {file}')
            continue
            
        needs_backup, file_meta = should_backup(file, cache)
        if not needs_backup:
            logging.debug(f'跳过未修改或无效文件 {file}')
            continue
            
        while True:
            if stop_event.is_set():
                logging.info('接收到停止信号，停止添加文件')
                break
            try:
                file_queue.put((file, file_meta), timeout=1)
                break
            except Full:
                continue
        if stop_event.is_set():
            break
                
    for _ in range(uploader_count):
        file_queue.put(None)


def uploader(file_queue: Queue, update_queue: Queue, label: str, instance: str):
    while True:
        if stop_event.is_set():
            logging.info('接收到停止信号，停止上传文件')
            update_queue.put(None)
            return
            
        try:
            task = file_queue.get(timeout=1)
        except Empty:
            continue
        if task is None:
            logging.info('没有更多文件，当前线程停止上传')
            update_queue.put(None)
            return
            
        file, file_meta = task
        try:
            ok = backup_file(file, label, instance)
        except Exception as e:
            logging.warning(f'上传文件异常 {file}: {e}')
            ok = False
        if ok:
            update_queue.put((file, file_meta))
            logging.debug(f'成功上传文件 {file}')
        else:
            logging.warning(f'上传文件失败 {file}')


def updater(update_queue: Queue, cache: dict, uploader_count: int, conn):
    finished_uploader_count = 0
    while True:
        task = update_queue.get()
        if task is None:
            finished_uploader_count += 1
            if finished_uploader_count == uploader_count:
                logging.info('所有上传已完成，停止更新缓存')
                return
            continue
            
        file, file_meta = task
        update_cache(file, file_meta, cache)
        try:
            write_entry(conn, file, file_meta)
        except Exception as e:
            logging.error(f'写入缓存失败 {file}: {e}')


def backup_instance(instance: str, label: str):
    try:
        cwd = get_cwd(instance)
        logging.info(f'获取实例 {instance} 的当前工作目录: {cwd}')
    except Exception as e:
        logging.warning(f'获取实例 {instance} 的当前工作目录失败: {e}，停止备份')
        return
    os.chdir(cwd)

    pcs_client.mkdir(f'{target_path}/{label}/{instance}')

    if not pre_backup(instance):
        return

    stop_event.clear()
    file_queue = Queue(maxsize=100)
    update_queue = Queue()

    conn = None
    try:
        conn = open_db(label, instance)
    except Exception as e:
        logging.error(f'打开数据库失败 {label}/{instance}: {e}')
        return
    cache = load_cache(conn)

    threads = []
    producer_thread = threading.Thread(target=producer, args=(file_queue, cache, max_upload_threads))
    threads.append(producer_thread)
    producer_thread.start()
    
    for _ in range(max_upload_threads):
        uploader_thread = threading.Thread(target=uploader, args=(file_queue, update_queue, label, instance))
        threads.append(uploader_thread)
        uploader_thread.start()
        
    updater_thread = threading.Thread(target=updater, args=(update_queue, cache, max_upload_threads, conn))
    threads.append(updater_thread)
    updater_thread.start()

    try:
        for thread in threads:
            thread.join()
        logging.info('所有线程已完成')
    except (KeyboardInterrupt, SystemExit):
        logging.info('接收到停止信号，正在停止所有线程')
        stop_event.set()
        for thread in threads:
            thread.join()
        logging.info('所有线程已停止')
    finally:
        if conn:
            conn.close()
        post_backup(instance)


def config_logging():
    # Configure logging module parameters
    logging.basicConfig(
        level=logging_level,
        format='[%(levelname)s] [%(asctime)s] %(message)s',  # Set log format to include level time and message
        datefmt='%Y-%m-%d %H:%M:%S',  # Set datetime format for logging output
        handlers=[
            logging.StreamHandler()  # Output logs directly to the console stream
        ]
    )


def handle_sigterm(signum, frame):
    logging.warning('接收到 SIGTERM 信号，正在退出')
    raise SystemExit('程序被终止')


def dump_cache(label, instance=None):
    if instance:
        conn = open_db(label, instance)
        cache = load_cache(conn)
        conn.close()
        if not cache:
            logging.info(f'{label}/{instance}: no cache entries')
            return
        dump_path = os.path.join(DB_DIR, label, instance, 'dump.json')
        with open(dump_path, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=4, ensure_ascii=False)
        logging.info(f'{label}/{instance}: {len(cache)} entries → {dump_path}')
    else:
        all_cache = {}
        for inst in instances.keys():
            conn = open_db(label, inst)
            cache = load_cache(conn)
            conn.close()
            if cache:
                all_cache[inst] = cache
        if not all_cache:
            logging.info(f'{label}: no cache entries')
            return
        dump_path = os.path.join(DB_DIR, label, 'dump.json')
        with open(dump_path, 'w', encoding='utf-8') as f:
            json.dump(all_cache, f, indent=4, ensure_ascii=False)
        logging.info(f'{label}: {sum(len(c) for c in all_cache.values())} entries → {dump_path}')


def main():
    signal.signal(signal.SIGTERM, handle_sigterm)
    config_logging()

    if len(sys.argv) < 2:
        logging.warning("使用方法: python(3) mcsm_bak.py <备份标签> [--dump] [--instance <name>]")
        sys.exit(1)
    label = sys.argv[1]

    if '--dump' in sys.argv:
        instance = None
        try:
            idx = sys.argv.index('--instance')
            instance = sys.argv[idx + 1]
        except (ValueError, IndexError):
            pass
        dump_cache(label, instance)
        return

    global pcs_client
    pcs_client = create_client(baidu_client_id, baidu_client_secret)
    if pcs_client is None:
        logging.error('Failed to initialize BaiduPCSClient')
        sys.exit(1)

    for instance in instances.keys():
        if stop_event.is_set():
            return
        logging.info(f'开始备份实例 {instance}，标签 {label}')
        try:
            backup_instance(instance, label)
        except Exception as e:
            logging.error(f'实例 {instance} 备份异常: {e}')
        logging.info(f'实例 {instance} 的备份结束')


if __name__ == '__main__':
    main()