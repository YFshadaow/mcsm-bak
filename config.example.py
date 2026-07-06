import logging

max_upload_threads = 4
logging_level = logging.INFO
target_path = '/apps/mcsm_bak'
mcsm_url = 'https://my.console.com:23333'
api_key = '123abc'
daemonId = '123abc'
instances = {
    'survival': '123abc',
}
exclusions = [
    r'.*/dynmap/web(/.*)?$',
    r'.*/CoreProtect/database\.db$'
]

baidu_client_id = '123abc'
baidu_client_secret = '123abc'
