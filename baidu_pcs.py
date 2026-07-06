import hashlib
import json
import logging
import os
import sys
import tempfile
import time

SDK_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pythonsdk_20220616')
sys.path.insert(0, SDK_DIR)
from openapi_client.api import fileupload_api, auth_api  # noqa: E402
import openapi_client  # noqa: E402

TOKEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.baidu_token.json')

CHUNK_SIZE = 4 * 1024 * 1024


class BaiduPCSClient:
    def __init__(self, access_token, refresh_token, client_id, client_secret):
        self._access_token = access_token
        self._refresh_token = refresh_token
        self._client_id = client_id
        self._client_secret = client_secret
        self._api_client = openapi_client.ApiClient()
        self._upload_api = fileupload_api.FileuploadApi(self._api_client)

    def _refresh_access_token(self):
        auth = auth_api.AuthApi(self._api_client)
        resp = auth.oauth_token_refresh_token(
            self._refresh_token, self._client_id, self._client_secret)
        if 'error' in resp:
            logging.error(f"Token refresh failed: {resp.get('error_description', resp)}")
            raise Exception(f"Token refresh failed: {resp.get('error_description', resp)}")
        self._access_token = resp['access_token']
        if 'refresh_token' in resp:
            self._refresh_token = resp['refresh_token']
        with open(TOKEN_FILE, 'w') as f:
            json.dump({'refresh_token': self._refresh_token}, f)
        logging.info("Access token refreshed and saved")

    def mkdir(self, remote_path):
        try:
            resp = self._upload_api.xpanfilecreate(
                self._access_token, remote_path, 1, 0, '', '[]', rtype=0)
        except openapi_client.UnauthorizedException:
            self._refresh_access_token()
            return self.mkdir(remote_path)
        except Exception as e:
            logging.warning(f"Mkdir error: {e}")
            return False

        errno = resp.get('errno', 0)
        if errno in (0, -8):
            return True
        logging.warning(f"Mkdir failed: errno={errno}, {resp}")
        return False

    def upload(self, local_path, remote_path):
        try:
            file_size = os.path.getsize(local_path)
        except OSError as e:
            logging.warning(f"Cannot read file {local_path}: {e}")
            return False

        chunk_md5s = []
        try:
            with open(local_path, 'rb') as f:
                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    chunk_md5s.append(hashlib.md5(chunk).hexdigest())
        except OSError as e:
            logging.warning(f"Cannot read file {local_path}: {e}")
            return False

        block_list = json.dumps(chunk_md5s)
        return self._do_upload(local_path, remote_path, file_size, block_list, chunk_md5s)

    def _do_upload(self, local_path, remote_path, file_size, block_list, chunk_md5s):
        try:
            precreate = self._upload_api.xpanfileprecreate(
                self._access_token, remote_path, 0, file_size, 1, block_list, rtype=3)
        except openapi_client.UnauthorizedException:
            self._refresh_access_token()
            return self._do_upload(local_path, remote_path, file_size, block_list, chunk_md5s)
        except Exception as e:
            logging.warning(f"Precreate error: {e}")
            return False

        errno = precreate.get('errno', 0)
        if errno != 0:
            logging.warning(f"Precreate failed: errno={errno}, {precreate}")
            return False

        uploadid = precreate['uploadid']

        if len(chunk_md5s) == 1:
            try:
                with open(local_path, 'rb') as f:
                    super_resp = self._upload_api.pcssuperfile2(
                        self._access_token, '0', remote_path, uploadid, 'tmpfile', file=f)
            except openapi_client.UnauthorizedException:
                self._refresh_access_token()
                return self._do_upload(local_path, remote_path, file_size, block_list, chunk_md5s)
            except Exception as e:
                logging.warning(f"Superfile2 error: {e}")
                return False

            errno = super_resp.get('errno', 0)
            if errno != 0:
                logging.warning(f"Superfile2 failed: errno={errno}, {super_resp}")
                return False
        else:
            for i in range(len(chunk_md5s)):
                try:
                    with open(local_path, 'rb') as f:
                        f.seek(i * CHUNK_SIZE)
                        chunk_data = f.read(CHUNK_SIZE)
                except OSError as e:
                    logging.warning(f"Cannot read chunk {i} of {local_path}: {e}")
                    return False

                tmp = tempfile.NamedTemporaryFile(suffix='.part', delete=False)
                tmp_path = tmp.name
                try:
                    tmp.write(chunk_data)
                    tmp.close()
                    with open(tmp_path, 'rb') as chunk_file:
                        super_resp = self._upload_api.pcssuperfile2(
                            self._access_token, str(i), remote_path, uploadid, 'tmpfile', file=chunk_file)
                except openapi_client.UnauthorizedException:
                    self._refresh_access_token()
                    return self._do_upload(local_path, remote_path, file_size, block_list, chunk_md5s)
                except Exception as e:
                    logging.warning(f"Superfile2 error for chunk {i}: {e}")
                    return False
                finally:
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass

                errno = super_resp.get('errno', 0)
                if errno != 0:
                    logging.warning(f"Superfile2 failed for chunk {i}: errno={errno}, {super_resp}")
                    return False

        try:
            create = self._upload_api.xpanfilecreate(
                self._access_token, remote_path, 0, file_size, uploadid, block_list, rtype=3)
        except openapi_client.UnauthorizedException:
            self._refresh_access_token()
            return self._do_upload(local_path, remote_path, file_size, block_list, chunk_md5s)
        except Exception as e:
            logging.warning(f"Create error: {e}")
            return False

        errno = create.get('errno', 0)
        if errno != 0:
            logging.warning(f"Create failed: errno={errno}, {create}")
            return False

        return True


def device_auth(client_id, client_secret):
    api_client = openapi_client.ApiClient()
    auth = auth_api.AuthApi(api_client)

    resp = auth.oauth_token_device_code(client_id, 'basic,netdisk')
    device_code = resp['device_code']
    user_code = resp['user_code']
    qrcode_url = resp.get('qrcode_url', '')
    interval = resp.get('interval', 5)

    print(f"\nComplete authorization at:", flush=True)
    if qrcode_url:
        print(f"  {qrcode_url}", flush=True)
    print(f"  User code: {user_code}", flush=True)

    auth2 = auth_api.AuthApi(openapi_client.ApiClient())
    logging.info("Waiting for device authorization...")
    sys.stdout.flush()

    while True:
        time.sleep(interval)
        try:
            resp = auth2.oauth_token_device_token(device_code, client_id, client_secret)
        except openapi_client.ApiException as e:
            if 'authorization_pending' in (e.body or ''):
                continue
            elif 'slow_down' in (e.body or ''):
                time.sleep(10)
                continue
            else:
                logging.error(f"Device token error: {e}")
                return None, None

        if 'access_token' in resp:
            access_token = resp['access_token']
            refresh_token = resp['refresh_token']
            with open(TOKEN_FILE, 'w') as f:
                json.dump({'refresh_token': refresh_token}, f)
            logging.info(f"Device authorized, token saved to {TOKEN_FILE}")
            return access_token, refresh_token
        else:
            logging.error(f"Device authorization failed: {resp}")
            return None, None


def create_client(client_id=None, client_secret=None):
    if client_id is None or client_secret is None:
        from config import baidu_client_id, baidu_client_secret
        client_id = baidu_client_id
        client_secret = baidu_client_secret

    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, 'r') as f:
                data = json.load(f)
                refresh_token = data.get('refresh_token')
                if refresh_token:
                    client = BaiduPCSClient('', refresh_token, client_id, client_secret)
                    client._refresh_access_token()
                    return client
        except Exception as e:
            logging.error(f"Failed to load or refresh token: {e}")

    logging.info("No valid token found, starting first-time authorization...")
    access_token, refresh_token = device_auth(client_id, client_secret)
    if access_token:
        return BaiduPCSClient(access_token, refresh_token, client_id, client_secret)
    return None


if __name__ == '__main__':
    import config
    device_auth(config.baidu_client_id, config.baidu_client_secret)
