from enum import Enum

import requests
from requests import Response

from config import api_key, mcsm_url, instances, daemonId

basic_params ={
    "apikey": api_key,
}

headers = {
    "Content-Type": "application/json; charset=utf-8",
    "X-Requested-With": "XMLHttpRequest"
}

def send_request(api_path: str, params: dict) -> Response:
    return requests.get(mcsm_url + api_path, params=basic_params | params, headers=headers)


def get_cwd(server: str) -> str:
    uuid = instances[server]
    response = send_request('/api/instance', {
        'uuid': uuid,
        'daemonId': daemonId
    })
    response.raise_for_status()
    response_json = response.json()
    return response_json['data']['config']['cwd']


class Status(Enum):
    BUSY = -1
    STOPPED = 0
    STOPPING = 1
    STARTING = 2
    RUNNING = 3


def get_status(server: str) -> Status:
    uuid = instances[server]
    response = send_request('/api/instance', {
        'uuid': uuid,
        'daemonId': daemonId
    })
    response.raise_for_status()
    response_json = response.json()
    status = response_json['data']['status']
    return Status(status)


def enable_auto_save(server: str):
    uuid = instances[server]
    response = send_request('/api/protected_instance/command', {
        'uuid': uuid,
        'daemonId': daemonId,
        'command': 'save-on'
    })
    response.raise_for_status()


def disable_auto_save(server: str):
    uuid = instances[server]
    response = send_request('/api/protected_instance/command', {
        'uuid': uuid,
        'daemonId': daemonId,
        'command': 'save-off'
    })
    response.raise_for_status()