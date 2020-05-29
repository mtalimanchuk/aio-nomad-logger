import base64
import fnmatch
import json
import re
import sys
import time

import grequests

from config import NOMAD_HOST, NOMAD_PORT


class Task:
    def __init__(self, name, alloc_id, offset, type, url_prefix):
        self.name = name
        self.alloc_id = alloc_id
        self.offset = offset
        self.type = type
        self.url_prefix = url_prefix

    @property
    def request(self):
        url = f"{self.url_prefix}/{self.alloc_id}?task={self.name}&type={self.type}&offset={self.offset}"
        return grequests.get(url)


class NomadTaskLogger:
    def __init__(self, host, port, task_name_glob):
        self.host = host
        self.port = port

        self.session = grequests.Session()
        self._base_url = f"http://{host}:{port}/v1"
        self._allocations_url = f"{self._base_url}/allocations"
        self._logs_url = f"{self._base_url}/client/fs/logs"
        self.log_delay = 2

        self.task_name_glob = task_name_glob
        self.task_cache = self.refresh_task_list()

    def refresh_task_list(self):
        tasks = []

        response = self.session.get(self._allocations_url)
        for alloc in response.json():
            for task_name in alloc["TaskStates"].keys():
                if fnmatch.fnmatch(name=task_name, pat=self.task_name_glob):
                    alloc_id = alloc["ID"]
                    task = Task(task_name, alloc_id, 0, "stderr", self._logs_url)
                    tasks.append(task)

        return tasks

    def parse_response_batch(self, responses):
        return [self._parse_log(r) for r in responses]

    def _create_log_request(self, alloc_id, name, type, offset):
        url = f"{self._logs_url}/{alloc_id}?task={name}&type={type}&offset={offset}"
        return grequests.get(url)

    def _parse_log(self, response):
        raw_text = response.text

        text = ""
        filename = ""

        for raw_json in re.findall(r"{.*?}", raw_text):
            log_json = json.loads(raw_json)

            data = log_json.get("Data", "")
            text += base64.b64decode(data).decode("utf-8").strip()
            filename = log_json.get("File", "Unknown file")
            offset = log_json.get("Offset", 0)

        return {"text": text, "filename": filename, "offset": offset}

    def stream(self):
        while True:
            request_batch = (task.request for task in self.task_cache)
            responses = grequests.map(request_batch)
            print(responses)
            for index, data in enumerate(self.parse_response_batch(responses)):
                text = data["text"]
                offset = data["offset"]
                filename = data["filename"]

                print(f"{filename:100} until offset {offset}\n{text}")

                self.task_cache[index].offset = offset


if __name__ == "__main__":
    patterns = sys.argv[1:]
    pattern = "|".join(patterns)

    try:
        logger = NomadTaskLogger(NOMAD_HOST, NOMAD_PORT, pattern)
        print(logger.task_cache)
        logger.stream()
    except KeyboardInterrupt:
        sys.exit("\nStreaming interrupted!")
