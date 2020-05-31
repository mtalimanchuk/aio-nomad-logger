# https://stackoverflow.com/a/37420223
# a dirty hack to make KeyboardInterrupt work again
import signal

signal.signal(signal.SIGINT, signal.SIG_DFL)

import asyncio
import base64
from datetime import datetime
import fnmatch
import json
import re
from secrets import token_hex

import aiofiles
import aiohttp

from config import NOMAD_HOST, NOMAD_PORT


class NomadTaskList:
    def __init__(
        self,
        host: str,
        port: int,
        task_name_glob: str,
        save_path: str = "tasks.json",
        log_delay: int = 1,
        max_log_length: int = 100000,
    ):
        self.host = host
        self.port = port

        self._session = aiohttp.ClientSession()
        self._base_url = f"http://{host}:{port}/v1"
        self._allocations_url = f"{self._base_url}/allocations"
        self._logs_url = f"{self._base_url}/client/fs/logs"

        self._token_bytes = 3   # used as random hex color
        self._log_delay = log_delay
        self._max_log_length = max_log_length

        self._save_path = save_path

        self.task_name_glob = task_name_glob

    async def __aenter__(self):
        await self.refresh_sources()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._session.close()

    async def _fetch_tasks(self) -> dict:
        response = await self._session.get(self._allocations_url)
        allocations = await response.json()

        tasks = {}
        for allocation in allocations:
            for task_name in allocation["TaskStates"].keys():
                # fnmatch allows matching Unix shell-style wildcards: * ? [seq] [!seq]
                if fnmatch.fnmatch(name=task_name, pat="Statistics*"):

                    import random
                    random_digit = random.choice(range(8))
                    color = f"\u001b[9{random_digit}m"

                    task_id = color
                    alloc_id = allocation["ID"]
                    now = datetime.utcnow()

                    tasks[task_id] = {
                        "alloc_id": alloc_id,
                        "name": task_name,
                        "type": "stderr",
                        "offset": 0,
                        # "last_updated": f"{now:%Y-%m-%dT%H:%M:%sZ}",
                    }

        return tasks

    async def _fetch_log(self, task_id, alloc_id, name, type, offset) -> dict:
        url = f"{self._logs_url}/{alloc_id}?task={name}&type={type}&offset={offset}"
        print(f"Fetching {name}")
        response = await self._session.get(url)
        raw_text = await response.text()

        text = ""
        filename = ""

        chunks = re.findall(r"{.*?}", raw_text)

        for raw_json in chunks:
            log_json = json.loads(raw_json)

            data = log_json.get("Data", "")
            text += base64.b64decode(data).decode("utf-8").strip()
            filename = log_json.get("File", "Unknown file")
            offset = log_json.get("Offset", 0)

        tasks = await self._load_tasks()
        tasks[task_id]["offset"] = offset
        if not tasks:
            print("gotcha!")
        await self._save_tasks(tasks)
        print(f"Fetched and saved {name}")

        return {"task_id": task_id, "text": text, "filename": filename, "offset": offset}

    async def _save_tasks(self, tasks: dict) -> None:
        async with aiofiles.open(self._save_path, mode="w") as alloc_f:
            content = json.dumps(tasks, indent=4)
            await alloc_f.write(content)

    async def _load_tasks(self) -> dict:
        async with aiofiles.open(self._save_path, mode="r") as alloc_f:
            content = await alloc_f.read()
            tasks = json.loads(content)
            return tasks

    async def refresh_sources(self) -> None:
        filtered_tasks = await self._fetch_tasks()
        if not filtered_tasks:
            print("gotcha2!")
        await self._save_tasks(filtered_tasks)

    def print_log(self, log):
        if log.get("text"):

            color = log["task_id"]
            color_stop = "\u001b[0m"

            text = log["text"]
            filename = log["filename"]
            offset = log['offset']
            delimiter = '=' * 20

            if len(text) > self._max_log_length:
                text = "... too long to print ..."

            print(
                f"{color}{delimiter} {filename} @offset {offset} {delimiter}\n{text}{color_stop}"
            )

    async def stream_batch(self):
        tasks = await self._load_tasks()
        aiotasks = []
        for task_id, task_data in tasks.items():
            coroutine = self._fetch_log(task_id, **task_data)
            aiotask = asyncio.create_task(coroutine)
            aiotasks.append(aiotask)

        for completed_task in asyncio.as_completed(aiotasks):
            completed_task_data = await completed_task
            self.print_log(completed_task_data)

    async def idle(self):
        await asyncio.sleep(self._log_delay)


async def main():
    do_allocation_refresh = True
    async with NomadTaskList(NOMAD_HOST, NOMAD_PORT, "Statistics*") as logger:
        while True:
            print(f"\n{'*' * 100}\n")
            if do_allocation_refresh:
                await logger.refresh_sources()
                do_allocation_refresh = False
            else:
                await logger.stream_batch()
                await logger.idle()


if __name__ == "__main__":
    asyncio.run(main())
