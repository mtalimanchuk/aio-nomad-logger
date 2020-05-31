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
        self, host: str, port: int, task_name_glob: str, save_path: str = "tasks.json",
    ):
        self.host = host
        self.port = port

        self._session = aiohttp.ClientSession()
        self._base_url = f"http://{host}:{port}/v1"
        self._allocations_url = f"{self._base_url}/allocations"
        self._logs_url = f"{self._base_url}/client/fs/logs"

        self._log_delay = 2
        self._token_bytes = 4

        self._save_path = save_path

        self.task_name_glob = task_name_glob
        print(self.__dict__)

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
                    task_id = token_hex(self._token_bytes)
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
        await self._save_tasks(tasks)

        return {"text": text, "filename": filename, "offset": offset}

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
        await self._save_tasks(filtered_tasks)

    async def stream_batch(self):
        tasks = await self._load_tasks()
        coroutines = (
            self._fetch_log(task_id, **task_data)
            for task_id, task_data in tasks.items()
        )
        log_batch = await asyncio.gather(*coroutines)
        return log_batch


async def main():
    do_allocation_refresh = True
    async with NomadTaskList(NOMAD_HOST, NOMAD_PORT, "Statistics*") as logger:
        while True:
            print("New batch")
            if do_allocation_refresh:
                await logger.refresh_sources()
                do_allocation_refresh = False
            else:
                batch = await logger.stream_batch()
                for log in batch:
                    if len(log["text"]) > 10000:
                        log["text"] = "Too long to print"
                    print(
                        f"{log['filename']} @offset {log['offset']} {'=' * 30}\n{log['text']}"
                    )
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
