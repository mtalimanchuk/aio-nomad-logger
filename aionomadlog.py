# https://stackoverflow.com/a/37420223
# a dirty hack to make KeyboardInterrupt work again
import signal

signal.signal(signal.SIGINT, signal.SIG_DFL)

import asyncio
import base64
import fnmatch
import json
import random
from uuid import uuid4

import aiohttp
import aiosqlite


from config import NOMAD_HOST, NOMAD_PORT


class TaskList:
    def __init__(self, mask, db):
        self.mask = mask
        self.db = db

    @classmethod
    async def init(cls, mask, db=":memory:"):
        self = TaskList(mask, db)
        self.db = await aiosqlite.connect(db)
        self.db.row_factory = aiosqlite.Row

        await self.db.execute(
            "CREATE TABLE IF NOT EXISTS task("
            "uuid TEXT,"
            "alloc_id TEXT,"
            "name TEXT,"
            "type TEXT,"
            "offset TEXT,"
            "color TEXT"
            ")"
        )

        return self

    async def refresh_all(self, tasks: list):
        await self.db.execute("DELETE FROM task")

        await self.db.executemany(
            "INSERT INTO "
            "task (uuid, alloc_id, name, type, offset, color)"
            "VALUES (:uuid, :alloc_id, :name, :type, :offset, :color)",
            tasks,
        )

    async def load(self):
        async with self.db.execute("SELECT * FROM task") as cursor:
            tasks = await cursor.fetchall()

        return tasks

    async def update_offset(self, uuid, offset):
        await self.db.execute(
            "UPDATE task SET offset=? WHERE uuid=?", (offset, uuid)
        )


class NomadLogger:
    def __init__(
        self,
        host: str,
        port: int,
        task_mask: str,
        db: str = ":memory:",
        log_delay: int = 1,
        max_log_length: int = 100000,
    ):
        self.host = host
        self.port = port

        self._session = aiohttp.ClientSession()
        self._base_url = f"http://{host}:{port}/v1"
        self._allocations_url = f"{self._base_url}/allocations"
        self._logs_url = f"{self._base_url}/client/fs/logs"

        self._log_delay = log_delay
        self._max_log_length = max_log_length

        self._db = db

        self.task_mask = task_mask

    async def __aenter__(self):
        self.tasks = await TaskList.init(mask=self.task_mask, db=self._db)
        await self.refresh_sources()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._session.close()

    async def _fetch_tasks(self) -> list:
        response = await self._session.get(self._allocations_url)
        allocations = await response.json()

        tasks = []
        for allocation in allocations:
            for task_name in allocation["TaskStates"].keys():
                # fnmatch allows matching Unix shell-style wildcards: * ? [seq] [!seq]
                if fnmatch.fnmatch(name=task_name, pat="Statistics*"):
                    color_code = random.choice(range(8))
                    color = f"\u001b[9{color_code}m"
                    task_uuid = str(uuid4())
                    alloc_id = allocation["ID"]
                    # now = datetime.utcnow()

                    task = {
                        "uuid": task_uuid,
                        "alloc_id": alloc_id,
                        "name": task_name,
                        "type": "stderr",
                        "offset": 0,
                        "color": color,
                        # "last_updated": f"{now:%Y-%m-%dT%H:%M:%sZ}",
                    }
                    tasks.append(task)

        return tasks

    async def _fetch_log(
        self, uuid: str, alloc_id: str, name: str, type: str, offset: str, color: str,
    ) -> dict:
        url = f"{self._logs_url}/{alloc_id}?task={name}&type={type}&offset={offset}"
        response = await self._session.get(url)
        raw_text = await response.text()

        for i, char in enumerate(raw_text[::-1]):
            if char == "{":
                raw_text = raw_text[-i-1:]
                break

        try:
            log_json = json.loads(raw_text)
            data = log_json.get("Data", "")
            text = base64.b64decode(data).decode("utf-8").strip()
            filename = log_json.get("File", "Unknown file")
            offset = log_json.get("Offset", 0)

            if int(offset) > 0:
                await self.tasks.update_offset(uuid, offset)

            log_data = {
                "text": text,
                "filename": filename,
                "offset": offset,
                "color": color,
            }

        except json.JSONDecodeError:
            log_data = {}

        return log_data

    async def refresh_sources(self) -> None:
        filtered_tasks = await self._fetch_tasks()
        await self.tasks.refresh_all(filtered_tasks)

    async def print_log(self, log: dict) -> None:
        if log.get("text"):
            color = log["color"]
            color_stop = "\u001b[0m"
            text = log["text"]
            text_length = len(text)
            filename = log["filename"]
            offset = log["offset"]
            delimiter = "=" * 20

            if text_length > self._max_log_length:
                text = text[-self._max_log_length:]
                text = (
                    f"... Log output truncated because too long ({text_length} chars) ...\n"
                    f"{text}"
                )

            print(
                f"{color}{delimiter} {filename} @offset {offset} {delimiter}\n"
                f"{text}{color_stop}"
            )

    async def stream_batch(self) -> None:
        tasks = await self.tasks.load()

        aiotasks = []
        for task_data in tasks:
            coroutine = self._fetch_log(**task_data)
            aiotask = asyncio.create_task(coroutine)
            aiotasks.append(aiotask)

        for completed_task in asyncio.as_completed(aiotasks):
            completed_task_data = await completed_task
            await self.print_log(completed_task_data)

    async def idle(self) -> None:
        await asyncio.sleep(self._log_delay)


async def main() -> None:
    async with NomadLogger(
        NOMAD_HOST,
        NOMAD_PORT,
        "Statistics*",
        db=":memory:",
        log_delay=1,
        max_log_length=1000,
    ) as logger:
        while True:
            await logger.stream_batch()
            await logger.idle()


if __name__ == "__main__":
    asyncio.run(main())
