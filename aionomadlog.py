# https://stackoverflow.com/a/37420223
# a dirty hack to make KeyboardInterrupt work again
import signal

signal.signal(signal.SIGINT, signal.SIG_DFL)

import asyncio
import base64
import fnmatch
import json
import random
from typing import Dict, List, Union
from uuid import uuid4

import aiohttp
import aiosqlite


from config import NOMAD_HOST, NOMAD_PORT


class TaskList:
    """
    ORM-like object. Must be initiated as 'await TaskList.init(...)'
    """

    def __init__(self, mask: str, db: str):
        self.mask = mask
        self.db = db

    @classmethod
    async def init(cls, mask: str = "*", db: str = ":memory:") -> TaskList:
        """The only way to properly instantiate the class

        Keyword Arguments:
            mask {str} -- glob-like task name pattern (default: {"*"})
            db {str} -- sqlite3-compatible database name (default: {":memory:"})

        Returns:
            TaskList -- class instance
        """

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

    async def refresh_all(self, tasks: List[Dict]) -> None:
        """Fully refreshes db and fills with new tasks

        Arguments:
            tasks {list} -- list of task dicts
        """

        await self.db.execute("DELETE FROM task")

        await self.db.executemany(
            "INSERT INTO "
            "task (uuid, alloc_id, name, type, offset, color)"
            "VALUES (:uuid, :alloc_id, :name, :type, :offset, :color)",
            tasks,
        )

    async def load(self) -> List[aiosqlite.Row]:
        """Loads a list of all tasks

        Returns:
            List[aiosqlite.Row] -- tasks. Behaves like a List[Dict]
        """

        async with self.db.execute("SELECT * FROM task") as cursor:
            tasks = await cursor.fetchall()

        return tasks

    async def update_offset(self, uuid: str, offset: Union[str, int]) -> None:
        """Updates a given task offset to eliminate old logs

        Arguments:
            uuid {str} -- task uuid
            offset {Union[str, int]} -- new offset value
        """

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
        log_delay: Union[int, float] = 1,
        max_log_length: int = 100000,
    ):
        """Async log streamer

        Arguments:
            host {str} -- Nomad host
            port {int} -- Nomad port
            task_mask {str} -- glob-like task name pattern

        Keyword Arguments:
            db {str} -- [description] (default: {":memory:"})
            log_delay {Union[int, float]} -- time to idle after finishing a batch (default: {1})
            max_log_length {int} -- tails log output if it's longer than this value (default: {100000})
        """

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

    async def _fetch_tasks(self) -> List[Dict]:
        """Requests allocations and extracts task matched with glob-like pattern

        Returns:
            List[Dict] -- a list of tasks

        """

        response = await self._session.get(self._allocations_url)
        allocations = await response.json()

        tasks = []
        for allocation in allocations:
            for task_name in allocation["TaskStates"].keys():
                # fnmatch allows matching Unix shell-style wildcards: * ? [seq] [!seq]
                if fnmatch.fnmatch(name=task_name, pat=self.task_mask):
                    color_code = random.choice(range(8))
                    color = f"\u001b[9{color_code}m"
                    task_uuid = str(uuid4())
                    alloc_id = allocation["ID"]

                    task = {
                        "uuid": task_uuid,
                        "alloc_id": alloc_id,
                        "name": task_name,
                        "type": "stderr",
                        "offset": 0,
                        "color": color,
                    }
                    tasks.append(task)

        return tasks

    async def _fetch_log(
        self, uuid: str, alloc_id: str, name: str, type: str, offset: str, color: str,
    ) -> dict:
        """Sends GET request, fixes json response and collects data
        Currently takes only the last chunk of the response to save time

        Arguments:
            uuid {str} -- task uuid
            alloc_id {str} -- task alloc_id
            name {str} -- task name
            type {str} -- task type
            offset {str} -- task offset
            color {str} -- task color

        Returns:
            dict -- a dict of values to be printed
        """

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
        """Refreshes allocations, extracts and saves new tasks
        """

        filtered_tasks = await self._fetch_tasks()
        await self.tasks.refresh_all(filtered_tasks)

    async def print_log(self, log: dict) -> None:
        """Prints [arguably] pretty logs

        Arguments:
            log {dict} -- [description]
        """

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
        """The main method of the logger.
        Loads a list of tasks and runs requests to them asynchronously;
        Prints the results

        """

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
        """Pauses execution to prevent aggressive request spam
        """

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
