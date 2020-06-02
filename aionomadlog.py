# https://stackoverflow.com/a/37420223
# a dirty hack to make KeyboardInterrupt work again
import signal

signal.signal(signal.SIGINT, signal.SIG_DFL)

import asyncio
import base64
import fnmatch
import json
import random
import sys
from typing import Dict, List, Union
from uuid import uuid4

import aiohttp
import aiosqlite
import click


class TaskList:
    """
    ORM-like object. Must be initiated as 'await TaskList.init(...)'
    """

    def __init__(self, mask: str, db: str):
        self.mask = mask
        self.db = db

    @classmethod
    async def init(cls, mask: str, db: str):
        """The only way to properly instantiate the class

        Keyword Arguments:
            mask {str} -- glob-like task name pattern
            db {str} -- sqlite3-compatible database name

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
            tasks {List[Dict]} -- list of task dicts
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

        await self.db.execute("UPDATE task SET offset=? WHERE uuid=?", (offset, uuid))


class NomadLogger:
    def __init__(
        self,
        host: str,
        port: int,
        mask: str,
        db: str,
        idle_time: Union[int, float],
        max_log_length: int,
    ):
        """Async log streamer

        Arguments:
            host {str} -- Nomad host
            port {int} -- Nomad port
            mask {str} -- glob-like task name pattern

        Keyword Arguments:
            db {str} -- sqlite database name (default: {":memory:"})
            idle_time {Union[int, float]} -- time to idle after finishing a batch
            max_log_length {int} -- tails log output if it's longer than this value
        """

        self.host = host
        self.port = port

        self._session = aiohttp.ClientSession()
        self._base_url = f"http://{host}:{port}/v1"
        self._allocations_url = f"{self._base_url}/allocations"
        self._logs_url = f"{self._base_url}/client/fs/logs"

        self._idle_time = idle_time
        self._max_log_length = max_log_length

        self._db = db

        self.mask = mask

    async def __aenter__(self):
        self.tasks = await TaskList.init(mask=self.mask, db=self._db)
        await self.refresh_sources()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._session.close()

    async def _fetch_tasks(self) -> List[Dict]:
        """Requests allocations and extracts tasks matched with glob-like pattern

        Returns:
            List[Dict] -- a list of tasks

        """

        response = await self._session.get(self._allocations_url)
        allocations = await response.json()

        import aiofiles
        async with aiofiles.open("allocations.json", "w") as aiof:
            await aiof.write(json.dumps(allocations, indent=4))

        tasks = []
        for allocation in allocations:
            for task_name in allocation["TaskStates"].keys():
                # fnmatch allows matching Unix shell-style wildcards: * ? [seq] [!seq]
                if fnmatch.fnmatch(name=task_name, pat=self.mask):
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
                raw_text = raw_text[-i - 1 :]
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
                "alloc_id": alloc_id,
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
            log {dict} -- a dict with text, color, offset, and filename
        """

        if log.get("text"):
            color = log["color"]
            color_stop = "\u001b[0m"
            text = log["text"]
            text_length = len(text)
            filename = log["filename"]
            offset = log["offset"]
            delimiter = "=" * 5

            if text_length > self._max_log_length:
                text = text[-self._max_log_length :]
                text = (
                    f"... Log output truncated because too long ({text_length} chars) ...\n"
                    f"{text}"
                )

            print(
                f"{color}{delimiter} {log['alloc_id']} {delimiter} {filename} @offset {offset} {delimiter}\n"
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

        await asyncio.sleep(self._idle_time)


async def main(host, port, mask, db, idle_time, max_log_length) -> None:
    async with NomadLogger(host, port, mask, db, idle_time, max_log_length) as logger:
        while True:
            await logger.stream_batch()
            await logger.idle()


@click.command()
@click.argument("mask")
@click.option("-h", "--host", required=True, help="Nomad host")
@click.option("-p", "--port", required=True, help="Nomad port")
@click.option(
    "--db", default=":memory:", show_default="in-memory db", help="SQLite db path",
)
@click.option(
    "--idle",
    "--idle-time",
    "idle_time",
    default=1,
    show_default=True,
    type=click.FLOAT,
    help="Seconds to idle after each batch",
)
@click.option(
    "--len",
    "--max-log-length",
    "max_log_length",
    default=10000,
    show_default=True,
    type=click.INT,
    help="Max number of latest characters printed by any task",
)
def runlogger(host, port, mask, db, idle_time, max_log_length):
    """MASK\t\tGlob-like mask to filter tasks
    """

    if not mask or mask == "*":
        prompt = input(
            "\n\t* WARNING *\n\n"
            "It is highly recommended to apply a mask.\n"
            "Streaming all tasks may lead to VERY poor perfomance.\n"
            "Proceed anyway?\tY/N\n"
        )
        if prompt.lower() not in ("y", "yes"):
            sys.exit("Exiting. Please, restart providing a mask")
    asyncio.run(main(host, port, mask, db, idle_time, max_log_length))


if __name__ == "__main__":
    runlogger()
