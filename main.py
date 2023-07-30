import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import signal
import sys
import requests
from requests.auth import HTTPBasicAuth

DRY_RUN = False
GENERATE_COMMAND = False
MY_GROUP = "thomas"


@dataclass
class Transfer:
    error: str
    name: str
    size: int
    bytes: int
    checked: bool
    started_at: datetime
    completed_at: datetime
    group: str

    @classmethod
    def datetime_from_string(cls, s: str) -> datetime:
        # Split the date string to separate the fractional seconds part
        date_parts = s.split(".")
        date_without_microseconds = date_parts[0]
        fractional_seconds_part = int(date_parts[1].split("+")[0])

        # Convert fractional seconds to microseconds (6 digits)
        microseconds = fractional_seconds_part // 1000

        # Parse the date without microseconds
        parsed_date = datetime.strptime(date_without_microseconds, "%Y-%m-%dT%H:%M:%S")

        # Apply the timezone offset
        timezone_offset = s[-6:]
        parsed_date -= timedelta(
            hours=int(timezone_offset[1:3]), minutes=int(timezone_offset[4:6])
        )

        # Add microseconds to the parsed date
        parsed_date = parsed_date.replace(microsecond=microseconds)

        return parsed_date

    @classmethod
    def from_dict(cls, d):
        return Transfer(
            error=d["error"],
            name=d["name"],
            size=d["size"],
            bytes=d["bytes"],
            checked=d["checked"],
            started_at=cls.datetime_from_string(d["started_at"]),
            completed_at=cls.datetime_from_string(d["completed_at"]),
            group=d["group"],
        )


@dataclass
class Statistics:
    @dataclass
    class Transferring:
        bytes: int
        eta: float | None
        group: str
        name: str
        percentage: float
        size: int
        speed: float
        speedAvg: float

        @classmethod
        def from_dict(cls, d):
            try:
                return cls(
                    name=d["name"],
                    size=d["size"],
                    bytes=d.get("bytes", None),
                    eta=d.get("eta", None),
                    group=d.get("group", None),
                    percentage=d.get("percentage", None),
                    speed=d.get("speed", None),
                    speedAvg=d.get("speedAvg", None),
                )
            except:
                print("Couldn't decode!")
                with open("invalid_transferring.json", "w") as f:
                    json.dump(d, f, indent=2)

                return None

    bytes: int
    checks: int
    deletedDirs: int
    deletes: int
    elapsedTime: float
    errors: int
    eta: int
    fatalError: bool
    renames: int
    retryError: bool
    speed: float
    totalBytes: int
    totalChecks: int
    totalTransfers: int
    transferTime: float
    transferring: list[Transferring]
    transfers: int

    @classmethod
    def from_dict(cls, d):
        return cls(
            bytes=d["bytes"],
            checks=d["checks"],
            deletedDirs=d["deletedDirs"],
            deletes=d["deletes"],
            elapsedTime=d["elapsedTime"],
            errors=d["errors"],
            eta=d["eta"],
            fatalError=d["fatalError"],
            renames=d["renames"],
            retryError=d["retryError"],
            speed=d["speed"],
            totalBytes=d["totalBytes"],
            totalChecks=d["totalChecks"],
            totalTransfers=d["totalTransfers"],
            transferTime=d["transferTime"],
            transferring=[
                cls.Transferring.from_dict(x) for x in d.get("transferring", list())
            ],
            transfers=d["transfers"],
        )


class RCloneClient:
    def __init__(self):
        self.server = "localhost"
        self.port = "5572"
        self.username = "thomas"
        self.password = "kowalski"

    @property
    def url(self):
        return f"http://{self.server}:{self.port}"

    @property
    def auth(self):
        if self.username is not None:
            return HTTPBasicAuth(self.username, self.password)

        return None

    async def start_sync(self, source, dest, trackRenames, exclude):
        url = f"{self.url}/sync/sync"
        body = {
            "srcFs": source,
            "dstFs": dest,
            "_async": True,
            "_group": MY_GROUP,
            "_config": {},
            "_filter": {},
        }
        if trackRenames is not None:
            body["_config"]["TrackRenames"] = trackRenames

        if exclude:
            body["_filter"]["ExcludeRule"] = exclude

        request = requests.post(url, json=body, auth=self.auth)
        result = request.json()
        return result

    async def get_job_status(self, job_id: int):
        url = f"{self.url}/job/status"
        body = {"jobid": job_id}

        request = requests.post(url, json=body, auth=self.auth)
        result = request.json()
        return result

    async def get_transferred(self) -> list[Transfer]:
        url = f"{self.url}/core/transferred"
        request = requests.post(url, auth=self.auth)
        result = request.json()
        transfers = [Transfer.from_dict(x) for x in result["transferred"]]
        return transfers

    async def get_stats(self, job_id: int) -> Statistics:
        url = f"{self.url}/core/stats"
        body = {
            "group": MY_GROUP,
        }
        request = requests.post(url, json=body, auth=self.auth)
        result = request.json()
        return Statistics.from_dict(result)

    async def sync(
        self, source: str, dest: str, trackRenames=None, exclude=None, timeout=None
    ):
        print(f"Syncing from {source} to {dest}, trackRenames={trackRenames}")
        if DRY_RUN:
            return

        assert timeout is None, "timeout isn't supported at the moment"

        start_result = await self.start_sync(source, dest, trackRenames, exclude)
        job_id = start_result["jobid"]

        all_transferred: dict[str, Transfer] = dict()
        while True:
            status = await self.get_job_status(job_id)
            finished = status["finished"]
            if finished:
                return status

            data = await self.get_transferred()
            for transferred in data:
                if transferred.name in all_transferred:
                    continue

                all_transferred[transferred.name] = transferred

            stats = await self.get_stats(job_id)

            print(
                f"\r{stats.transfers} / {stats.totalTransfers} ({stats.bytes / 1e6:.0f} / {stats.totalBytes / 1e6:.0f}) (speed: {stats.speed / 1E6:.2f}MB/s)",
                end="",
            )

            # print(".", end="", flush=True)
            # sys.stdout.flush()
            await asyncio.sleep(0.1)

    def stop_all_from_group(self):
        url = f"{self.url}/job/stopgroup"
        body = {"group": MY_GROUP}
        request = requests.post(url, json=body, auth=self.auth)
        return request.json()

    def handler(self, *args, **kwargs):
        stop_result = self.stop_all_from_group()
        assert stop_result == {}
        sys.exit(0)


@dataclass
class Source:
    Device: str
    SourceDirectory: str
    DestinationDirectory: str
    Exclude: list[str]
    Disabled: bool
    TrackRenames: bool


@dataclass
class Configuration:
    Devices: list[str]
    Sources: list[Source]


def load_configuration() -> Configuration:
    with open("config.json") as f:
        data = json.load(f)

    sources: list[Source] = list()
    for source_json in data["Sources"]:
        sources.append(
            Source(
                Device=source_json["Device"],
                SourceDirectory=source_json["SourceDirectory"],
                DestinationDirectory=source_json["DestinationDirectory"],
                Exclude=source_json.get("Exclude", list()),
                Disabled=source_json.get("Disabled", False),
                TrackRenames=source_json.get("TrackRenames", False),
            )
        )

    config = Configuration(
        Devices=data["Devices"],
        Sources=sources,
    )
    return config


async def main():
    config = load_configuration()
    client = RCloneClient()

    this_device = "DDLaptop"
    assert this_device in config.Devices, "Current device is not in configuration"

    signal.signal(signal.SIGINT, client.handler)

    for source in config.Sources:
        if source.Device != this_device:
            continue
        elif source.Disabled:
            print(f"Skipping disabled source {source.SourceDirectory}")
            continue

        if GENERATE_COMMAND:
            command = f"rclone sync --stats=0.1s --progress"
            exclusions = " ".join(f'--exclude "{ex}"' for ex in source.Exclude)
            command += f' {source.SourceDirectory}" "{source.DestinationDirectory}" {exclusions}'
            command += f" --dry-run"
            if source.TrackRenames:
                command += f" --track-renames"
            print(command)
        else:
            print(f"Syncing from {source.SourceDirectory}")
            await client.sync(
                source.SourceDirectory,
                source.DestinationDirectory,
                trackRenames=source.TrackRenames,
                exclude=source.Exclude,
            )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
