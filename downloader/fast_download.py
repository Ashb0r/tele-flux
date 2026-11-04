import math
import asyncio
import logging
from typing import Optional, AsyncGenerator
from telethon import TelegramClient, utils
from telethon.network import MTProtoSender
from telethon.tl import functions, types
from telethon.tl.alltlobjects import LAYER

logger = logging.getLogger("FastTelethon")

class DownloadSender:
    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file: types.TypeInputFileLocation,
        offset: int,
        limit: int,
        stride: int,
        count: int
    ):
        self.client = client
        self.sender = sender
        self.request = functions.upload.GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count

    async def next(self) -> Optional[bytes]:
        if self.remaining <= 0:
            return None
        result = await self.client._call(self.sender, self.request)
        self.remaining -= 1
        self.request.offset += self.stride
        return result.bytes

    async def disconnect(self):
        if self.sender:
            try:
                await self.sender.disconnect()
            except Exception:
                pass


class ParallelTransferrer:
    def __init__(self, client: TelegramClient, dc_id: int):
        self.client = client
        self.dc_id = dc_id
        self.auth_key = None
        self.senders: list[DownloadSender] = []

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)

        # If accessing the same DC as the primary client session, reuse auth_key
        if not self.auth_key and getattr(self.client.session, 'dc_id', None) == self.dc_id:
            self.auth_key = getattr(self.client.session, 'auth_key', None)

        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        local_addr = getattr(self.client, '_local_addr', None)
        conn_kwargs = {
            "loggers": self.client._log,
            "proxy": self.client._proxy
        }
        if local_addr:
            conn_kwargs["local_addr"] = local_addr

        await sender.connect(
            self.client._connection(
                dc.ip_address,
                dc.port,
                dc.id,
                **conn_kwargs
            )
        )
        if not self.auth_key:
            auth = await self.client(functions.auth.ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = functions.auth.ImportAuthorizationRequest(
                id=auth.id, bytes=auth.bytes
            )
            req = functions.InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    async def _create_download_sender(
        self,
        file: types.TypeInputFileLocation,
        index: int,
        part_size: int,
        stride: int,
        part_count: int
    ) -> DownloadSender:
        sender = await self._create_sender()
        return DownloadSender(
            client=self.client,
            sender=sender,
            file=file,
            offset=index * part_size,
            limit=part_size,
            stride=stride,
            count=part_count
        )

    async def _init_download(
        self,
        connections: int,
        file: types.TypeInputFileLocation,
        part_count: int,
        part_size: int,
        start_part: int = 0
    ):
        minimum, remainder = divmod(part_count, connections)

        def get_part_count():
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        stride = connections * part_size

        # Create first sender (ensures auth_key is negotiated for this DC)
        first_count = get_part_count()
        first_sender = await self._create_download_sender(
            file, start_part, part_size, stride, first_count
        )
        self.senders = [first_sender]

        # Concurrently create remaining senders using established auth_key
        if connections > 1:
            other_senders = await asyncio.gather(*[
                self._create_download_sender(
                    file, start_part + i, part_size, stride, get_part_count()
                )
                for i in range(1, connections)
            ])
            self.senders.extend(other_senders)

    async def download(
        self,
        file: types.TypeInputFileLocation,
        file_size: int,
        start_offset: int = 0,
        part_size: int = 512 * 1024,
        connections: int = 6
    ) -> AsyncGenerator[bytes, None]:
        bytes_to_download = file_size - start_offset
        if bytes_to_download <= 0:
            return

        input_location = utils.get_input_location(file)
        part_count = math.ceil(bytes_to_download / part_size)
        start_part = start_offset // part_size
        connections = min(connections, part_count)
        connections = max(1, connections)

        await self._init_download(connections, input_location, part_count, part_size, start_part)

        part = 0
        try:
            while part < part_count:
                # Concurrently execute chunk download across only active senders!
                active_senders = [s for s in self.senders if s.remaining > 0]
                if not active_senders:
                    break

                tasks = [asyncio.create_task(s.next()) for s in active_senders]
                try:
                    for task in tasks:
                        chunk = await task
                        if not chunk:
                            break
                        yield chunk
                        part += 1
                finally:
                    for t in tasks:
                        if not t.done():
                            t.cancel()
        finally:
            await self._cleanup()

    async def _cleanup(self):
        if self.senders:
            await asyncio.gather(*[s.disconnect() for s in self.senders], return_exceptions=True)
            self.senders = []
