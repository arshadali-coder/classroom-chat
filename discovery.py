import asyncio
import json
import socket


DISCOVERY_PORT = 37020
ANNOUNCE_INTERVAL = 2


def get_local_ip():
    """
    Try to determine the computer's LAN IP address.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("10.255.255.255", 1))
        ip = sock.getsockname()[0]
        sock.close()
        return ip
    except Exception:
        return "127.0.0.1"


class DiscoveryProtocol(asyncio.DatagramProtocol):

    def __init__(self, callback):
        self.callback = callback
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            message = json.loads(data.decode())

            if message.get("type") == "ROOM":
                message["sender_ip"] = addr[0]
                self.callback(message)

        except Exception:
            pass


class Discovery:

    def __init__(self):
        self.transport = None
        self.rooms = {}

    async def start(self):

        loop = asyncio.get_running_loop()

        transport, _ = await loop.create_datagram_endpoint(
            lambda: DiscoveryProtocol(self.room_received),
            local_addr=("0.0.0.0", DISCOVERY_PORT),
            allow_broadcast=True,
            reuse_port=True
        )

        self.transport = transport

    def room_received(self, room):

        room_id = room.get("room_id")

        if room_id:
            self.rooms[room_id] = room

    def announce(self, room):

        if not self.transport:
            return

        message = json.dumps(room).encode()

        self.transport.sendto(
            message,
            ("255.255.255.255", DISCOVERY_PORT)
        )

    async def stop(self):

        if self.transport:
            self.transport.close()