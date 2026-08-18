import asyncio
import json
import socket
import sys


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


def get_broadcast_destinations():
    destinations = {"255.255.255.255"}
    local_ip = get_local_ip()
    if local_ip and local_ip != "127.0.0.1":
        parts = local_ip.split(".")
        if len(parts) == 4:
            # Guess Class C broadcast address for the local subnet
            destinations.add(".".join(parts[:3] + ["255"]))
    return list(destinations)


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
            reuse_port=sys.platform != "win32"
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

        for dest in get_broadcast_destinations():
            try:
                self.transport.sendto(
                    message,
                    (dest, DISCOVERY_PORT)
                )
            except Exception:
                pass

    async def stop(self):

        if self.transport:
            self.transport.close()