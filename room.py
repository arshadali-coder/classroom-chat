import asyncio
import json
import uuid


class RoomServer:

    def __init__(self, room_name, username):

        self.room_name = room_name
        self.username = username

        self.room_id = str(uuid.uuid4())[:8]

        self.server = None
        self.port = None

        self.clients = {}

    async def start(self):

        self.server = await asyncio.start_server(
            self.handle_client,
            "0.0.0.0",
            0
        )

        self.port = self.server.sockets[0].getsockname()[1]

        print(f"\nRoom created!")
        print(f"Room: {self.room_name}")
        print(f"Room ID: {self.room_id}")
        print(f"Port: {self.port}")

        return self

    async def handle_client(self, reader, writer):

        try:

            # First message must contain username
            data = await reader.readline()

            if not data:
                writer.close()
                return

            message = json.loads(data.decode())

            if message.get("type") != "JOIN":
                writer.close()
                return

            username = message.get("username", "Anonymous")

            self.clients[writer] = username

            print(f"\n✓ {username} joined the room")

            await self.broadcast({
                "type": "SYSTEM",
                "message": f"{username} joined the room."
            })

            while True:

                data = await reader.readline()

                if not data:
                    break

                message = json.loads(data.decode())

                if message.get("type") == "MESSAGE":

                    text = message.get("message", "")

                    if text.strip():

                        await self.broadcast({
                            "type": "MESSAGE",
                            "username": username,
                            "message": text
                        })

        except Exception as e:

            print(f"Client error: {e}")

        finally:

            username = self.clients.pop(writer, None)

            if username:

                print(f"\n✗ {username} left the room")

                await self.broadcast({
                    "type": "SYSTEM",
                    "message": f"{username} left the room."
                })

            writer.close()

            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def broadcast(self, message):

        data = (json.dumps(message) + "\n").encode()

        dead_clients = []

        for writer in self.clients:

            try:

                writer.write(data)
                await writer.drain()

            except Exception:

                dead_clients.append(writer)

        for writer in dead_clients:

            self.clients.pop(writer, None)

    async def stop(self):

        if self.server:

            self.server.close()

            await self.server.wait_closed()

    def get_room_info(self, host_ip):

        return {
            "type": "ROOM",
            "room_id": self.room_id,
            "room_name": self.room_name,
            "host": host_ip,
            "port": self.port,
            "users": len(self.clients) + 1
        }


class RoomClient:

    def __init__(self, host, port, username):

        self.host = host
        self.port = port
        self.username = username

        self.reader = None
        self.writer = None

    async def connect(self):

        self.reader, self.writer = await asyncio.open_connection(
            self.host,
            self.port
        )

        join_message = {
            "type": "JOIN",
            "username": self.username
        }

        self.writer.write(
            (json.dumps(join_message) + "\n").encode()
        )

        await self.writer.drain()

        print(f"\n✓ Connected to {self.host}:{self.port}")

    async def receive_messages(self):

        while True:

            try:

                data = await self.reader.readline()

                if not data:
                    print("\nDisconnected from room.")
                    break

                message = json.loads(data.decode())

                message_type = message.get("type")

                if message_type == "SYSTEM":

                    print(
                        f"\n[System] {message['message']}"
                    )

                elif message_type == "MESSAGE":

                    print(
                        f"\n[{message['username']}] "
                        f"{message['message']}"
                    )

                print("> ", end="", flush=True)

            except Exception:

                break

    async def send_message(self, text):

        message = {
            "type": "MESSAGE",
            "message": text
        }

        self.writer.write(
            (json.dumps(message) + "\n").encode()
        )

        await self.writer.drain()

    async def close(self):

        if self.writer:

            self.writer.close()

            try:
                await self.writer.wait_closed()
            except Exception:
                pass