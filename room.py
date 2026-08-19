import asyncio
import base64
import json
import os
import uuid


def format_size(size_bytes):
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"


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

            print(f"\n[+] {username} joined the room")

            await self.broadcast({
                "type": "SYSTEM",
                "message": f"{username} joined the room."
            })

            while True:

                data = await reader.readline()

                if not data:
                    break

                message = json.loads(data.decode())

                message_type = message.get("type")

                if message_type == "MESSAGE":

                    text = message.get("message", "")

                    if text.strip():

                        await self.broadcast({
                            "type": "MESSAGE",
                            "username": username,
                            "message": text
                        })

                elif message_type == "FILE":

                    filename = message.get("filename")
                    filesize = message.get("filesize", 0)
                    content = message.get("content", "")

                    if filename and content:

                        await self.broadcast({
                            "type": "FILE",
                            "username": username,
                            "filename": filename,
                            "filesize": filesize,
                            "content": content
                        })

        except Exception as e:

            print(f"Client error: {e}")

        finally:

            username = self.clients.pop(writer, None)

            if username:

                print(f"\n[-] {username} left the room")

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
            "users": len(self.clients)
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

        print(f"\n[+] Connected to {self.host}:{self.port}")

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

                elif message_type == "FILE":

                    sender = message.get("username", "Unknown")
                    raw_filename = message.get("filename", "file")
                    filesize = message.get("filesize", 0)
                    content = message.get("content", "")

                    safe_filename = os.path.basename(raw_filename)
                    if not safe_filename:
                        safe_filename = "downloaded_file"

                    os.makedirs("downloads", exist_ok=True)

                    dest_path = os.path.join("downloads", safe_filename)
                    base_name, ext = os.path.splitext(safe_filename)
                    counter = 1

                    while os.path.exists(dest_path):
                        dest_path = os.path.join(
                            "downloads", f"{base_name} ({counter}){ext}"
                        )
                        counter += 1

                    file_bytes = base64.b64decode(content)
                    with open(dest_path, "wb") as f:
                        f.write(file_bytes)

                    size_str = format_size(len(file_bytes))
                    print(
                        f"\n[File] {sender} shared '{safe_filename}' "
                        f"({size_str}) -> Saved to {dest_path}"
                    )

                print("> ", end="", flush=True)

            except Exception as e:

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

    async def send_file(self, file_path):

        clean_path = file_path.strip("\"'")

        if not os.path.isfile(clean_path):
            print(f"\n[Error] File not found: {clean_path}")
            return False

        file_size = os.path.getsize(clean_path)
        max_size = 50 * 1024 * 1024  # 50 MB limit

        if file_size > max_size:
            print(f"\n[Error] File exceeds max size limit of 50 MB ({format_size(file_size)})")
            return False

        try:
            with open(clean_path, "rb") as f:
                raw_bytes = f.read()

            encoded_content = base64.b64encode(raw_bytes).decode("ascii")
            filename = os.path.basename(clean_path)

            message = {
                "type": "FILE",
                "filename": filename,
                "filesize": file_size,
                "content": encoded_content
            }

            self.writer.write(
                (json.dumps(message) + "\n").encode()
            )

            await self.writer.drain()

            print(
                f"\n[+] Shared file '{filename}' ({format_size(file_size)})"
            )
            return True

        except Exception as e:
            print(f"\n[Error] Failed to send file: {e}")
            return False

    async def close(self):

        if self.writer:

            self.writer.close()

            try:
                await self.writer.wait_closed()
            except Exception:
                pass