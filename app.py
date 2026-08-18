import asyncio

from discovery import Discovery, get_local_ip
from room import RoomServer, RoomClient


async def chat_interface(client):

    receiver = asyncio.create_task(
        client.receive_messages()
    )

    while True:

        try:

            message = await asyncio.to_thread(
                input,
                "> "
            )

        except (KeyboardInterrupt, EOFError):

            break

        if message.strip() == "/quit":

            break

        if message.strip() == "/help":

            print(
                "\nCommands:"
                "\n  /help  - Show commands"
                "\n  /quit  - Leave room\n"
            )

            continue

        if message.strip():

            await client.send_message(message)

    receiver.cancel()

    await client.close()


async def create_room(username):

    room_name = input("\nRoom name: ").strip()

    if not room_name:
        print("Room name cannot be empty.")
        return

    room = RoomServer(
        room_name,
        username
    )

    await room.start()

    discovery = Discovery()

    await discovery.start()

    local_ip = get_local_ip()

    print(f"\nYour LAN IP: {local_ip}")
    print("\nWaiting for people to join...")
    print("Press CTRL+C to stop the room.\n")

    try:

        while True:

            room_info = room.get_room_info(
                local_ip
            )

            discovery.announce(room_info)

            await asyncio.sleep(2)

    except KeyboardInterrupt:

        print("\nStopping room...")

    finally:

        await discovery.stop()
        await room.stop()


async def join_room(username, room):

    print(
        f"\nConnecting to "
        f"{room['room_name']}..."
    )

    client = RoomClient(
        room["host"],
        room["port"],
        username
    )

    try:

        await client.connect()

        await chat_interface(client)

    except ConnectionRefusedError:

        print(
            "\nCould not connect to the room."
        )

    except Exception as e:

        print(
            f"\nConnection error: {e}"
        )


async def main():

    print(
        """
╔══════════════════════════════════════╗
║        CLASSROOM CHAT v0.1           ║
║                                      ║
║     Local • Fast • Offline           ║
╚══════════════════════════════════════╝
"""
    )

    username = input("Username: ").strip()

    if not username:

        print("Username cannot be empty.")
        return

    discovery = Discovery()

    await discovery.start()

    print(
        "\nSearching for rooms on your network..."
    )

    await asyncio.sleep(2)

    rooms = list(discovery.rooms.values())

    print("\nRooms found:\n")

    if not rooms:

        print("  No rooms found.\n")

    else:

        for index, room in enumerate(rooms, 1):

            print(
                f"  [{index}] "
                f"{room['room_name']} "
                f"({room['users']} users)"
            )

    print(
        """
  [C] Create a room
  [R] Refresh
  [Q] Quit
"""
    )

    choice = input("> ").strip().lower()

    await discovery.stop()

    if choice == "c":

        await create_room(username)

    elif choice == "q":

        print("Goodbye!")

    elif choice.isdigit():

        index = int(choice) - 1

        if 0 <= index < len(rooms):

            await join_room(
                username,
                rooms[index]
            )

        else:

            print("Invalid room.")

    elif choice == "r":

        await main()

    else:

        print("Invalid choice.")


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print("\nGoodbye!")