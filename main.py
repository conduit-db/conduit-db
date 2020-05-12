import asyncio

from logs import logs
from session_manager import SessionManager


logger = logs.get_logger("main")


async def main():
    session_manager = SessionManager(network="main", host="127.0.0.1", port=8000)
    await session_manager.run()


if __name__ == "__main__":
    asyncio.run(main())
