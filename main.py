import asyncio
import logging

from constants import LOGGING_FORMAT
from session_manager import SessionManager

logging.basicConfig(format=LOGGING_FORMAT, level=logging.DEBUG, datefmt="%Y-%m-%d %H-%M-%S")
logger = logging.getLogger("main")

async def main():
    session_manager = SessionManager(network='main', host='127.0.0.1', port=8000)
    await session_manager.run()

if __name__ == "__main__":
    asyncio.run(main())

