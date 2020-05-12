import asyncio
import selectors

from logs import logs
from session_manager import SessionManager

selector = selectors.SelectSelector()
loop = asyncio.SelectorEventLoop(selector)
asyncio.set_event_loop(loop)

logger = logs.get_logger("main")


async def main():
    session_manager = SessionManager(network="main", host="127.0.0.1", port=8000)
    await session_manager.run()


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
