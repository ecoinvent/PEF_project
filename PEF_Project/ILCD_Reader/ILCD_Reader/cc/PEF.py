import logging
import time

from pef.next import main

formatter = logging.Formatter(
    '%(asctime)s | %(name)s |  %(levelname)s: %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
file_handler = logging.FileHandler("app.log")
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        stream_handler,
        file_handler
    ]
)

log = logging.getLogger(__name__)

start_time = time.time()
log.info("start pipeline")
main.main()
log.info("completed in %s seconds", round(time.time() - start_time))
