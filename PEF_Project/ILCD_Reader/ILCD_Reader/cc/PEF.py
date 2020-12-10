import logging
import time
import pef.config.transition_phase as cfg
import pef.config.next as next
import pef.runner as pr

formatter = logging.Formatter(
    '%(asctime)s | %(name)s |  %(levelname)s: %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
file_handler = logging.FileHandler("app.log")
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
        stream_handler,
        file_handler
    ]
)

log = logging.getLogger(__name__)

start_time = time.time()
log.info("start pipeline")
res = pr.run_pef_steps(cfg.steps, cfg.starts, cfg.ends, cfg.load_init_data)
#res = pr.run_pef_steps(next.steps, next.starts, next.ends, next.load_init_data)
log.info("completed in %s seconds", round(time.time() - start_time))
