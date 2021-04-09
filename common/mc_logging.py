import logging

from logging import FileHandler
from logging import Formatter
import logging.handlers

# "%(asctime)s [%(levelname)s]: %(message)s in %(pathname)s:%(lineno)d")
LOG_FORMAT = (
    "%(asctime)s %(name)s:%(lineno)d [%(levelname)s]: %(message)s")

LOG_LEVEL = logging.INFO

# messaging logger
LOG_FILENAME = "./clickhouse_migrate.log"


mc_logger = logging.getLogger("clickhouse_migrate")
mc_logger.setLevel(LOG_LEVEL)
# Use rotatingFileHandler
mc_logger_file_handler = logging.handlers.RotatingFileHandler(
    LOG_FILENAME, maxBytes=200000000, backupCount=5)

#mc_logger_file_handler = FileHandler(MESSAGING_LOG_FILE)
mc_logger_file_handler.setLevel(LOG_LEVEL)
mc_logger_file_handler.setFormatter(Formatter(LOG_FORMAT))
mc_logger.addHandler(mc_logger_file_handler)
