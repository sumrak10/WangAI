import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path


class LoggerConfig:
    LOG_DIR = Path('./logs')  # Относительный путь для удобства
    LOG_FILE = 'application.log'
    LOG_LEVEL = int(os.getenv('LOGGING_LEVEL', logging.INFO))
    MAX_LOG_SIZE = 5 * 1024 * 1024  # 5 MB
    BACKUP_COUNT = 5

    @classmethod
    def setup_logger(cls):
        """Настраивает глобальный логгер один раз."""
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_path = cls.LOG_DIR / cls.LOG_FILE

        logger_ = logging.getLogger()
        logger_.setLevel(cls.LOG_LEVEL)

        # Проверяем, что обработчики не добавлены ранее
        # if not any(isinstance(h, RotatingFileHandler) for h in logger_.handlers):
        #     file_handler = RotatingFileHandler(
        #         log_path,
        #         maxBytes=cls.MAX_LOG_SIZE,
        #         backupCount=cls.BACKUP_COUNT,
        #     )
        #     file_handler.setLevel(cls.LOG_LEVEL)
        #     formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        #     file_handler.setFormatter(formatter)
        #     logger_.addHandler(file_handler)

        if not any(isinstance(h, logging.StreamHandler) for h in logger_.handlers):
            console_handler = logging.StreamHandler()
            console_handler.setLevel(cls.LOG_LEVEL)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            logger_.addHandler(console_handler)

        logger_.info('Logger configured')
