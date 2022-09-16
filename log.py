import sys
import logging

logger = logging.getLogger(__name__)
# INFO以下のログを標準出力する
stdout_handler = logging.StreamHandler(stream=sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)


# ロガーにハンドラを設定する
logger.setLevel(logging.DEBUG)
logger.addHandler(stdout_handler)
