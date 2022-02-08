from vin import VinDecoder
from loguru import logger
import sys
import os
# -----------------------------------------------------------------------------
# Loger initialization
# -----------------------------------------------------------------------------
currentdir = os.path.dirname(os.path.realpath(__file__))
logger.remove()
logger.add(sys.stdout, format="<y><b>{time:YYYY-MM-DD at HH:mm:ss}</b></y> | <level>{level}</level> | <level>{message}</level>",
                        enqueue=True,colorize=True)
logger.add(os.path.join(currentdir,'logs.log'),
           rotation="1 week",
           colorize=True,
           format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",enqueue=True)
# -----------------------------------------------------------------------------

if __name__ == '__main__':
    VinDecoder().process_vin()