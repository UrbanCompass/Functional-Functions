import logging

logger = logging.getLogger("basic")
if not logger.hasHandlers():
	logger.setLevel(logging.DEBUG)

	consoleHandler = logging.StreamHandler()
	consoleHandler.setLevel(logging.DEBUG)

	formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
	consoleHandler.setFormatter(formatter)

	logger.addHandler(consoleHandler)
