import logging

_log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"


def get_file_handler():
    """
        Create log info
        
        Returns:
            file ------>files of different programs
    """
    file_handler = logging.FileHandler("hive_data_handling.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(_log_format))
    return file_handler


def get_logger(name):
    """
        Integrate log info
        
        Returns:
            file ------>integrate all files to one
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(get_file_handler())
    
    return logger
