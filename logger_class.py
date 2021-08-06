import logging
import os

def getLog(nm):
    # Creating custom logger
    logger = logging.getLogger(nm)
    # reading contents from properties file
    f = open("properties.txt", 'r')
    if f.mode == "r":
        loglevel = f.read()
    if loglevel == "ERROR":
        logger.setLevel(logging.ERROR)
    elif loglevel == "DEBUG":
        logger.setLevel(logging.DEBUG)
    # Creating Formatters
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
    # Creating Handlers
    file_handler = logging.FileHandler('test.log')
    # Adding Formatters to Handlers
    file_handler.setFormatter(formatter)
    # Adding Handlers to logger
    logger.addHandler(file_handler)
    return logger


class Logger:
    def __init__(self, filename):
        self.filename = filename

    def logger(self, logtype, error1):
        if self.filename not in os.listdir():
            with open(os.path.join(os.getcwd(), self.filename), 'a+') as f:
                print(f.read())

        logging.basicConfig(filename=os.path.join(os.getcwd(), self.filename), level=logging.INFO, format='%(name)s - %(asctime)s - %(message)s')
        if logtype == 'INFO':
            logging.info(error1)
        elif logtype == 'ERROR':
            logging.error(error1)
        logging.shutdown()
