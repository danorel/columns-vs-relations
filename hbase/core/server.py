import logging

from flask import Flask

logging.basicConfig(filename='../history.log', level=logging.DEBUG)
app = Flask(__name__)
