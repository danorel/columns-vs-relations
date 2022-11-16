from abc import ABC
from mrjob.job import MRJob

from ..server import app


class MRTop10TwoGoodsCount(MRJob, ABC):
    def mapper(self, _, line):
        yield "words", len(line.split())

    def reducer(self, key, values):
        app.logger.info(f"Sum of values: {len(values)}")
        yield key, len(values)
