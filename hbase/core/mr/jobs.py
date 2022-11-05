from abc import ABC
from mrjob.job import MRJob

from ..server import app


class MRTop10TwoGoodsCount(MRJob, ABC):

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self, key, values):
        app.logger.info(f"Sum of values: {sum(values)}")
        yield key, sum(values)
