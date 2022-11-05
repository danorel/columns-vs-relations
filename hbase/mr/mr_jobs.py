from abc import ABC

from mrjob.job import MRJob


class MRTop10TwoGoodsCount(MRJob, ABC):

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRTop10TwoGoodsCount.run()
