from mrjob.job import MRJob,MRStep
from mrjob.protocol import TextValueProtocol
class MRVersionCount(MRJob):
    INPUT_PROTOCOL=TextValueProtocol
    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_version,
                   combiner=self.combiner_count_version,
                   reducer=self.reducer_count_version),
            ]
    def mapper_count_version(self, _, line):
        version=line.strip().split('∑')[-1]
        if version not in ['Android Ver']:
            yield version,1

    def combiner_count_version(self, version, counts):
        yield (version, sum(counts))

    def reducer_count_version(self, version, counts):
        yield version, sum(counts)
        pass

if __name__=='__main__':
    MRVersionCount.run()
