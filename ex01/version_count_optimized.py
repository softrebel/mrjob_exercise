from mrjob.job import MRJob,MRStep
from mrjob.protocol import TextValueProtocol
from mrjob import protocol

class MRVersionCount(MRJob):
    INPUT_PROTOCOL=TextValueProtocol
    def configure_args(self):
        super(MRVersionCount, self).configure_args()
        self.add_passthru_arg(
        '--ignore-words', 
        type=str, 
        default='', 
        help='how many lines skipped from the first of input file')

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_skip_lines,
                mapper=self.mapper_count_version,
                combiner=self.combiner_count_version,
                reducer=self.reducer_count_version),
            ]
    def mapper_skip_lines(self):
        self.ignore_words = self\
            .options\
                .ignore_words\
                    .strip()\
                        .split(',')
    def mapper_count_version(self, _, line):
        version=line.strip().split('∑')[-1]
        if version not in self.ignore_words:
            yield version,1

    def combiner_count_version(self, version, counts):
        yield (version, sum(counts))

    def reducer_count_version(self, version, counts):
        yield version, sum(counts)
        pass

if __name__=='__main__':
    MRVersionCount.run()
