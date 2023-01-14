from mrjob.job import MRJob,MRStep
from mrjob.protocol import TextValueProtocol
from mrjob import protocol

class MRReviewWordOccurrence(MRJob):
    INPUT_PROTOCOL=TextValueProtocol
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                reducer=self.reducer),
            MRStep(
                mapper=self.mapper_app_word,
                reducer=self.reducer_app_word
                ),
            MRStep(
                reducer=self.reducer_second_sort
                ),
            MRStep(
                mapper=self.mapper_second_sort
            ),
        ]

    def mapper(self, _, line):
        items=line.strip().split('∑')
        app=items[0]
        review=items[1]
        if review.lower() != 'nan':
            tokens=review.split()
            for i in range(len(tokens)-2):
                yield (app,tokens[i],
                        tokens[i+1]),1

    def reducer(self, key,value):
        yield key, sum(value)  

    def mapper_app_word(self,key,count):
        yield key[0],(count,key[1],key[2])
    def reducer_app_word(self ,app,word_count):
        yield None,(app,max(word_count,key=lambda x:x[0]))
    
    
    def reducer_second_sort(self,_,items):
        yield None,sorted(items,key=lambda x:x[1][0],reverse=True)
        
    
    def mapper_second_sort(self,_,items):
        for item in items:
            yield item[0],item[1]


if __name__=='__main__':
    MRReviewWordOccurrence.run()
