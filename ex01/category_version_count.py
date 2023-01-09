from mrjob.job import MRJob,MRStep
from mrjob.protocol import TextValueProtocol
from mrjob import protocol

class MRCategoryVersionCount(MRJob):
    INPUT_PROTOCOL=TextValueProtocol
    def steps(self):
        return [
            # MRStep(mapper=self.preprocess),
            MRStep(mapper=self.mapper,
                reducer=self.reducer),
            MRStep(
                mapper=self.mapper_category_version,
                reducer=self.reducer_category_version
                )
        ]

    
    # def preprocess(self,_,line):
    #     items=line.strip().split('∑')
    #     category=items[1]
    #     version=items[-1]
    #     yield (category,version),1

    def mapper(self, app, line):
        items=line.strip().split('∑')
        category=items[1]
        version=items[-1]
        yield (category,version),1


    def reducer(self, key,value):
        yield key, sum(value)
    
    def mapper_category_version(self,key,count):
        yield key[0],(count,key[1])
    def reducer_category_version(self ,category,versions):
        yield category,sorted(versions,reverse=True)
if __name__=='__main__':
    MRCategoryVersionCount.run()
