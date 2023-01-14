from mrjob.job import MRJob,MRStep
from mrjob.protocol import TextValueProtocol
from mrjob import protocol

class MRKBestApp(MRJob):
    INPUT_PROTOCOL=TextValueProtocol
    def steps(self):
        return [
            MRStep(mapper=self.preprocess),
            MRStep(mapper=self.mapper,
                reducer=self.reducer),
            MRStep(
                mapper=self.mapper_k_best,
                reducer=self.reducer_k_best
                )
        ]
    def configure_args(self):
        super(MRKBestApp, self).configure_args()
        self.add_passthru_arg(
        '--k-best', 
        type=int, 
        default=1, 
        help='top k best app based on review')

    
    def preprocess(self,_,line):
        items=line.strip().split('∑')
        app=items[0]
        rating=items[2]
        try:
            rating=int(rating)
        except :
            rating=0
        # if rating == 'NaN':
        #     rating = 0
        # else:
        #     rating
        extended_items=items+[rating]
        yield app,extended_items

    def mapper(self, app, extended_items):
        
        category=extended_items[1]
        yield category,extended_items


    def reducer(self, category,items):
        yield category, sorted(items,key=lambda x: x[-1],reverse=True)[:self.options.k_best]
    
    def mapper_k_best(self,category,items):
        for item in items:
            yield item[0],item
    def reducer_k_best(self ,app,items):
        yield app,next(items)
if __name__=='__main__':
    MRKBestApp.run()
