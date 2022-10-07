from mrjob.job import MRJob
from mrjob.step import MRStep

class count_incidents(MRJob):
    
    def mapper(self, _, line):
        try:
            (id,satrt,end,update,status,cause,Severity,code_objet_IdFM,type_objet,station_Name_IdFM,titre_du_message,corps_du_message) = line.split(";")
            yield (cause, titre_du_message), 1
        except:
            pass

    def reducer(self, key, values):
        yield None, (sum(values), key)

    def reduce_sort(self, _, word_counts):
        for count, key in sorted(word_counts, reverse=True):
            yield (int(count), key)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reduce_sort)
        ]

if __name__ == '__main__':
    count_incidents.run()
