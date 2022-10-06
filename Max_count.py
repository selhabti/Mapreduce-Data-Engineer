from mrjob.job import MRJob
from mrjob.step import MRStep
# -*- coding: utf-8 -*-
class count_incidents(MRJob):
    
    def mapper(self, _, line):
        try:
            (id;debut_application;fin_application;mise_a_jour;status;cause;severite;code_objet_référentiel_IdFM;type_objet;nom_objet_referentiel_IdFM;titre_du_message;corps_du_message) = line.split(";")
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
