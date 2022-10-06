from mrjob.job import MRJob

from mrjob.step import MRStep


class Types_count(MRJob):

    def steps(self):

        return [

            MRStep(mapper=self.mapper_get_types,

            reducer=self.reducer_count_types)

        ]


    def mapper_get_types(self, _, line):

        (pokemonId, pokemonName, pokemonType1, pokemonType2, HP, Attack, Defense, SpAttack, SpDef, Speed, Generation, Lengendary) = line.split(',')

        yield pokemonType1, 1


    def reducer_count_types(self, key, values):

        yield key, sum(values)


if __name__ == '__main__':

    Types_count.run()