# mr_sport_medals.py
# a file to run mapreduce job
# returns the gold silver and bronze of every game in each simulated round
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRSportMedals(MRJob):

    def mapper(self, _, line):
        line_cols = line.split(' ')
        game = line_cols[5].strip('"')
        numid = line_cols[6].strip('"')
        athlete = int(numid)
        rs_q = float(line_cols[7])
        rs_f = float(line_cols[8])
        label = int(line_cols[9])
        yield (game, label), (athlete, rs_q, rs_f)

    def reducer_init(self):
        self.current_game_label = None
        self.athletes_scores = []

    def reducer(self, key, values):
        if self.current_game_label != key:
            if self.current_game_label:
                yield from self.rank_athletes()
            self.current_game_label = key
            self.athletes_scores = []
        self.athletes_scores.extend(values)

    def reducer_final(self):
        if self.current_game_label:
            yield from self.rank_athletes()
            
    def rank_athletes(self):
        top_athletes = sorted(self.athletes_scores, key=lambda x: x[2], reverse=True)[:3]
        medals = ['Gold', 'Silver', 'Bronze']
        game, label = self.current_game_label
        results = []
        for i, athlete_info in enumerate(top_athletes):
            results.append(((game, label, medals[i]), athlete_info[0]))
        return results

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer,
                   reducer_final=self.reducer_final) 
        ]

if __name__ == '__main__':
    MRSportMedals.run()
