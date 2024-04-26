from mrjob.job import MRJob

class MedalTally(MRJob):

    def mapper(self, _, line):
        parts = line.strip().split('\t')
        if len(parts) == 2:
            event_info, numid = parts
            event_info = event_info.strip("[]\"")
            _, _, medal_type = event_info.split(',')
            medal_type = medal_type.strip('" ')
            numid = numid.strip() 
            yield numid, medal_type

    def reducer(self, key, values):
        gold = silver = bronze = 0
        for value in values:
            if value == 'Gold':
                gold += 1
            elif value == 'Silver':
                silver += 1
            elif value == 'Bronze':
                bronze += 1
        yield key, {'Gold': gold, 'Silver': silver, 'Bronze': bronze, 'Total': 3*gold + 2*silver + bronze}
if __name__ == '__main__':
    MedalTally.run()
