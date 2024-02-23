"""
Find the number of hot days in Austin for 2017
"""
from mrjob.job import MRJob

class Hot_Days(MRJob):
    # Add your mapper function here.
    def mapper(self, key, line):
        (station, name, state, date, snow, tmax, tmin) = line.split(",")
        if tmax and int(tmax) > 90:
            yield date, 1


    # Add your reducer function here.
    def reducer(self, date, hot_day):
        yield date, sum(hot_day)


if __name__ == "__main__":
    Hot_Days.run()