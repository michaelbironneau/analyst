from subprocess import call
import json

resampling_times = [("2017-12-01T12:00:00Z", "2017-12-01T12:10:00Z"),
                    ("2017-12-01T12:10:00Z", "2017-12-01T12:20:00Z"),
                    ("2017-12-01T12:20:00Z", "2017-12-01T12:30:00Z")]

if __name__ == "__main__":
    for start, finish in resampling_times:
        opts = json.dumps({"Start": start, "Finish": finish})
        print("RESAMPLING {0} to {1}".format(start, finish))
        call(["./analyst", "run", "--script", "example3.aql", "--params", opts])