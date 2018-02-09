---
id: logic
title: Using Python for execution logic
---

Analyst excels at orchestrating data movement and manipulation in a simple way. However, it is not designed to replace an imperative programming language when it comes to adding logic. For compile-time logic, the templating features can certainly help. However, for run-time logic, this will simply be insufficient.

In the below example, we use a Python script to orchestrate a series of AQL script executions, parsing the output of the script and outputting it to the console.

The objective is to downsample a timeseries. There is no "for each" operator in AQL, so this is handled in Python.

AQL Script (with dummy data)
```
GLOBAL 'CreateTables' (
    CREATE TABLE Timeseries (
        LoadId int not null,
        Variable text not null,
        Time  text not null,
        Value real
    );

    INSERT INTO Timeseries (LoadId, Variable, Time, Value)
     VALUES
     (1, 'power', '2017-12-01T11:59:00Z', 10),
     (1, 'power', '2017-12-01T12:13:01Z', 0),
     (1, 'power', '2017-12-01T12:57:00Z', 1.1),
     (2, 'power', '2017-12-01T11:52:00Z', 120),
     (2, 'power', '2017-12-01T11:45:00Z', 100),
     (3, 'power', '2017-12-01T12:33:00Z', 119),
     (3, 'power', '2017-12-01T12:20:00Z', 50),
     (3, 'power', '2017-12-01T11:59:00Z', 100),
     (1, 'temperature', '2017-12-01T11:59:00Z', 129.5),
     (1, 'temperature', '2017-12-01T12:13:01Z', 130.3);
)

TRANSFORM 'Resample' FROM GLOBAL (
    AGGREGATE LoadId, Variable, ZOH(Time, Value, '{{ .Start }}', '{{ .Finish }}') As Value
    GROUP BY LoadId, Variable
) INTO CONSOLE
    WITH (TABLE = 'Timeseries', OUTPUT_FORMAT = 'JSON')
```

Python script

```
from subprocess import check_output
import json

resampling_times = [("2017-12-01T12:00:00Z", "2017-12-01T12:10:00Z"),
                    ("2017-12-01T12:10:00Z", "2017-12-01T12:20:00Z"),
                    ("2017-12-01T12:20:00Z", "2017-12-01T12:30:00Z")]

if __name__ == "__main__":
    for start, finish in resampling_times:
        opts = json.dumps({"Start": start, "Finish": finish})
        print("RESAMPLING {0} to {1}".format(start, finish))
        output = json.loads(check_output((["./analyst", "run", "--script", "example3.aql", "--params", opts])))
        for row in output:
            print("OUTPUT VALUE for Load {0} is {1}".format(row['LoadId'], row['Value']))
```