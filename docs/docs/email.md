---
id: email
title: Data-Driven Email
---

In this example we're going to fetch data from a database corresponding to an alert condition and send a templated email if any rows are returned.

We have a connection file 'connections.aql' that looks as follows:

```
CONNECTION 'OpsDb' (
	DRIVER = 'mssql',
	CONNECTIONSTRING = 'server=localhost\\SQLExpress;user id=sa;database=Ops;connection timeout=30'
)

CONNECTION 'OpsEmails' (
	DRIVER = 'MANDRILL',
	API_KEY = 'XXXXXXXXXXXXXXXXXXXXX',
	RECIPIENTS = 'Team Lead <test@test.com>, Ops Guy <test2@test2.com>',
	SUBJECT = 'High CPU',
	TEMPLATE = 'alert-email',
	SPLIT = 'True'
)

```

This includes both the database connection where the metrics are recorded, and the Mandrill connection to send the alerts by email.


## Basic Example
The below contains the logic to determine whether there is an alert condition. This is very primitive. The main downside is that the alert email could be triggered every time the job is run.

```
INCLUDE 'connections.aql';

--An anomaly is where CPU has been over 90% for 5 minutes at any point in the last 15 minutes
QUERY 'GetLatestAnomaly' FROM CONNECTION OpsDb (
	SELECT TOP 1 
		'CPU Usage has been >90% in the past 5 minutes' AS Name, 
		Time 
	 FROM (
		SELECT , CONVERT(SMALLDATETIME, ROUND(CAST(RecordedAt) AS float) * (24*12.0),0,1)/(24*12.0)) As Time, AVG(CPUValue) As Value FROM Metrics
		WHERE RecordedAt >= DATEADD(MINUTE, -15, GETDATE())
		GROUP BY 
			CONVERT(SMALLDATETIME, ROUND(CAST(RecordedAt) AS float) * (24*12.0),0,1)/(24*12.0))
	) a	
	WHERE a.Value > 0.9
) INTO CONNECTION OpsEmails
```

## Example that avoids re-triggers within 1h

In the below example, we add an `EXEC` block to record that the alert has been send and avoid overwhelming the team with emails.

This assumes that an additional table `Alert` has been created in `OpsDb`.

```
INCLUDE 'connections.aql';

DECLARE @LatestTime;
DECLARE @LatestName;

--An anomaly is where CPU has been over 90% for 5 minutes at any point in the last 15 minutes
QUERY 'GetLatestAnomaly' FROM CONNECTION OpsDb (
	SELECT TOP 1 'CPU Usage has been >90% in the past 5 minutes' AS Name, Time FROM (
		SELECT 
			CONVERT(SMALLDATETIME, ROUND(CAST(RecordedAt) AS float) * (24*12.0),0,1)/(24*12.0)) As Time, 
			AVG(CPUValue) As Value 
		FROM Metrics
		WHERE RecordedAt >= DATEADD(MINUTE, -15, GETDATE())
		GROUP BY 
			CONVERT(SMALLDATETIME, ROUND(CAST(RecordedAt) AS float) * (24*12.0),0,1)/(24*12.0))
	) a	
	WHERE a.Value > 0.9 AND NOT EXISTS (
		SELECT TOP 1 Id FROM Alert al
		WHERE al.Name = a.Name AND al.Time > DATEADD(HOUR, -1, GETDATE()) 
	)
) INTO CONNECTION OpsEmails, PARAMETER (@LatestName, @LatestTime)

EXEC 'RecordAlert' FROM CONNECTION OpsDb (
	INSERT INTO Alert (Name, Time) VALUES (?,?)	
) USING PARAMETER @LatestName, @LatestTime
  AFTER GetLatestAnomaly
```

## Configurable example

This example makes the 90% CPU value and 1h re-trigger configurable via options `AlertThreshold` and `RetriggerInterval`. These could be set via the CLI such as invoking the script like
```
analyst run --params="{\"AlertThreshold\": 0.9, \"RetriggerInterval\": 60}
```

The connections file is the same as above.


```
INCLUDE 'connections.aql';

DECLARE @LatestTime;
DECLARE @LatestName;

--An anomaly is where CPU has been over 90% for 5 minutes at any point in the last 15 minutes
QUERY 'GetLatestAnomaly' FROM CONNECTION OpsDb (
	SELECT TOP 1 'CPU Usage has been >90% in the past 5 minutes' AS Name, Time FROM (
		SELECT 
			CONVERT(SMALLDATETIME, ROUND(CAST(RecordedAt) AS float) * (24*12.0),0,1)/(24*12.0)) As Time, 
			AVG(CPUValue) As Value 
		FROM Metrics
		WHERE RecordedAt >= DATEADD(MINUTE, -15, GETDATE())
		GROUP BY 
			CONVERT(SMALLDATETIME, ROUND(CAST(RecordedAt) AS float) * (24*12.0),0,1)/(24*12.0))
	) a	
	WHERE a.Value > {{ .AlertThreshold }} AND NOT EXISTS (
		SELECT TOP 1 Id FROM Alert al
		WHERE al.Name = a.Name AND al.Time > DATEADD(MINUTE, -1*{{ .RetriggerInterval }}, GETDATE()) 
	)
) INTO CONNECTION OpsEmails, PARAMETER (@LatestName, @LatestTime)

EXEC 'RecordAlert' FROM CONNECTION OpsDb (
	INSERT INTO Alert (Name, Time) VALUES (?,?)	
) USING PARAMETER @LatestName, @LatestTime
  AFTER GetLatestAnomaly
```