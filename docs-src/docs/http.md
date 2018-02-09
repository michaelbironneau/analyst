---
id: http
title: Getting data from Web APIs
---

In this easy recipe, we're going to get a list of awardees from Chronicling America and figure out how many awardees are University-based academics.

Looking at the data in a web browser, you will see the below structure, truncated for brevity:

```
{
  "awardees": [
    {
      "url": "http://chroniclingamerica.loc.gov/awardees/ak.json", 
      "name": "Alaska State Library Historical Collections"
    }, 
    {
      "url": "http://chroniclingamerica.loc.gov/awardees/az.json", 
      "name": "Arizona State Library, Archives and Public Records; Phoenix, AZ"
    }, 
    {
      "url": "http://chroniclingamerica.loc.gov/awardees/mimtptc.json", 
      "name": "Central Michigan University, Clark Historical Library"
    }]
}
```

The array of awardees is contained in the JSON-path `awardees`, and the column names will be `URL` and `Name`.

## Printing the Awardees on the console

```
	CONNECTION 'WebAPI' (
		DRIVER = 'http',
		URL = 'https://chroniclingamerica.loc.gov/awardees.json',
		JSON_PATH = 'awardees',
		COLUMNS = 'URL, Name'
	)

	QUERY 'Aggregate' FROM CONNECTION WebAPI (
		--Select how many awardees are universities
		SELECT 'The Magic Answer Is', COUNT(*) As NumberOfUniversityAwardees FROM WebAPI
		WHERE Name LIKE '%university%'
	) INTO CONSOLE
```

In the above, we are making use of the Auto-SQL source. This will fetch all the rows from the HTTP API and insert them into a temporary staging table, in-memory, allowing us to run any SQLite3-compatible query.