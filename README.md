# Analyst

Analyst is an automated data analyst (to some extent). It provides a facility to create Excel reports driven by arbitrarily complex or long-running SQL queries. If you try and to the same thing in Excel using PowerQuery or a macro, you'll crash Excel.

Specifically, using Analyst you can:

* Create scripted, templated Excel reports out of complex SQL queries, mapping query results to ranges
* Let your users update those reports at the click of a button
* Keep track of all generated/updated reports over time
* Separate reports by user group

## Why not do all this in Excel?

It crashes. 

It requires installing database clients on users' machines and updating credentials when they inevitably change. 

It doesn't automatically track historically generated reports.

Developers and analysts should not have to spend large amounts of time providing IT support or teaching people how to use Excel.

## Installing

Install Go. Clone the repo. 

If need be open up port 8989 on your firewall. Run 

`go get github.com/michaelbironneau/analyst && go install github.com/michaelbironneau/analyst`

This will compile the binary and put it in `$GOPATH/bin`. 

Analyst requires a Postgres database. You can specify the connection details in the configuration file. 

If you don't want to worry about that run the `install.sh` script with root privileges. To run Analyst, 

`analyst -config="file.yml"`

Then visit `http://localhost:8989` in your browser.

If you want Analyst to run as a service you can edit `analyst.conf` with location of config file and binary, copy to `etc/analyst.conf` and run `sudo service analyst start`. 


## Scaling and Reliability

You probably don't need to read this section. If you are deploying this in a small to medium-sized company which can tolerate the odd hour of downtime, stop reading now.

You can run multiple load-balanced instances of the web server against the same Postgres database and stuff should just work, but I haven't tested this so tread carefully. 

Backing up the database is your responsibility.