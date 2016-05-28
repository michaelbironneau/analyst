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

For small and medium-sized organisations I can't imagine why one instance of Analyst wouldn't be enough, and I also can't imagine you wouldn't be able to tolerate the occasional hour of downtime if something goes wrong. 

If that doesn't sound like your company, you can run multiple load-balanced instances of the web server against the same Postgres database and stuff should just work, but I haven't tested this so tread carefully. 

Backing up the database is your responsibility.