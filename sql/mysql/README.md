# MySQL schemas

Cloud Flexible Server deployment is blocked; `001_bootstrap.sql` is for local development.
It creates empty reporting/control databases and a run audit table. Reporting marts, publisher
transactions, application-user control permissions and watermark migrations are not implemented.
The Docker `taxi` user has access to `taxi_reporting` by default. Inspect control tables using
root for bootstrap verification; design narrower control privileges with the publisher.
