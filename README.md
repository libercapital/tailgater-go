# Tailgater

This project has the objective to implement the transactional log tailing pattern describe here: https://microservices.io/patterns/data/transaction-log-tailing.html

The principle idea is to tail the database transctional log as way to trigger amqp message publishes. It looks at any insert made to an outbox table (Also a pattern described here: https://microservices.io/patterns/data/transactional-outbox.html) and sends a message with its table data. This ways, its possible to use the database trasactions as a way to ensure both data updates and amqp messages happens always together.

This project its designed to run at the side of any other project that wants to propagated database changes as amqp messages. It relies on the database having a table with the name outbox comprised of the following properties:

```sql
create table outbox (
	id bigInt primary key not null,
	message json,
	exchange varchar(100),
	router_key varchar(100),
	correlation_id varchar(100),
	reply_to varchar(100)
)
```

Its also necessary to change the database wal_level to logical, so as to work with logical replication slots.


## Getting started

Use the docker-compose contained in the project to start its necessary infrasctructure (rabbit and postgresql). After that its neceessary to create a environment with the envs described in the .env.example. To run the project, just use go run main.go
