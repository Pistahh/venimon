# venimon: a monitoring system for URLs

Venimon periodically "monitors" URLs and stores the status code and the elapsed
time in a PostgreSQL database. Optionally it also checks if a specific string
appears in the response body or not.

It consists of two programs:

* collector.py: monitors the URLs defined in the configuration and publishes the results into a Kafka stream
* dbstore.py: receives the data from the Kafka stream and stores them in a PostgreSQL database

## Configuration

The programs share a common configuration file. This file should be a YAML file with the following content:

    probes:
        - url: 'https://localhost:1'
        - url: 'https://www.iwanttomonitorthis.com'
          pattern: 'function'
        - url: 'https://doesnotexist'
    period: 10
    timeout: 3
    publish:
        bootstrap_servers: kafka-6caf36c-iii-2421.venimoncloud.com:22500
        ssl_cafile: /config/kafka-ca.pem
        ssl_certfile: /config/kafka-crt.pem
        ssl_keyfile: /config/kafka-key.pem
        client_id: "mymon"
        security_protocol: "SSL"
    db:
        uri: 'postgres://mon:asdfasdfasdf@pg-f2af18b-ooo-2421.venimoncloud.com:22498/defaultdb?sslmode=require'

### probes

List of URLs to monitor.
* url: url to monitor
* pattern: to check if the pattern is in the response body

### period

How often to check the URLS

### timeout

How long to wait for the HTTP requests to complete (not currently implemented)

### publish

Configuration of the Kafka produces.
See "keyword arguments" at https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
for the possible key/value pairs

### db
The uri of the PostgreSQL database where the data is to be stored

## Running the applications in Docker

### Building the containers

Running `build.sh` builds two Docker containers:
* collector: the monitor that collects monitoring data and publishes into Kafka
* dbstore: receives the data from Kafka and stores them in the DB.

### Running the containers

Put the configuraton file (with the name "config.yml") and the SSL
certificates/keys into a directory and mount them into the /config directory
of the container, e.g.

    $ docker run -v /here/is/my/config/:/config dbstore
    $ docker run -v /here/is/my/config/:/config collector
