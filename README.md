Flo
=======

A naive and amateurish attempt at an event stream server. This is mostly just a fun/learning project at the moment, so don't get your panties in a bunch if it doesn't work properly (or at all).

## The Problem with Microservices

Traditional microservices are made using either Http or message queues. HTTP and Message Queues aren't actually great for microservices, though. Http microservices require a ton of extra work to operate in the real world (monitoring, tracing, orchestration). Message Queues allow services to be further decoupled (Service A doesn't have to know whether Service B is currently running), but they really don't help much with being able to monitor the system or have visibility into it.

The [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) pattern aims to address these issues by allowing services to be truly decoupled. The biggest problem with using Event Sourcing is the infrastructure supporting it really sucks. Kafka has ridiculous hardware/software requirements, and pushes tons of complexity down onto the clients. MongoDB is currently on of the most popular tools, and it lacks basic stuff like sequential ID generation so consumers can restart where they left off.

Flo is an attempt (albeit an amateurish one) to be better. The server should be fault-tolerant, scalable, fast, and simple to operate. It should not require any external dependencies such as zookeeper or other databases. Flo client libraries should be very simple to use without losing power or flexibility. This is all still in the early stages, though, so don't use it for ...anything.

## Features

- [X] Persistent storage of events
- [X] Client subscription
- [X] Globbing on event namespaces
- [X] Client responses to an event persist the parent id
- [X] Logging controllable from command line
- [X] Controllable max memory usage by limiting in-memory cache size
- [ ] remove oldest events once max event threshold is reached
- [ ] multithread client I/O on server
- [ ] Persist event index / deal with index larger than will fit in memory
- [ ] clustering
- [ ] Consumer groups
- [ ] automatic serialization/deserialization in a higher-level consumer
- [ ] Other language clients (Java, Ruby, NodeJS, C#)
