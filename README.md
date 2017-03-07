Flo
=======

A naive and amateurish attempt at an event stream server. This is mostly just a fun/learning project at the moment, so don't get your panties in a bunch if it doesn't work properly (or at all).

Installation
------------

- You'll need a nightly version of the Rust compiler. I recommend installing rustup and just running `rustup install nightly`.
- Inside the root of the repository, run `rustup override set nightly` to make sure it builds using the nightly version of the rust compiler.
- run `cargo install` from the root of the repository to install the executables. This will install both the `flo` server and the `flo-client` CLI.

## Running the Server

To run a basic server in standalone (non-clustering) mode, just running `flo` is enough. this will start the server with the default options and persist events in the current directory. Use `flo -d /path/to/data/dir` to specify a directory to use for persisting events. You can always run `flo --help` to get information on all the available options.

To start the server in clustering mode, you must supply the `--actor-id` argument to supply a unique integer id for the server. The actor id must be unique to the cluster and it can have a maximum value of 65536. Then you must also supply an argument to provide the address and port of at least one other node in the cluster. Additional cluster nodes will be discovered and joined automatically. Here's an example of starting a three node cluster locally:

- `$ flo -d /data1 -p 3000 --actor-id 1 --peer-addr 127.0.0.1:3001 --peer-addr 127.0.0.1:3002`
- `$ flo -d /data1 -p 3001 --actor-id 2 --peer-addr 127.0.0.1:3000 --peer-addr 127.0.0.1:3002`
- `$ flo -d /data1 -p 3002 --actor-id 3 --peer-addr 127.0.0.1:3000 --peer-addr 127.0.0.1:3001`

Voila, you now have a fault-tolerant cluster that will replicate all events to all three nodes.

## Using the Client CLI

Just having a server sitting there isn't very interesting, so let's start interacting with it. For now, we can just use the included `flo-client` cli tool to get the basics before we write our own client application. For starters, we don't currently have any events in the stream, so let's produce an event.

```bash
$ flo-client -H localhost -p 3000 produce -n "/food/breakfast/eggs" -d "I made some eggs"
0000000000000000000100001
Successfully produced 1 events to /food/breakfast/eggs
```

This command produced an event to the namespace "/food/breakfast/eggs" with "I made some eggs" as the event payload. The client kindly printed the id of the event: `0000000000000000000100001`. This is the canonical id of the event forevermore. For this example we used the `-H` and `-p` options to set the host and port of the server to connect to. It just so happens that `localhost:3000` is the default host and port for both the client and server applications, though, so we're just going to omit those from now on.

Now that we have an event in the stream, let's fire up a consumer to read the events. Open up another terminal and run:
```bash
$ flo-client consume -t

EventId: 0000000000000000000100001
Namespace: /food/breakfast/eggs
I made some eggs
```

The `-t` option tells the consumer to continue waiting for new events after it reaches the end of the stream, kind of like a `tail -f` for the events in the server. If you go back to the other terminal and produce more events, you should see them immediately printed in the consumer terminal. You can just `ctrl-c` to stop the consumer.

Now that we can produce and consume basic events, let's try an example using namespaces! Namespaces give consumers control over which events they are sent. Let's start by producing a few more events.

- `flo-client produce -n /food/breakfast/bacon -d "data about bacon"` - Events aren't required to have a body
- `flo-client produce -n /food/lunch/salad` - Events aren't required to have a body
- `flo-client produce -n /drinks/beer -d "wow, I'm such a great dancer"` 

Now, if you look at the first consumer, you'd see all of the events that you just produced. But most consumers aren't interested in _all_ the events in the stream. Consumers can subscribe to just a subset of events by using a glob pattern. Try specifying different namespace globs with `flo-client consume -n <your-glob-here>`.

Anatomy of a Flo Event
----------------------

An event in flo is defined as having a few attributes:
- `id` - This is the primary key of the event.
- `parent_id` - Events can optionally have a parent. This signifies a causal relationship between them. If a client application is processing event 1 and it responds with event 2, then the parent_id of event 2 should be set to event 1. Client libraries should do this automatically and transparently. This allows a much more sophisticated understanding of the interplay between application components. It also makes it _way_ easier to go back and see _what happened_ as a result of a particular event.
- `namespace` - Namespaces serve to categorize events. They are intended to help you more richly model your domain. Technically, a namespace can be any UTF-8 string that is terminated by a newline `\n` character, but glob patterns work way better with heirarchical paths, so it's a strongly recommended convention.
- `timestamp` - This is a UTC timestamp with millisecond precision that is set by the server when the event is persisted. Note that this is _not_ a monotonic timestamp, so it's possible for a prior event to have a `timestamp` that is _later_ than that of a subsequent event. This is not a particular limitation of flo so much as just how time works on computers, so it's not likely to change any time soon.
- `data` - Events may have an arbitrary payload of data. Flo imposes no restrictions about what you do with this section. It's just an array of binary data, as far as the server is concerned. Folks are of course strongly encouraged to use one encoding scheme exclusively. Client libraries may also have some opinions on how event data is serialized. It's perfectly fine to have an event without any extra `data`. 

## Guarantees

#### Durability

When a producer receives acknowledgement that an event was persisted successfully, that event will be 100% durable on one server. There is not currently an option to wait for more acknowledged writes from other servers. Once the event is persisted it will eventually be replicated to all other flo servers.

#### Consistency

In terms of [CAP Theorem](https://en.wikipedia.org/wiki/CAP_theorem), Flo is **AP**. That is, it chooses to be  _available_ and _partitionable_. It is _eventually consistent_, meaning that all the Flo servers in a cluster will eventually all contain the same sequence of events, but this is not guaranteed to happen immediately.

#### Ordering of events

Within each server events are strictly ordered by their `id`, and every Flo server instance will store the events in exactly the same order. Flo guarantees that, for any sequence of events written to a single server instance, those events will be _observed_ by all consumers in the order they were written. However, since Flo chooses to be _highly available_, it is possible for events that are written to separate servers to be observed out of order by some consumers. If your application requires events to be observed _exactly_ in the order they were produced, then you'll need to just write all events to a single server (you can still consume from many servers, though).


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
- [X] multithread client I/O on server
- [ ] Persist event index / deal with index larger than will fit in memory
- [X] clustering
- [ ] Consumer groups
- [ ] automatic serialization/deserialization in a higher-level consumer
- [ ] Other language clients (Java, Ruby, NodeJS, C#)
