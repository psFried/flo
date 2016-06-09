Flo
=======

A naive and amateurish attempt at an event stream server in rust.

## Todo:

- [X] parse events as JSON
- [X] persist events
- [X] consumers start reading events at a specified point
- [X] logging
- [X] namespaces
- [X] persist events across restarts
- [X] rebuild indexes from storage
- [ ] remove oldest events once max event threshold is reached
- [ ] remove consumers after connections are closed
- [ ] multithreaded server
- [ ] batch writes to consumers
- [ ] Persist event index
- [ ] event filtering
- [ ] clustering
