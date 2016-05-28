Flo
=======

A naive and amateurish attempt at an event stream server in rust.

## Todo:

- [X] parse events as JSON
- [X] persist events
- [X] consumers start reading events at a specified point
- [X] logging
- [ ] namespaces
- [ ] remove consumers after connections are closed
- [ ] rebuild indexes from storage
- [ ] add endpoint to clear event cache
- [ ] multithreaded server
- [ ] batch writes to consumers
- [ ] Persist event index
- [ ] event filtering
