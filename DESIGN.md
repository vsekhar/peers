# Design

## Layers

* Hosts open sockets via QUIC and just fire async messages to each other
  * frame.proto: a simple frame with a giant one_of containing all other possible messages
  * membership.proto: a membership gossip message
* Is this necessary? QUIC does framing already, can we re-use this and just open N async streams for different types of protobufs?
