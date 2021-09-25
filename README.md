# peers

Peers is a library for peer to peer discovery and services in Go.

## TODO

* Peers discovery mechanisms
  * Local:
    * File? Peers exclusive open, write their name if empty, every so often check if that peer is still there?
    * Directory? Each peer writes a file? How to cleanup?
    * In either case, mark as not suitable for production use.
  * GCE managed instance group
  * Kubernetes downward API
