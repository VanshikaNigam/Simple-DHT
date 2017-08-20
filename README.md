# Simple-DHT

A Simple Distributed Hash Table on a Chord
There are three things implemented: 
1. ID space partitioning/re-partitioning, 
2. Ring-based routing, and 
3. Node joins.

On running multiple instances of the app, all content provider instances form a Chord ring and serve insert/query requests in a distributed fashion according to the Chord protocol.

