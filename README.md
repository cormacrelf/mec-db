MecDB
-----

A distributed key-value store based on Riak and Dynamo, built in Go on top of ZeroMQ and LevelDB.

This is a fun project, not an enterprise-ready behemoth. Please don't use it for anything. 

### Installation

```
go get github.com/cormacrelf/mec-db/mec/
```

### Usage

#### Mec command

Syntax:

```
mec --config /path/to/config.conf
```

The MecDB config file is formatted with [TOML](https://github.com/mojombo/toml), and follows this format:

```toml
name = "apple-juice"    # name of instance must be unique in cluster.

port = 7000             # on which the cluster communicates

httpport = 3000         # on which the HTTP server runs

root = "/path/to/leveldb/root/directory"
                        # created if it doesn't already exist

# then a list of other known nodes in the cluster (I recommend 3 total at this stage)
[[node]]
    host = "127.0.0.1"  
    port = 8000
[[node]]
    host = "127.0.0.1"
    port = 9000
```
