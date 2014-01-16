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
# name of instance must be unique in cluster.
name = "apple-juice-93"
# on which the cluster communicates
port = 7000
# on which the HTTP server runs
httpport = 3000
# created if it doesn't already exist
root = "/path/to/leveldb/root/directory"

# then a list of other known nodes in the cluster (I recommend 3 total at this stage)
[[node]]
    host = "127.0.0.1"  
    port = 8000
[[node]]
    host = "127.0.0.1"
    port = 9000
```

Note that you'll want `port + 1` to be open as well, since MecDB binds an N-to-1 ZeroMQ socket there.

#### API

MecDB offers a HTTP API.

**GET /mec/:key**

Performs a repairing read using R nodes. Gives back the consolidated data and a Vector Clock (X-Mec-Vclock) which a client should send when making PUT/POST requests.

Does not yet handle multiple responses for siblings.

Response format:

```
HTTP/1.1 200 OK
Content-Length: 17
Content-Type: application/json
Date: Thu, 16 Jan 2014 08:42:53 GMT
X-Mec-Vclock: gapjb3JtYWNyZWxmgqdDb3VudGVyAalUaW1lc3RhbXDPE0nH5/H4nFU=

"I am a machine"
```

**PUT /mec/:key**
**POST /mec/:key**

Performs a write using W nodes. The client must pass its latest known Vector Clock associated with the key to avoid siblings. Gives back an incremented VClock.

Response format:

```
HTTP/1.1 200 OK
Content-Length: 0
Content-Type: application/json
Date: Thu, 16 Jan 2014 08:52:18 GMT
X-Mec-Vclock: gapjb3JtYWNyZWxmgqdDb3VudGVyAqlUaW1lc3RhbXDPE0nIbta/ck0=

```

### License

```
Copyright (c) 2014 Cormac Relf

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```

