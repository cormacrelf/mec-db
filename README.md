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

Handles multiple responses for siblings with `300 Multiple Choices`.

**Single Response format**:

```
HTTP/1.1 200 OK
Content-Length: 17
Content-Type: application/json
Date: Thu, 16 Jan 2014 08:42:53 GMT
X-Mec-Vclock: gapjb3JtYWNyZWxmgqdDb3VudGVyAalUaW1lc3RhbXDPE0nH5/H4nFU=

"I am a machine"
```

**Multiple Choice format**:

When clients A, B, and C have written versions 1-3 respectively, simultaneously on different servers such that they have divergent clocks. Consists of `mime/multipart`-separated responses that each have a Last-Modified and unix-nanosecond timestamp. Only one VClock is returned, which is a descendent of each of the multiple responses such that a client may resolve the conflict by POST/PUTting passing the merged clock and a merged value.

```
HTTP/1.1 300 Multiple Choices
Content-Length: 625
Content-Type: mime/multipart
Date: Fri, 17 Jan 2014 23:52:51 GMT
X-Mec-Vclock: g6FDgqdDb3VudGVyAalUaW1lc3RhbXDPE0pH93pICm+hQYKnQ291bnRlcgGpVGltZXN0YW1wzxNKR+q4T3ofoUKCp0NvdW50ZXIBqVRpbWVzdGFtcM8TSkf15mgjww==

--0029149167aa0f3629c99cf2e5e07aa0d87f4843608c765c546776224747
Content-Type: application/json; charset=utf-8
Last-Modified: Sat, 18 Jan 2014 10:49:23 GMT
X-Mec-Timestamp: 1390002563231255151

version 3

--0029149167aa0f3629c99cf2e5e07aa0d87f4843608c765c546776224747
Content-Type: application/json; charset=utf-8
Last-Modified: Sat, 18 Jan 2014 10:48:28 GMT
X-Mec-Timestamp: 1390002508437355039

version 1

--0029149167aa0f3629c99cf2e5e07aa0d87f4843608c765c546776224747
Content-Type: application/json; charset=utf-8
Last-Modified: Sat, 18 Jan 2014 10:49:16 GMT
X-Mec-Timestamp: 1390002556455363523

version 2

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

