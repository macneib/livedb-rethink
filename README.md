# livedb-rethink

RethinkDB database adapter for [livedb](https://github.com/share/livedb). This driver can be used both as a snapshot
store and oplog.

Snapshots are stored where you'd expect (the named collection with
\_id=docName). Operations are stored in `COLLECTION_ops`. If you have a
users collection, the operations are stored in `users_ops`. If you have a
document called `fred`, operations will be stored in documents called `fred
v0`, `fred v1`, `fred v2` and so on.

JSON document snapshots in livedb-rethink are unwrapped so you can use RethinkDB
queries directly against JSON documents. (They just have some extra fields in
    the form of `_v` and `_type`). You should always use livedb to edit
documents - don't just edit them directly in rethink. You'll get weird behaviour
if you do.

## Usage

LiveDB-rethink wraps [rethinkdbdash](https://github.com/neumino/rethinkdbdash). It
passes all the arguments straight to rethinkdbdash's constructor. `npm install
livedb-rethink` then create your database wrapper using the same arguments you
would pass to rethinkdbdash:

```javascript

TODO

```

If you prefer, you can instead create a rethinkdbdash instance yourself and pass it to livedb-rethink:

```javascript

TODO

```


## MIT License
Copyright (c) 2013 by Joseph Gentle, Nate Smith and Blair MacNeil

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
