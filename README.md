livedb-rethink
==============


RethinkDB database adapter for [livedb](https://github.com/share/livedb).
This driver can be used both as a snapshot store and oplog.

## Install

```sh
npm install --save livedb-rethink
```

## Schema

### Requirements

TODO

#### Snapshots Table

```javascript

{
	"data":  "Intersting text" ,
	"id":  "docName" ,
	"m": {
		"ctime": 1426789227620 ,
		"mtime": 1426790262477
	} ,
	"type": http://sharejs.org/types/textv1, »
	"v": 0
}

```

#### Operations Table

```javascript
{
	"id":  "docName v1" ,
	"m": {
		"ts": 1426789258264
	} ,
	"name":  "docName" ,
	"op": [
		15 ,
		"0" ,
		25 ,
		"Intersting text"
	] ,
	"preValidate": { } ,
	"seq": 2 ,
	"src":  "d73321d8db0fa4c51c28bdc57d22152f" ,
	"v": 1 ,
	"validate": { }
}
```

### Example

Here is an example statement that will work with sharejs and livedb-rethinkdb
note: you should already create a db called 'docShare' with two tables: 'users' and 'users_ops'
In the RethinkDD Data Explorer you should create a  compoundIndex like so
```javascript
r.db("docShare").table("users_ops").indexCreate("operations", [r.row("name"), r.row("v")])
```
now you can try

```javascript

var livedb = require('livedb');
var sharejs = require('share');

var dbConfig = {
  host: 'localhost',
  port: 28015,
  authKey: '',
  db: 'docShare',
};

var livedbrethink = require('livedb-rethink');
var db = require('livedb-rethink')(dbConfig);

var backend = livedb.client(db);
var share = sharejs.server.createClient({backend: backend});

```

## Usage

```javascript

TODO

```


## Testing

After creating database tables:

```sh
npm test
```

## Warning
This code is very new, untested and not definitely not optimized.

## MIT License
Copyright (c) 2013 by Blair MacNeil and Simon Clampitt

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