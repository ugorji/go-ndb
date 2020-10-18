# Package Documentation for github.com/ugorji/go-ndb/ndb

Package ndb provides a data management system built atop a key-value store
of bytes.

The key-value store should provide the following:

  - sorted key/value table (ala big table)
  - Get (with Iteration), Put, Delete
  - batch writes

ndb layers on top of this to provide:

  - data storage and retrieval
  - querying off indexes

There are 2 categories of data stored:

  - Entities
  - Indexes

A single database is used to store all entities and indexes. This affords us
some level of transactionality. However, the design allows sharding later.


## Entities

Typically, there are a few top-level entities (e.g. user) and all other data
pieces hang off these. To allow entities which naturally go together to
exist around the same area in the datastore, applications will use the same
localId (only changing the entityType).

The key in this table is 8-bytes (uint64) which can be broken down into:

```
    discrim:shardId:localid:entityType:entityShape:entryType
      Each of these sub-components is stored big-endian for easy ordering
```

where:

```
    discrim:      4-bits [0...16), used to pre-discriminate e.g. index, entity, etc
                  Ensure discrim id for entity is low (e.g. 1).
                  This allows us to easily convert the key to a int64 and back
                  without losing precision.
    shardId:      12-bits [0...4096), shard id
    localId:      32-bits [0...4.3 billion), id local to the shard
                  0-8192 are reserved
    entityType:   8-bits [0...256), used to store the entity type e.g. user, userprofile, etc
                  0-32 are reserved for internal use
    entityShape:  5-bits [0...32), used to store entity shape
    entryType:    3-bits [0...8), used to store entry type e.g. metadata, data, indexrow, etc
```

Because of this model of assigning ids, the id of an entity is "rich" and
cannot be randomly assigned (as it contains a lot of information about the
entity location and contents.

The intId as received by Entities is a subset of this, and includes just the
shardId and localId portions (ie 44bits, 4 0's in front and 16 0's at back).
This will allow you to still assign the same "entity id" to different types.

Configuration exists which maps entityType(string) to entityType(uint8).

There may be concerns with the limitations on number of entityTypes (only
255 allowed) or indexes (only 255 allowed) inherent in this design. This is
ok. The database is for only one application. Any other application can
interop via a loosely coupled API.


## Pinning Entities

Some entities can be pinned to shard id 1.

Those are entities with struct tag value pinned.

This way, it's easy to get all entities for that type by just checking one
shard alone.


## Auto Gen Entity Ids

These are also stored in the datastore.

Their format is similar to that listed above for entities, except that it
only contains:

```
    discrim: IDGEN
    shardid:
    entityType:
```


## Indexes

Each query maps to an index and must have a properly declared index.

Simple queries are supported:

  - Multiple Equality filters
  - At most one inequality filter at the end

At write time, we examine the metadata against the configured indexes and
write the appropriate indexes.

An index must intrinsically know what fields are in it. This is got from the
index configuration file.

Only primitives can be indexed, and we store values appropriately to enable
easy ascending sorting.

The following primitives can be indexed:

  - bool, int8...64, uint8...64, float32, float64, string

Internally, they are stored as uint8...64, or as []byte (for string). We
convert appropriately based on the type when storing.

Each index row has a prefix:

```
    Prefix: (uint8 containing initial 4-bits discrim in front)
            (1-byte typeId)(1-byte index id)
            (... indexed field values ...)
    Suffix: (sequence of uint64 which is the data key)
```

The value in the table for each index is always a single-byte: false. The
value is not used at all.

In the index corresponding to the query:

```
    select from (type) where (bool)=true, (int8)=9, (float32)=32.0,
                             (string)="yes",(string)>="abc"
```

The index row will have value of uint8 (0) and a key as:

```
    (index prefix)(uint8)(uint8)(uint32)(string)(nil-byte)(string)(nil-byte)(index-suffix)
```

Note the following rules which apply above to handle variable-length
strings:

  - Strings must come at the end
  - Strings are always terminated by a nil byte

From a user call site P.O.V., the following restrictions apply:

  - All indexes must be configured and named
  - A query runs against a specific index.
    The index must be inferred from the parameters to the query call.
  - If you need to sort on a set of fields, ensure they appear last in the properties
    in the sort order you want.
    The API only allows you say ascending or descending when you query.


## Anatomy of a write

For each key that is written, we write rows for:

  - data (codec encoded typed object)
  - metadata (codec encoded db.PropertyList).
  This is done so we can re-create the index later without having the schema of the typed object.
  - index rows.
  This is necessary so that we can delete current set of index  rows before creating new ones.

A Put operation is like:

```go
    type Metadata struct { key string, value interface{} }
      metadata values must be primitives
        (bool, int8...64, uint8...64, float32, float64, string)
      However, the type must match with the registered index.
```

```
    Put(key uint64, value interface{}, metadata []Metadata)
      if key == 0, then a new id is assigned.
```

When a Put is performed, we will in one atomic update:

  - Look up data and metadata for the key
  - If found, figure out old set of composite keys
  - Determine new set of index row keys from newly supplied metadata
  - Determine the full set of new writes and updates to index rows, entity data and entity metadata
  - Determine which index rows are no longer valid and should be removed
  - Do all changes in one write batch

Note that we currently store indexes in different databases from the actual
data. This gives us:

  - faster queries
  (as we don't have to distribute the query across the different servers)
  - support for limit, cursors (for paging, etc), etc.
  This is impossible with distributed queries, as all results must be returned, and sorted in-line.

For a delete:

  - Look up metadata
  - Look up index rows
  - Delete all in one write batch


## Ensuring Atomic Updates at row-level

For all these to work, we need to implement key-level locks for batches of
keys. i.e. you can get a lock for keys: 7, 9 and 13, then batch write them
all, and release the locks. This only works now while we use a single
server.


## Query Support

## For starters, our query support will be limited, and will NOT include

  - multiple inequality filters on a single property
    (only support single inequality filter)
  - sort order (Implementing sorting does not makes sense for ndb)


## Configuration

A single configuration file is used called ndb.json. It lives in the root
directory from which the server is started. It includes:

  - server location (defined later after sharding happens)


## Integration with db package

The following things to note:

  - ndb.json keeps mapping of kind to int


## Caching

Some caching will be configured in the database. ndb will not do any extra
caching beyond what is performed by the app (based on caching
configuration).


## Sharding

A shard is the unit of consistency. All entities in a shard can be edited in
an atomic fashion. This is because a shard is always managed by a single
server.

Entities can be sharded by shardid, while indexes can be sharded by index
name.

Note the following limitations once sharding is implemented:

  - Writing to indexes becomes a best effort.
    Indexes will not be written to the same datastore as the actual data,
    and so there's a possibility that data was written but index writing failed.

We chose not to store indexes alongside entities to allow us support:

  - Limit on queries:
    Limits are impractical if you have to hit 1000 servers
  - Paging:
    Paging is practical if we are only looking at a single index
  - Non-Distributed queries:
    To do a query, only check one place.

ndb.json will contain information on:

  - indexes (and how/where to access them)
  - servers and what data they serve (ie which servers serve which shards)


## Package Contents

This package contains everything related to ndb and napp:

  - Context implementation
  - low level driver
  - database management functionality as described above
  - etc.

There is no extra napp package.


## Starting

Initially, we will use a small shard range (shardid 1-16) and keep
everything on a single machine. In the future, we can change the range to
start creating entities on new shards.


## Single Server simplification

In a single server, things are simplified:

  - There might be no need for an external cache server, since the database
    already caches msgpack-encoded byte streams.
  - All shards are on single server. There's much less need to have to distribute
    configuration changes.
  - Indexes and Data live on same server.
  - Shards do not move between servers.
  - Mostly no need to synchronize ndb.json changes across a collection of nodes.

In a distributed deployment, none of these hold.


## Distributed Deployment

ndb must be developed in a way that can support multiple frontends and
multiple backends.

In a multi-server setup, we will use:

  - 1 database process per group of shards
  - n memcache processes

We need a way that changes to ndb.json are auto-detected and server
auto-reloaded, since it could hold information about cache servers, etc.

## Exported Package API

```go
const E_METADATA ...
const D_INDEX ...
var RpcCodecHandle ...
var Base64Enc = base64.URLEncoding ...
func CloseBackends() (err error)
func DecodeKey(s string) (k *Key, err error)
func KindAndShapeId(types []*Type, kind, shape string) (ikind uint8, ishp uint8)
func NewKey(uu ...uint64) (k *Key, err error)
func NewLowLevelDriver(cfg *Cfg, uuid string, indexes ...*Index) (l *LowLevelDriver, err error)
func ParseKeyBytes(bs []byte) (k *Key, err error)
func ReadConfig(cfgFile string) (cfg *Cfg, err error)
func SetupRpcServer(rpcsvr *rpc.Server, ss *Server, qlimitmax uint16, indexes ...*Index) (err error)
func UpdateEntityIdFromKey(d interface{}, intId int64) (err error)
func ValidateEntityData(ctxId app.Context, dst interface{}, k KeyParts) (err error)
type Blob struct{ ... }
type Cfg struct{ ... }
type Context struct{ ... }
type EntityIdParts struct{ ... }
    func GetEntityIdParts(v uint64) (x EntityIdParts)
type GetArgs struct{ ... }
type GetResult struct{ ... }
type Index struct{ ... }
type IndexProp struct{ ... }
type KV struct{ ... }
type Key struct{ ... }
type KeyParts struct{ ... }
    func GetKeyParts(ikey uint64) (x KeyParts)
type LowLevelDriver struct{ ... }
type NewIdArgs struct{ ... }
type PutArgs struct{ ... }
type QueryArgs struct{ ... }
type QueryIterResult struct{ ... }
type Range struct{ ... }
type Server struct{ ... }
type Type struct{ ... }
```
