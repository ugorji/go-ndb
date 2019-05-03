package ndb

import (
	"fmt"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"

	"github.com/ugorji/go-serverapp/app"
	"github.com/ugorji/go/codec"
	"github.com/ugorji/go-serverapp/db"
	"github.com/ugorji/go-common/errorutil"
	"github.com/ugorji/go-common/printf"
	"github.com/ugorji/go-common/safestore"
	"github.com/ugorji/go-common/simpleblobstore"
)

const (
	int64Max          = int64(math.MaxInt64)
	errIfPutZeroId    = true
	pinnedShardId     = 1
	entityNotFoundMsg = string(db.EntityNotFoundErr)
)

var (
	int64MaxBigint = big.NewInt(int64Max)
	RpcCodecHandle = &codec.BincHandle{}
)

type shardArgsResult struct {
	ShardId uint16
	Indexes []int
	Args    struct {
		Keys   []*Key
		Values [][]byte
		Props  []*db.PropertyList
	}
	Success bool
	Result  struct {
		GetResult
		Success bool
	}
	Error error
}

type LowLevelDriver struct {
	// TODO: support reconnect to rpc server.
	servers       []Server
	instanceCache app.Cache
	clients       map[uint16]*rpc.Client
	ti            []*Type
	mu            sync.RWMutex
	// cfg           *Cfg
	newEntityShard Range
	simpleblobstore.BlobDriver
}

type Context struct {
	// should implement logging.Detachable
	app.BasicContext
	r *http.Request
}

func (c *Context) Detach() interface{} {
	// detached context should implement logging.HasHostRequestId
	return &c.BasicContext
}

// func (l *LowLevelDriver) ServerStatus(ctx app.Context) (flags uint64, err error) {
// 	return l.db.SvrStatus(ctx)
// }

func (l *LowLevelDriver) updateShardIdForKind(shardId *uint16, kindid uint8) {
	if *shardId == 0 {
		for _, ty := range l.ti {
			if ty.KindId == kindid {
				if ty.tm.Pinned {
					*shardId = pinnedShardId
					log.Debug(nil, "Set shardId to %d, due to pinned kind: %s", pinnedShardId, ty.Kind)
				}
				break
			}
		}
	}
	if *shardId == 0 {
		*shardId = uint16(Rand.Int31n(int32(l.newEntityShard.Num))) + l.newEntityShard.Min
	}
	log.Debug(nil, ">>>> updateShardIdForKind: kind: %d, shardId, %d", kindid, *shardId)
}

func (l *LowLevelDriver) server(shardId uint16) (ss *Server, err error) {
	for i := 0; i < len(l.servers); i++ {
		ss = &l.servers[i]
		if shardId >= ss.Shards.Min && (shardId-ss.Shards.Min) < ss.Shards.Num {
			return ss, nil
		}
	}
	return nil, fmt.Errorf("ndb.LowLevelDriver: No Server for shardId: %d", shardId)
}

func (l *LowLevelDriver) client(shardId uint16) (c *rpc.Client, err error) {
	if shardId == 0 {
		err = fmt.Errorf("ndb.LowLevelDriver: Invalid key: Shard can't be 0")
		return
	}
	l.mu.RLock()
	c, ok := l.clients[shardId]
	l.mu.RUnlock()
	if !ok {
		var ss *Server
		if ss, err = l.server(shardId); err != nil {
			return
		}
		var conn net.Conn
		if conn, err = net.Dial("tcp", ss.Addr); err != nil {
			return
		}
		c = rpc.NewClientWithCodec(codec.GoRpc.ClientCodec(conn, RpcCodecHandle))
		l.mu.Lock()
		if c2, ok := l.clients[shardId]; ok {
			c.Close()
			c = c2
		} else {
			// l.clients[shardId] = c
			// maintain a single connection/rpc client to each server.
			// Go's RPC supports pipelining very well using goroutines/channels/codec locks.
			for i := uint16(0); i < ss.Shards.Num; i++ {
				l.clients[i+ss.Shards.Min] = c
			}
		}
		l.mu.Unlock()
	}
	return
}

func (l *LowLevelDriver) call(shardId uint16, methodName string, args interface{}, reply interface{}) (err error) {
	// if shardId == 0 && methodName == "IdForNewKey" {
	// 	shardId = uint16(Rand.Int31n(int32(l.newEntityShard.Num))) + l.newEntityShard.Min
	// }
	log.Debug(nil, ">>>> call: shardId: %d, method: %s", shardId, methodName)
	c, err := l.client(shardId)
	if err != nil {
		return
	}
	err = c.Call(fmt.Sprintf("shard-%d.%s", shardId, methodName), args, reply)
	return
}

func (l *LowLevelDriver) callIt(
	keys []app.Key, dst [][]byte, dprops []interface{}, fn func(*shardArgsResult) error,
) (m0 map[uint16]*shardArgsResult, err error) {
	// separate the keys by shardid.
	// for each shard, call the function in a goroutine.
	// collect the results back into dst.
	var merr errorutil.Multi
	m0 = make(map[uint16]*shardArgsResult)
	nkeys := appKeysToKeys(keys)
	for i, nk := range nkeys {
		ep := GetEntityIdParts(nk.V)
		m1, ok := m0[ep.Shard]
		if !ok {
			m1 = &shardArgsResult{ShardId: ep.Shard}
			m0[ep.Shard] = m1
		}
		m1.Indexes = append(m1.Indexes, i)
		m1.Args.Keys = append(m1.Args.Keys, nk)
		if len(dst) > 0 {
			m1.Args.Values = append(m1.Args.Values, dst[i])
			m1.Args.Props = append(m1.Args.Props, dprops[i].(*db.PropertyList))
		}
	}
	var wg sync.WaitGroup
	for _, v := range m0 {
		wg.Add(1)
		go func(v *shardArgsResult) {
			// v.Error = l.call(k, "Get", GetArgs{v.Keys, true}, &v.GetResult)
			v.Error = fn(v)
			wg.Add(-1)
		}(v)
	}
	wg.Wait()
	for _, v := range m0 {
		merr = append(merr, v.Error)
	}
	err = merr.NonNilError()
	return
}

func (l *LowLevelDriver) DriverName() string {
	return "ndb"
}

func (l *LowLevelDriver) InstanceCache() app.Cache {
	return l.instanceCache
}

func (l *LowLevelDriver) SharedCache(returnInstanceCacheIfNil bool) (c app.Cache) {
	if returnInstanceCacheIfNil {
		c = l.instanceCache
	}
	return
}

func (l *LowLevelDriver) IndexesOnlyInProps() bool {
	return true
}

func (l *LowLevelDriver) UseCache(ctx app.Context, preferred bool) bool {
	return preferred
}

func (l *LowLevelDriver) HttpClient(ctx app.Context) (c *http.Client, err error) {
	c = http.DefaultClient
	return
}

func (l *LowLevelDriver) Host(ctx app.Context) (s string, err error) {
	s = ctx.(*Context).r.Host
	return
}

func (l *LowLevelDriver) ParentKey(ctx app.Context, key app.Key) (pkey app.Key) {
	if pk2 := key.(*Key).Parent; pk2 != nil {
		pkey = pk2
	}
	return
}

func (l *LowLevelDriver) EncodeKey(ctx app.Context, key app.Key) (s string) {
	s = key.(*Key).Encode()
	return
}

func (l *LowLevelDriver) DecodeKey(ctx app.Context, s string) (k app.Key, err error) {
	defer errorutil.OnError(&err)
	return DecodeKey(s)
}

func (l *LowLevelDriver) NewContext(r *http.Request, appUUID string, seqnum uint64) (c app.Context, err error) {
	c = &Context{
		BasicContext: app.BasicContext{
			SeqNum:     seqnum,
			SafeStore:  safestore.New(false),
			TheAppUUID: appUUID,
		},
		r: r,
	}
	return
}

func (l *LowLevelDriver) GetInfoFromKey(c app.Context, key app.Key,
) (kind string, shape string, intId int64, err error) {
	// println();	println();	debug.PrintStack();	println();
	defer errorutil.OnError(&err)
	ikey := key.(*Key).V
	kp := GetKeyParts(ikey)
	cctx := app.CtxCtx(c)
	log.Debug(cctx, "GetInfoFromKey: KeyParts: %#v", kp)
	for _, ty := range l.ti {
		if ty.KindId == kp.Kind && ty.ShapeId == kp.Shape {
			kind, shape = ty.Kind, ty.Shape
			log.Debug(cctx, "GetInfoFromKey: Found match: kind: %v (%v), shape: %v (%v)",
				kind, kp.Kind, shape, kp.Shape)
			break
		}
	}
	if kind == "" {
		err = fmt.Errorf("No kind extracted from key, with extracted kindId: %v", kp.Kind)
		return
	}
	intId = kp.EntityId()
	log.Debug(cctx, "GetInfoFromKey: kind %v, shape %v, intId %v, err: %v", kind, shape, intId, err)
	return
}

// Ensure that the NewKey is in same shard as parent.
// The shard is the unit of consistency (parents/children live on same shard).
func (l *LowLevelDriver) NewKey(ctx app.Context, kind string, shape string, intId int64, pkey app.Key,
) (k app.Key, err error) {
	defer errorutil.OnError(&err)
	cctx := app.CtxCtx(ctx)
	log.Debug(cctx, "NewKey: kind: %v, shape: %v, Id: %v, pkey: %v", kind, shape, intId, pkey)
	kp := KeyParts{
		Discrim: D_ENTITY,
		Entry:   E_DATA,
	}
	kp.Kind, kp.Shape = KindAndShapeId(l.ti, kind, shape)
	if kp.Kind == 0 {
		err = fmt.Errorf("No kind found for name: %v", kind)
		return
	}
	var npkey *Key
	if pkey != nil {
		npkey = pkey.(*Key)
	}
	if intId > 0 {
		kp.EntityIdParts = GetEntityIdParts(uint64(intId))
		log.Debug(cctx, "NewKeyComponents: From intId: %v, got ishrd: %v, ilocid: %v, ==> %v",
			intId, kp.Shard, kp.Local, kp.EntityId())
	} else if intId <= 0 {
		// TODO: Validate this. Id == 0 also means get new id.
		idargs := NewIdArgs{Ikind: kp.Kind}
		// if npkey == nil {
		// 	for _, ty := range l.ti {
		// 		if ty.KindId == kp.Kind {
		// 			if ty.tm.Pinned {
		// 				idargs.IshardId = pinnedShardId
		// 				log.Debug(nil, "Set shard to 1, due to pinned %s", ty.Kind)
		// 			}
		// 			break
		// 		}
		// 	}
		// }
		if npkey != nil {
			idargs.IshardId = GetKeyParts(npkey.V).Shard
			log.Debug(cctx, "NewKeyComponents: Create Id in same shard (%v) as parent. Parent Bytes: %v",
				idargs.IshardId, npkey.Bytes())
		}
		l.updateShardIdForKind(&idargs.IshardId, idargs.Ikind)
		err = l.call(idargs.IshardId, "IdForNewKey", idargs.Ikind, &kp.EntityIdParts)
		if err != nil {
			return
		}
	}
	var nk *Key = &Key{
		Parent: npkey,
		V:      kp.V(),
	}
	log.Debug(cctx, "ndb.NewKey: Returning Shard: %d, Key: %v, Bytes: %v", kp.Shard, nk, nk.Bytes())
	k = nk
	return
}

func (l *LowLevelDriver) queryAllShards(qArgs *QueryArgs, qs *QueryIterResult) (err error) {
	// This will query all shards concurrently. By definition, it's more expensive.
	var chans []chan *rpc.Call
	var qsall []*QueryIterResult
	var c *rpc.Client
	for i := 0; i < len(l.servers); i++ {
		ss := &l.servers[i]
		for j := uint16(0); j < ss.Shards.Num; j++ {
			shardId := j + ss.Shards.Min
			if c, err = l.client(shardId); err != nil {
				return
			}
			qs2 := new(QueryIterResult)
			rc := c.Go(fmt.Sprintf("shard-%d.%s", shardId, "Query"), qArgs, qs2, make(chan *rpc.Call, 1))
			chans = append(chans, rc.Done)
			qsall = append(qsall, qs2)
		}
	}
	var merr errorutil.Multi
	for i, ch := range chans {
		rc := <-ch
		merr = append(merr, rc.Error)
		qs.Rows = append(qs.Rows, qsall[i].Rows...)
	}
	err = merr.NonNilError()
	return
}

func (l *LowLevelDriver) Query(ctx app.Context, parent app.Key,
	kind string, opts *app.QueryOpts, filters ...*app.QueryFilter,
) (res []app.Key, endCursor string, err error) {
	defer errorutil.OnError(&err)
	var qs QueryIterResult
	var npkey *Key
	if parent != nil {
		npkey = parent.(*Key)
	}
	qString := db.QueryAsString(parent, kind, opts, filters...)
	shape := ""
	if opts != nil {
		shape = opts.Shape
	}

	cctx := app.CtxCtx(ctx)
	//Do this Query lazily.
	//QuerySupport will call nextFn iff it didn't find results in cache.
	//nextFn will execute query on first request.
	var qsIterFn func() (app.Key, string, error)
	nextFn := func() (k3 app.Key, cursor3 string, err3 error) {
		if qsIterFn == nil {
			if err == nil {
				qArgs := &QueryArgs{kind, opts, filters, npkey}
				var pinned bool
				for _, ty := range l.ti {
					if ty.Kind == kind {
						if ty.tm.Pinned {
							pinned = true
						}
						break
					}
				}
				if pinned {
					err = l.call(pinnedShardId, "Query", qArgs, &qs)
				} else {
					err = l.queryAllShards(qArgs, &qs)
				}
			}
			if err == nil {
				var ress []string
				for _, bs := range qs.Rows {
					ress = append(ress, fmt.Sprintf("0x%x", bs))
				}
				log.Debug(cctx, "ndb.Driver: Query Results: %v", ress)
			} else {
				log.Debug(cctx, "ndb.Driver: Error from Backend Query: %v", err)
				err3 = err
				return
			}
			log.Debug(cctx, "Query Results: pkey: %v, kind: %v, qs: %v", npkey, kind, qs)
			qsIterFn = qs.IterFn()
		}
		return qsIterFn()
	}
	log.Debug(cctx, "ndb.Driver: Calling QuerySupport: kind: %v, shape: %v", kind, shape)
	return db.QuerySupport(ctx, qString, kind, shape, nextFn)
}

func (l *LowLevelDriver) DatastoreGet(ctx app.Context, keys []app.Key, dst []interface{}) (err error) {
	defer errorutil.OnError(&err)
	fn := func(v *shardArgsResult) error {
		return l.call(v.ShardId, "Get", GetArgs{v.Args.Keys, true}, &v.Result.GetResult)
	}
	m0, err := l.callIt(keys, nil, nil, fn)
	// res, err := l.db.SvrGet(ctx, appKeysToKeys(keys), true)
	if err != nil {
		return
	}
	cctx := app.CtxCtx(ctx)
	//log.Debug(cctx, ">>>>>>>> res: %v", logging.ValuePrinter{res})
	foundNonNilErr := false
	errs := make([]error, len(keys))
	for _, v := range m0 {
		gr := &v.Result.GetResult
		for i := range gr.Values {
			idx := v.Indexes[i]
			if gr.Errors != nil && gr.Errors[i] != "" {
				errs[idx] = maybeNotFoundError(gr.Errors[i])
			} else {
				errs[idx] = db.Codec.DecodeBytes(gr.Values[i], dst[idx])
				log.Debug(cctx, ">>>>>>>> (len: %v, err: %v) res: after unmarshal: %v",
					len(gr.Values[i]), errs[idx], printf.ValuePrintfer{dst[idx]})
			}
			if errs[idx] != nil && !foundNonNilErr {
				foundNonNilErr = true
			}
		}
	}
	// //There might be less errors than results. Bit me once.
	// for i := range keys {
	// 	if len(res.Errors) > i && res.Errors[i] != "" {
	// 		errs[i] = maybeNotFoundError(res.Errors[i])
	// 	} else {
	// 		errs[i] = db.Codec.DecodeBytes(res.Values[i], dst[i])
	// 		log.Debug(cctx, ">>>>>>>> (len: %v, err: %v) res: after unmarshal: %v",
	// 			len(res.Values[i]), errs[i], printf.ValuePrintfer{dst[i]})
	// 	}
	// 	if errs[i] != nil && !foundNonNilErr {
	// 		foundNonNilErr = true
	// 	}
	// }
	if foundNonNilErr {
		err = errorutil.Multi(errs)
	}
	return
}

func (l *LowLevelDriver) DatastoreDelete(ctx app.Context, keys []app.Key) (err error) {
	defer errorutil.OnError(&err)
	// res, err := l.db.SvrGet(ctx, appKeysToKeys(keys), true)
	// err = l.db.SvrDelete(ctx, appKeysToKeys(keys))
	fn := func(v *shardArgsResult) error {
		return l.call(v.ShardId, "Delete", v.Args.Keys, &v.Result.Success)
	}
	_, err = l.callIt(keys, nil, nil, fn)
	return
}

func (l *LowLevelDriver) DatastorePut(ctx app.Context, keys []app.Key, dst []interface{}, dprops []interface{},
) (keys2 []app.Key, err error) {
	defer errorutil.OnError(&err)
	//Validate here that if all keys are valid, since we can't validate on the server. (DONE)
	lkeys := appKeysToKeys(keys)
	keys2 = make([]app.Key, len(keys))
	ldst := make([][]byte, len(dst))
	for i := range dst {
		kp := GetKeyParts(lkeys[i].V)
		if kp.Shard == 0 || kp.Local == 0 {
			if errIfPutZeroId {
				err = fmt.Errorf("ndb.LowLevelDriver: Invalid key: Neither Shard (%v) nor Local (%v) can be 0",
					kp.Shard, kp.Local)
				return
			}
			if kp.Shard == 0 {
				if tm, err2 := db.GetStructMeta(dst[i]); err2 == nil && tm != nil && tm.Pinned {
					kp.Shard = pinnedShardId
					log.Debug(nil, "Set shard to 1, due to pinned %T", dst[i])
				}
			}
			// get new key from datastore
			// if kp.EntityIdParts, err = l.db.IdForNewKey(ctx, kp.Shard, kp.Kind)
			l.updateShardIdForKind(&kp.Shard, kp.Kind)
			if err = l.call(kp.Shard, "IdForNewKey", kp.Kind, &kp.EntityIdParts); err != nil {
				return
			}
			lkeys[i].V = kp.V()
			// Update Id in dst
			if err = UpdateEntityIdFromKey(dst[i], kp.EntityId()); err != nil {
				return
			}
		} else {
			// validate that real keys previously got from datastore are for this kind of component
			if err = ValidateEntityData(ctx, dst[i], kp); err != nil {
				return
			}
		}
		keys2[i] = lkeys[i]
		if err = db.Codec.EncodeBytes(&ldst[i], dst[i]); err != nil {
			return
		}
	}
	// ldprops := make([]*db.PropertyList, len(dprops))
	// for i := range dprops {
	// 	ldprops[i] = dprops[i].(*db.PropertyList)
	// }

	//Do actual inserts into the datastore
	fn := func(v *shardArgsResult) error {
		return l.call(v.ShardId, "Put", &PutArgs{v.Args.Keys, v.Args.Values, v.Args.Props},
			&v.Result.Success)
	}
	_, err = l.callIt(keys2, ldst, dprops, fn)
	// err = l.db.SvrPut(ctx, lkeys, ldst, ldprops)
	return
}

func NewLowLevelDriver(cfg *Cfg, uuid string, indexes ...*Index) (l *LowLevelDriver, err error) {
	defer errorutil.OnError(&err)

	// s := cfg.Servers[0]
	// db, err := newClient(uuid, s.NumConn, cfg.QueryLimitMax, s.Addr, cfg.NewEntityShardId, ti)
	// if err != nil {
	// 	return
	// }

	// ss := cfg.FindServer(soloServerName)
	// if ss == nil {
	// 	err = fmt.Errorf("ndb/Driver: No server configured with name: %s", soloServerName)
	// 	return
	// }
	l = &LowLevelDriver{
		servers:       cfg.Servers,
		instanceCache: app.SafeStoreCache{safestore.New(true)},
		clients:       make(map[uint16]*rpc.Client),
		ti:            readTypes(),
		BlobDriver:    simpleblobstore.BlobDriver{Dir: cfg.Blob.BaseDir},
		// cfg:           cfg,
		newEntityShard: cfg.NewEntityShardId,
	}

	return
}

func appKeysToKeys(keys []app.Key) (kss []*Key) {
	kss = make([]*Key, len(keys))
	for i := range keys {
		kss[i] = keys[i].(*Key)
	}
	return
}

func keysEncoded(keys []app.Key) (ss []interface{}) {
	ss = make([]interface{}, len(keys))
	for i := 0; i < len(keys); i++ {
		ss[i] = keys[i].(*Key).Encode()
	}
	return
}

//If s starts with EntityNotFoundMsg, return an instance of NotFound
//else just return an String
func maybeNotFoundError(s string) (err error) {
	if strings.HasPrefix(s, entityNotFoundMsg) {
		err = db.EntityNotFoundError(s)
	} else {
		err = errorutil.String(s)
	}
	return
}

var _ app.LowLevelDriver = &LowLevelDriver{}
