// +build cgo,linux ignore

// Deprecated: Now the server is a C++ server and we communicate over the network.
// This is now effectively ignored during a build.

package ndb

// This whole file is VERY UNSAFE. CARE IS TAKEN SERIOUSLY.
//
// This tries to use cgo efficiently.
// We will use unsafe extensively to allow us share memory between C and Go.
// We only copy to go memory if the value must escape the function.
//
// All C calls are coarse calls. This way, chatter and cgo boundary crossing
// costs are managed.
//
// This file depends on libndb.so created from <ugorji/ndb> c/c++ package.

import (
	"bytes"
	"fmt"
	"net/rpc"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"time"

	//"container/list"
	"unsafe"

	"github.com/ugorji/go-serverapp/app"
	"github.com/ugorji/go-serverapp/db"
	"github.com/ugorji/go-common/logging"
	"github.com/ugorji/go-common/printf"
	"github.com/ugorji/go-common/errorutil"
)

// No need to define these here, since all c code already compiled.
// // #include <stddef.h>
// // #cgo CFLAGS: -std=c99
// // #cgo CXXFLAGS: -std=c++11

// NOTE: ndb.h and ndb shared library are expected to be found in the current directory.

// #cgo CPPFLAGS: -fPIC -I${SRCDIR} -g
// #cgo LDFLAGS: -lpthread -lglog -lrocksdb -lsnappy -lbz2 -lz -L${SRCDIR} -lndb -g
// #include <ndb.h>
import "C"

var basicQueriesOnlyErr error = errorutil.String(
	"Queries are not supported for inequality filters (except last filter) or sort order")

type bytesWIndex struct {
	B []byte
	I int
}

type bytesWIndexSlice []bytesWIndex

func (x bytesWIndexSlice) Len() int      { return len(x) }
func (x bytesWIndexSlice) Swap(i, j int) { x[i], x[j] = x[j], x[i] }
func (x bytesWIndexSlice) Less(i, j int) bool {
	return bytes.Compare(x[i].B, x[j].B) < 0
}

// this object is exposed in rpc server
type rpcbackend struct {
	b *backend
}

func (l rpcbackend) Get(args GetArgs, res *GetResult) (err error) {
	log.Debug(nil, "rpcbackend.Get")
	return l.b.SvrGet(args.Keys, args.ErrIfNotFound, res)
}

func (l rpcbackend) Delete(args []*Key, res *bool) (err error) {
	log.Debug(nil, "rpcbackend.Delete")
	err = l.b.SvrDelete(args)
	*res = err == nil
	return
}

func (l rpcbackend) Put(args PutArgs, res *bool) (err error) {
	log.Debug(nil, "rpcbackend.Put")
	err = l.b.SvrPut(args.Keys, args.Values, args.Props)
	*res = err == nil
	return
}

func (l rpcbackend) IdForNewKey(ikind uint8, x *EntityIdParts) (err error) {
	log.Debug(nil, "rpcbackend.IdForNewKey")
	return l.b.IdForNewKey(ikind, x)
}

func (l rpcbackend) Query(args *QueryArgs, qs *QueryIterResult) (err error) {
	log.Debug(nil, "rpcbackend.Query")
	return l.b.Query(args, qs)
}

type backend struct {
	qlimitmax uint16
	types     *typeInfo
	shardId   uint16
	db        *C.struct_ndb_t
}

func (l *backend) IdForNewKey(ikind uint8, x *EntityIdParts) (err error) {
	defer errorutil.OnError(&err)
	x.Shard = l.shardId
	kp := KeyParts{Discrim: D_IDGEN, EntityIdParts: EntityIdParts{Shard: x.Shard}, Kind: ikind}
	bs := Key{V: kp.V()}.Bytes()
	// _, bs := nextLocalIdKey(x.Shard, ikind)
	nextid, err := l.nextLocalId(bs)
	if err != nil {
		return
	}
	log.Debug(nil, "IdForNewKey: Inserted new nextid: %v for kind: %v (shard: %v)",
		nextid, ikind, x.Shard)
	x.Local = nextid
	return
}

// func (l *backend) SvrQuery(pkey *Key, kind string,
// 	opts *app.QueryOpts, filters ...*app.QueryFilter,
// ) (qs QueryIterResult, err error) {
func (l *backend) Query(args *QueryArgs, qs *QueryIterResult) (err error) {
	defer errorutil.OnError(&err)
	pkey, kind, opts, filters := args.ParentKey, args.Kind, args.Opts, args.Filters
	log.Debug(nil, "Running Query: shard %d, pkey: %v, kind: %v, opts: %v, filters: %v",
		l.shardId, pkey, kind, opts, filters)
	//currently, ndb only supports basic queries
	//  - no inequality filters (except for last filter)
	//  - no sort order
	var shape string
	if opts != nil {
		shape = opts.Shape
		if opts.Order != "" {
			err = basicQueriesOnlyErr
			return
		}
	}
	kindid, shapeid := KindAndShapeId(l.types.Types, kind, shape)
	_, _ = kindid, shapeid
	var irpfx, irpfx2 []byte
	lastFilterOp := app.EQ
	ancestorOnlyQuery := (len(filters) == 0 && pkey != nil)
	withCursorQuery := opts != nil && opts.StartCursor != ""
	if ancestorOnlyQuery {
		irpfx = pkey.Bytes()
		log.Debug(nil, "Ancestor Query using scan prefix: %v", irpfx)
	} else if withCursorQuery {
		if irpfx, err = Base64Enc.DecodeString(opts.StartCursor); err != nil {
			return
		}
		if opts.EndCursor != "" {
			if irpfx2, err = Base64Enc.DecodeString(opts.EndCursor); err != nil {
				return
			}
		}
	} else {
		// all but last filter must be equalities filters
		// last filter may be an inequality filter
		for i := 0; i < len(filters)-1; i++ {
			if filters[i].Op != app.EQ {
				err = basicQueriesOnlyErr
				return
			}
		}
		// find the index which has exactly the same set of keys
		// as the equality filters.
		fnames := make([]string, len(filters))
		fvalues := make([]interface{}, len(filters))
		for i := 0; i < len(filters); i++ {
			fnames[i] = filters[i].Name
			fvalues[i] = filters[i].Value
		}
		cty, cindxs := getIndexes(nil, l.types, 'q', 0, kind, fnames)
		if len(cindxs) == 0 {
			err = fmt.Errorf("No Index found matching filters: %v", filters)
			return
		}
		if len(cindxs) != 1 {
			err = fmt.Errorf("Multiple (%v) Indexes found matching filters: %v", len(cindxs), filters)
			return
		}
		//construct a scan prefix from the filters
		irpfx = indexRowBytes(nil, cindxs[0], nil, cty.KindId, fnames, fvalues)
		log.Debug(nil, "Index Query using scan prefix: %v", irpfx)

		if len(filters) > 0 {
			lastFilterOp = filters[len(filters)-1].Op
			irpfx2 = indexRowBytes(nil, cindxs[0], nil, cty.KindId, fnames[:len(fnames)-1],
				fvalues[:len(fvalues)-1])
		}
	}

	var limit, offset uint16
	if opts != nil {
		limit, offset = uint16(opts.Limit), uint16(opts.Offset)
	}
	if limit == 0 {
		limit = uint16(l.qlimitmax)
	}

	log.Debug(nil, "Will Now send request to ndbserver: lastFilterOp: [%v], %v", lastFilterOp, irpfx)

	var xRes *C.slice_bytes_t
	var xNumRes C.size_t
	var xErr *C.slice_bytes_t
	var irpfx0, irpfx20 *C.char
	if len(irpfx) > 0 {
		irpfx0 = (*C.char)(unsafe.Pointer(&irpfx[0]))
	}
	if len(irpfx2) > 0 {
		irpfx20 = (*C.char)(unsafe.Pointer(&irpfx2[0]))
	}
	time0 := time.Now()
	xReqKey := C.ndb_query(l.db,
		C.slice_bytes_t{len: C.size_t(len(irpfx)), v: irpfx0},
		C.slice_bytes_t{len: C.size_t(len(irpfx2)), v: irpfx20},
		C.uint8_t(kindid), C.uint8_t(shapeid),
		C.bool(ancestorOnlyQuery), //(bool2Byte(ancestorOnlyQuery)),
		C.bool(withCursorQuery),
		C.uint8_t(uint8(lastFilterOp)),
		C.size_t(offset),
		C.size_t(limit),
		&xRes, &xNumRes, xErr)
	defer C.ndb_release(&xReqKey, 1)
	log.Debug(nil, "C.ndb_query completed (with %d results) in %v", xNumRes, time.Since(time0))
	if xErr != nil {
		err = errorutil.String(C.GoStringN(xErr.v, C.int(xErr.len)))
		return
	}

	if xRes != nil && xNumRes > 0 {
		var xResGo []C.slice_bytes_t
		log.Debug(nil, "Server.Query: xRes: %p, xRes64bit: 0x%x", xRes, *(*uint64)(unsafe.Pointer(xRes)))
		c2goArray(unsafe.Pointer(&xResGo), unsafe.Pointer(xRes), int(xNumRes))
		for i := 0; i < int(xNumRes); i++ {
			// convert to Go bytes so we can send it upwards and free c memory
			// log.Debug(nil, "Server.Query: %d, arr: %p, %v", i, &xResGo[i], xResGo[i])
			res1 := C.GoBytes(unsafe.Pointer(xResGo[i].v), C.int(xResGo[i].len))
			qs.Rows = append(qs.Rows, res1)
			log.Debug(nil, "Server.Query: %d, arr: %p, len: %d, v: %v, val: 0x%x",
				i, &xResGo[i], xResGo[i].len, xResGo[i].v, res1)
		}
	}

	log.Debug(nil, "Server.Query: Retrieved %v results. Error: %v",
		len(qs.Rows), err != nil)
	return
}

//--------------------------------------------------------

func (l *backend) SvrGet(nkss []*Key, errIfNotFound bool, res *GetResult) (err error) {
	bkss := make([][]byte, len(nkss))
	for i := range nkss {
		bkss[i] = nkss[i].Bytes()
	}
	return l.svrGet(nkss, bkss, errIfNotFound, res)
}

func (l *backend) SvrGetBytes(bkss [][]byte, errIfNotFound bool, res *GetResult) (err error) {
	return l.svrGet(nil, bkss, errIfNotFound, res)
}

func (l *backend) svrGet(nkss []*Key, bkss [][]byte, errIfNotFound bool, res *GetResult) (err error) {
	//For best performance, sort the keys bytewise, before calling ndb_getXXX.
	//This way, ndb just walks the query in forward motion only
	sbytes := make(bytesWIndexSlice, len(bkss))
	for i := range bkss {
		sbytes[i] = bytesWIndex{bkss[i], i}
	}
	sort.Sort(sbytes)

	ckeys := make([]C.slice_bytes_t, len(bkss))
	for i := 0; i < len(bkss); i++ {
		if len(bkss[i]) == 0 {
			ckeys[i] = C.slice_bytes_t{len: 0, v: nil}
		} else {
			ckeys[i] = C.slice_bytes_t{len: C.size_t(len(bkss[i])), v: (*C.char)(unsafe.Pointer(&bkss[i][0]))}
		}
	}

	var xRes *C.slice_bytes_t // ptr to element 0 in array of slice_bytes
	var xErr *C.slice_bytes_t // ptr to element 0 in array of strings
	time0 := time.Now()
	xReqKey := C.ndb_get_multi(l.db, C.size_t(len(bkss)), &ckeys[0], &xRes, &xErr)
	defer C.ndb_release(&xReqKey, 1)
	log.Debug(nil, "C.ndb_get_multi completed in %v", time.Since(time0))

	res.Errors = make([]string, len(bkss))
	res.Values = make([][]byte, len(bkss))

	var xResGo []C.slice_bytes_t
	var xErrGo []C.slice_bytes_t

	c2goArray(unsafe.Pointer(&xResGo), unsafe.Pointer(xRes), len(bkss))
	c2goArray(unsafe.Pointer(&xErrGo), unsafe.Pointer(xErr), len(bkss))

	var xErrGoBytes [][]byte = c2goArrayBytes(xErrGo)
	var xErrGoStr []string = c2goArrayString(xErrGo)
	var xResGoBytes [][]byte = c2goArrayBytes(xResGo)

	// Note: Don't retain c memory beyond this call. Copy them into res.Errors/res.Values
	log.Debug(nil, ">>>>>> Get: Shard: %d, Request for %v keys", l.shardId, len(bkss))
	foundErr := false
	var numSuccess, numFail int
	for i := range bkss {
		j := sbytes[i].I
		log.Debug(nil, ">>>> Get: Shard: %d, Index: %v/%v, Error: %v", l.shardId, i, j, xErrGoStr[i])
		if xErrGoStr[i] != "" {
			if xErrGoStr[i] == entityNotFoundMsg {
				if errIfNotFound {
					if len(nkss) > j {
						res.Errors[j] = fmt.Sprintf("%s Key: %v", entityNotFoundMsg, nkss[j])
					} else if len(bkss) > j {
						res.Errors[j] = fmt.Sprintf("%s Bytes: %v", entityNotFoundMsg, bkss[j])
					} else {
						res.Errors[j] = entityNotFoundMsg
					}
					foundErr = true
				}
			} else {
				// res.Errors[j] = xErrGoStr[i]
				res.Errors[j] = string(xErrGoBytes[i])
				foundErr = true
			}
			numFail++
		} else {
			// res.Values[j] = xResGoBytes[i]
			res.Values[j] = make([]byte, len(xResGoBytes[i]))
			copy(res.Values[j], xResGoBytes[i])
			numSuccess++
		}
	}
	if !foundErr {
		res.Errors = nil
	}
	log.Debug(nil, ">>>>> Get: Shard: %d, returning. Success: %v, Fail: %v", l.shardId, numSuccess, numFail)
	return
}

// appendIndexRows will append index rows to hkssptr.
// The parameter irkbss is a slice of keys with the E_INDEXROW appropriately set.
// The parameter hkssptr is a pointer to the slice that the index rows should be appended to.
func (l *backend) appendIndexRows(hkssptr *[][]byte, irkbss [][]byte) (err error) {
	hkss := *hkssptr
	//irkbsVs, irkbsErrs := l.SvrGetBytes(nil, irkbss, false)
	var res GetResult
	if err = l.SvrGetBytes(irkbss, false, &res); err != nil {
		return
	}
	if res.Errors != nil {
		errs := make([]error, len(res.Errors))
		for i := range errs {
			errs[i] = maybeNotFoundError(res.Errors[i])
		}
		err = errorutil.Multi(errs)
		return
	}
	for i := range res.Values {
		if len(res.Values[i]) > 0 {
			irks := make([][]byte, 0, 4)
			if err = db.Codec.DecodeBytes(res.Values[i], &irks); err != nil {
				return
			}
			for _, irk0 := range irks {
				hkss = append(hkss, irk0)
			}
		}
	}
	*hkssptr = hkss
	return
}

func (l *backend) SvrDelete(keys []*Key) (err error) {
	defer errorutil.OnError(&err)
	// re-organized to make only 2 C calls.
	hkss := make([][]byte, 0, len(keys)*4)
	irkbss := make([][]byte, 0, len(keys))
	for i := range keys {
		kbs := keys[i].Bytes()
		hkss = append(hkss, kbs)
		mdkbs := keyBytesForEntryType(kbs, E_METADATA)
		hkss = append(hkss, mdkbs)
		irkbs := keyBytesForEntryType(kbs, E_INDEXROW)
		irkbss = append(irkbss, irkbs)
	}
	if err = l.appendIndexRows(&hkss, irkbss); err != nil {
		return
	}
	err = l.svrUpdate(nil, nil, hkss)
	return
}

func (l *backend) oneOffPut(key []byte, val []byte) (err error) {
	err = l.svrUpdate([][]byte{key}, [][]byte{val}, nil)
	return
}

func (l *backend) SvrPut(keys []*Key, dst [][]byte, dprops []*db.PropertyList,
) (err error) {
	defer errorutil.OnError(&err)

	log.Debug(nil, "DB.PUT: KEYS: (%d) %v, dst: (%d) %v, dprops: (%d) %v",
		len(keys), keys, len(dst), printf.ValuePrintfer{dst}, len(dprops), printf.ValuePrintfer{dprops})

	hkss := make([][]byte, 0, len(keys)*4)
	hvss := make([][]byte, 0, len(keys)*4)
	hdss := make([][]byte, 0, len(keys)*4)
	irkbss := make([][]byte, 0, len(keys))

	for i := range keys {
		kp := GetKeyParts(keys[i].V)
		// All keys here are expected to be valid. We err out if not.
		if kp.Shard == 0 || kp.Local == 0 {
			err = fmt.Errorf("SvrPut: Invalid key: Neither ishrd (%v) nor ilocid(%v) can be 0",
				kp.Shard, kp.Local)
			return
		}
		//put actual data
		// log.Debug(nil, "Adding write data for: %v", k) //%064b
		kbs := keys[i].Bytes()
		hkss = append(hkss, kbs)
		hvss = append(hvss, dst[i])

		//delete old index rows
		irkbs := keyBytesForEntryType(kbs, E_INDEXROW)
		irkbss = append(irkbss, irkbs)

		if len(dprops) > i {
			mdkbs := keyBytesForEntryType(kbs, E_METADATA)
			vdprops := *(dprops[i])
			li := newDbIndex(nil, vdprops)
			if len(li.P) > 0 {
				var vbsx []byte
				if err = db.Codec.EncodeBytes(&vbsx, li); err != nil {
					return
				}
				hkss = append(hkss, mdkbs)
				hvss = append(hvss, vbsx)
			}

			//we need to find the combinations of all the keys/values
			_, indxs := getIndexes(nil, l.types, 's', kp.Kind, "", li.P)
			if len(indxs) > 0 {
				irbsbs := make([][]byte, 0, len(indxs))
				for _, tyin3 := range indxs {
					irnvg := li.subset(nil, tyin3).newIndexRowNameValueGen(nil)
					for pvalues := irnvg.first(); pvalues != nil; pvalues = irnvg.next() {
						irbs5 := indexRowBytes(nil, tyin3, kbs, kp.Kind, irnvg.pnames(), pvalues)
						// log.Debug(nil, "irbs5: %v", irbs5)
						hkss = append(hkss, irbs5)
						hvss = append(hvss, []byte{0})
						irbsbs = append(irbsbs, irbs5)
					}
				}
				//add indexrows in entities side
				var vbsx []byte
				if err = db.Codec.EncodeBytes(&vbsx, irbsbs); err != nil {
					return
				}
				hkss = append(hkss, irkbs)
				hvss = append(hvss, vbsx)
			}
		}
	}

	if err = l.appendIndexRows(&hdss, irkbss); err != nil {
		return
	}

	log.Debug(nil, "Will now write to ndb")
	err = l.svrUpdate(hkss, hvss, hdss)
	log.Debug(nil, "Write to ndb: DONE")
	return
}

func (l *backend) svrUpdate(putkeys [][]byte, putvalues [][]byte, delkeys [][]byte) (err error) {
	// keys and values must not be 0-len slices
	var xErr *C.slice_bytes_t
	var xPutKvs = make([]C.slice_bytes_t, len(putkeys)*2)
	var xDels = make([]C.slice_bytes_t, len(delkeys))
	for i := range putkeys {
		xPutKvs[i*2] = C.slice_bytes_t{len: C.size_t(len(putkeys[i])), v: (*C.char)(unsafe.Pointer(&putkeys[i][0]))}
		xPutKvs[(i*2)+1] = C.slice_bytes_t{len: C.size_t(len(putvalues[i])), v: (*C.char)(unsafe.Pointer(&putvalues[i][0]))}
	}
	for i := range delkeys {
		xDels[i] = C.slice_bytes_t{len: C.size_t(len(delkeys[i])), v: (*C.char)(unsafe.Pointer(&delkeys[i][0]))}
	}
	var xPutKvs0, xDels0 *C.slice_bytes_t
	if len(xPutKvs) > 0 {
		xPutKvs0 = &xPutKvs[0]
	}
	if len(xDels) > 0 {
		xDels0 = &xDels[0]
	}
	time0 := time.Now()
	xReqKey := C.ndb_update(l.db,
		xPutKvs0, C.size_t(len(xPutKvs)),
		xDels0, C.size_t(len(xDels)),
		xErr)
	defer C.ndb_release(&xReqKey, 1)
	log.Debug(nil, "C.ndb_update completed in %v", time.Since(time0))

	if xErr != nil {
		err = errorutil.String(C.GoStringN(xErr.v, C.int(xErr.len)))
		return
	}

	return
}

//----------------------------------------------------------

func (l *backend) nextLocalId(bs []byte) (nextid uint32, err error) {
	defer errorutil.OnError(&err)

	var xVal C.uint64_t
	var xErr *C.slice_bytes_t
	time0 := time.Now()
	xReqKey := C.ndb_incr_decr(l.db,
		C.slice_bytes_t{len: C.size_t(len(bs)), v: (*C.char)(unsafe.Pointer(&bs[0]))},
		C.bool(true),
		C.uint16_t(1),
		C.uint16_t(0),
		&xVal,
		xErr)
	defer C.ndb_release(&xReqKey, 1)
	log.Debug(nil, "C.ndb_incr_decr completed in %v", time.Since(time0))

	if xErr != nil {
		err = errorutil.String(C.GoStringN(xErr.v, C.int(xErr.len)))
		return
	}

	nextid = uint32(xVal)

	return
}

//------------------------------------------------------------

func bool2Byte(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func keyBytesForEntryType(kbs []byte, entryType uint8) (nbs []byte) {
	nbs = make([]byte, len(kbs))
	copy(nbs, kbs)
	//switch last 3 bits of last element
	nbs[len(kbs)-1] = (nbs[len(kbs)-1] & 0xf8) | entryType
	return
}

//------------------------------------------------------------

var openDbKeys []C.uint32_t

// these are not necessary to run, since ndb_init takes no parameters
// and is run by C.ndb_open automatically
const checkNoopSpeedInCAndGo = true
const runCNdbInit = false

func serverNoop1() {}
func serverNoop()  { serverNoop1() }

func SetupRpcServer(rpcsvr *rpc.Server, ss *Server, qlimitmax uint16, indexes ...*Index) (err error) {

	ti := readTypesIndexes(indexes...)

	var time0 time.Time

	if checkNoopSpeedInCAndGo {
		for i := 0; i < 4; i++ {
			log.Debug(nil, "====== Checking Noop Speed In C And Go: %d ======", i)
			time0 = time.Now()
			log.Debug(nil, "go <nothing> completed in %s", time.Since(time0))
			time0 = time.Now()
			serverNoop()
			log.Debug(nil, "go serverNoop() completed in %s", time.Since(time0))
			time0 = time.Now()
			C.ndb_noop()
			log.Debug(nil, "c ndb_noop() completed in %s", time.Since(time0))
		}
	}

	if runCNdbInit {
		time0 = time.Now()
		C.ndb_init()
		log.Debug(nil, "c ndb_init() completed in %s", time.Since(time0))
	}

	for i := uint16(0); i < ss.Shards.Num; i++ {
		shardId := i + uint16(ss.Shards.Min)
		var xDb *C.ndb_t
		var xErr *C.slice_bytes_t
		sName := "shard-" + strconv.Itoa(int(shardId))
		xdir := filepath.Join(ss.DataDir, sName)
		xdir, _ = filepath.Abs(xdir)

		time0 = time.Now()
		// cdir := C.slice_bytes_t{len: C.size_t(len(xdir)), v: (*C.char)(unsafe.Pointer(&xsb[0]))}
		xdirh := (*reflect.StringHeader)(unsafe.Pointer(&xdir))
		cdir := C.slice_bytes_t{len: C.size_t(len(xdir)), v: (*C.char)(unsafe.Pointer(xdirh.Data))}
		openDbKeys = append(openDbKeys, C.ndb_open(cdir, &xDb, xErr))
		log.Debug(nil, "C.ndb_open completed in %v", time.Since(time0))
		if xErr != nil {
			log.Error(nil, "Error opening ndb db: %s", C.GoStringN(xErr.v, C.int(xErr.len)))
			continue
		}

		// backendDbs = append(backendDbs, &xDb)
		log.Debug(nil, "Opened ndb db: %s", xdir)
		be := &backend{
			qlimitmax: qlimitmax,
			types:     ti,
			shardId:   shardId,
			db:        xDb,
		}
		rpcsvr.RegisterName(sName, rpcbackend{be})
	}

	return
}

// CloseBackends on ndb will close all opened backends.
// This should only be called by a close/exit handler.
// The services will still be registered in rpc, even though the backends have been closed.
func CloseBackends() (err error) {
	if len(openDbKeys) > 0 {
		C.ndb_release(&openDbKeys[0], C.size_t(len(openDbKeys)))
		openDbKeys = nil
	}
	return
}

//-------------------------------------------------
// Convenience methods for exposing c memory as an array
//-------------------------------------------------

// use as
// var theGoSlice []TheCType
// var theCArray *TheCType := C.getTheArray()
// c2goArray(unsafe.Pointer(&theGoSlice), unsafe.Pointer(&theCArray[0]), length)
func c2goArray(goSlice, cArray unsafe.Pointer, length int) {
	sliceHeader := (*reflect.SliceHeader)(goSlice)
	sliceHeader.Cap = length
	sliceHeader.Len = length
	sliceHeader.Data = uintptr(cArray)
	log.Debug(nil, "c2goArray: Data: 0x%x", sliceHeader.Data)
}

func c2goArrayBytes(c []C.slice_bytes_t) (s [][]byte) {
	s = make([][]byte, len(c))
	for i, v := range c {
		if v.len == 0 || v.v == nil {
			continue
		}
		sh := (*reflect.SliceHeader)(unsafe.Pointer(&s[i]))
		sh.Len = int(v.len)
		sh.Cap = sh.Len
		sh.Data = uintptr(unsafe.Pointer(v.v))
	}
	return
}

func c2goArrayString(c []C.slice_bytes_t) (s []string) {
	s = make([]string, len(c))
	for i, v := range c {
		if v.len == 0 || v.v == nil {
			continue
		}
		sh := (*reflect.StringHeader)(unsafe.Pointer(&s[i]))
		sh.Len = int(v.len)
		sh.Data = uintptr(unsafe.Pointer(v.v))
	}
	return
}
