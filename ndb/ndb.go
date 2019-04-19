package ndb

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/ugorji/go-common/app"
	"github.com/ugorji/go-common/db"
	"github.com/ugorji/go-common/logging"
	"github.com/ugorji/go-common/zerror"
)

//Entry Types
const (
	_ = iota
	E_METADATA
	E_DATA
	E_INDEXROW
)

//Discriminators
const (
	_ = iota
	D_INDEX
	D_ENTITY
	D_IDGEN
)

// MUST USE big-endian, because representation of a key in bytes assumes big endian, etc.

// TODO: Use better sipHash keys
const (
	sipHashK0 uint64 = 17
	sipHashK1        = 23
)

var (
	Base64Enc = base64.URLEncoding
	Benc      = binary.BigEndian //binary.ByteOrder
	// Rand has the same seed for consistency.
	Rand = rand.New(rand.NewSource(1 << 31))
	// Rand = rand.New(rand.NewSource(int64(time.Now().Nanosecond())))

	// decOpts = msgpack.NewDecoderOptions()
	// encOpts = msgpack.NewEncoderOptions()
)

type Key struct {
	V      uint64
	Parent *Key
	//use exported fields so that it can easily be encoded in gob/json/etc
}

type EntityIdParts struct {
	Shard uint16
	Local uint32
}

type KeyParts struct {
	EntityIdParts
	Discrim uint8
	Kind    uint8
	Shape   uint8
	Entry   uint8
}

func (x KeyParts) V() (ikey uint64) {
	ikey = uint64(x.Entry) |
		(uint64(x.Shape) << 3) |
		(uint64(x.Kind) << 8) |
		(uint64(x.Local) << 16) |
		(uint64(x.Shard) << 48) |
		(uint64(x.Discrim) << 60)
	return
}

func (x EntityIdParts) EntityId() int64 {
	return int64((uint64(x.Local) << 16) | (uint64(x.Shard) << 48))
}

type Range struct {
	Min, Num uint16
}

type Blob struct {
	BaseDir string
	L1      Range
	L2      Range
}

type Server struct {
	Name    string
	Addr    string
	DataDir string
	Shards  Range
	Indexes []uint8
}

type Cfg struct {
	//These are passed in on command line (so we can have multiple backends)
	//ListenAddress string
	//TLSListenAddress string
	Blob             Blob
	NewEntityShardId Range
	Servers          []Server
	QueryLimitMax    uint16
}

type Type struct {
	KindId  uint8
	ShapeId uint8
	Kind    string
	Shape   string
	tm      *db.TypeMeta
}

type IndexProp struct {
	Name        string
	Type        string
	ReflectKind reflect.Kind
}

type Index struct {
	Id         uint8
	Name       string
	Kind       string
	KindId     uint8
	Properties []*IndexProp
}

type typeInfo struct {
	Types   []*Type
	Indexes []*Index
}

type NewIdArgs struct {
	IshardId uint16
	Ikind    uint8
}

type GetArgs struct {
	Keys          []*Key
	ErrIfNotFound bool
}

type KV struct {
	K *Key
	V []byte
}

type PutArgs struct {
	Keys   []*Key
	Values [][]byte
	Props  []*db.PropertyList
}

// Gets are not atomic. So one potential error per Result.
type GetResult struct {
	Values [][]byte
	Errors []string
}

type QueryArgs struct {
	Kind      string
	Opts      *app.QueryOpts
	Filters   []*app.QueryFilter
	ParentKey *Key
}

type QueryIterResult struct {
	Rows [][]byte
	err  error
}

func (qs QueryIterResult) IterFn() func() (k app.Key, cursor string, err error) {
	i := 0
	var iterFn func() (app.Key, string, error)
	iterFn = func() (ak3 app.Key, lastCursor3 string, err3 error) {
		//fmt.Printf(" - \n")
		if qs.err != nil {
			err3 = qs.err
			return
		}
		if i == len(qs.Rows) {
			err3 = io.EOF
			qs.err = err3
			return
		}
		if qs.Rows[i] == nil {
			i++
			return iterFn()
		}
		var k *Key
		if k, err3 = ParseKeyBytes(qs.Rows[i]); err3 != nil {
			qs.err = err3
			return
		}
		ak3 = k
		lastCursor3 = Base64Enc.EncodeToString(qs.Rows[i])
		i++
		return
	}
	return iterFn
}

func (k Key) Incomplete() bool {
	//return k.V <= 0
	return k.V <= 0 || k.EntityId() <= 0
}

func (k Key) EntityId() int64 {
	return GetKeyParts(k.V).EntityId()
}

func (k Key) Uints(forward bool) (ks []uint64) {
	ks = append(ks, k.V)
	for pk := k.Parent; pk != nil; pk = pk.Parent {
		ks = append(ks, pk.V)
	}
	if forward {
		//reverse slice
		for i, j := 0, len(ks)-1; i < j; i, j = i+1, j-1 {
			ks[i], ks[j] = ks[j], ks[i]
		}
	}
	return
}

// Bytes of the Key, with ancestors first
func (k Key) Bytes() (bs []byte) {
	ru := k.Uints(true)
	lenk := len(ru)
	bs = make([]byte, lenk*8)
	for i := 0; i < lenk; i++ {
		Benc.PutUint64(bs[i*8:(i+1)*8], ru[i])
	}
	return
}

func (k Key) String() string {
	return fmt.Sprintf("%v", k.Uints(true))
}

func (k Key) Encode() (s string) {
	ru := k.Uints(true)
	klen := len(ru)
	switch klen {
	case 0:
	case 1:
		s = strconv.FormatUint(ru[0], 10)
	default:
		ss := make([]string, klen)
		for i := 0; i < klen; i++ {
			ss[i] = strconv.FormatUint(ru[i], 10)
		}
		s = strings.Join(ss, "-")
	}
	return
}

func ParseKeyBytes(bs []byte) (k *Key, err error) {
	nelem := len(bs) / 8
	nrem := len(bs) % 8
	if nelem == 0 || nrem != 0 {
		err = fmt.Errorf("Invalid bytes for ndb Key. Must be exact multiple of 8 and > 8.: Len: %d", len(bs))
		return
	}
	uu := make([]uint64, nelem)
	for i := 0; i < nelem; i++ {
		uu[i] = Benc.Uint64(bs[i*8 : (i+1)*8])
	}
	return NewKey(uu...)
}

func DecodeKey(s string) (k *Key, err error) {
	ss := strings.Split(s, "-")
	uu := make([]uint64, len(ss))
	for i := 0; i < len(ss); i++ {
		if uu[i], err = strconv.ParseUint(s, 10, 64); err != nil {
			return
		}
	}
	return NewKey(uu...)
}

func NewKey(uu ...uint64) (k *Key, err error) {
	//No need to validate when we initialize a key. We validate when we PUT.
	//for i := len(uu)-1; i >= 0; i-- {
	for i := 0; i < len(uu); i++ {
		k = &Key{V: uu[i], Parent: k}
	}
	return
}

func GetEntityIdParts(v uint64) (x EntityIdParts) {
	x.Shard = uint16((v << 4) >> 52)
	x.Local = uint32((v << 16) >> 32)
	return
}

func GetKeyParts(ikey uint64) (x KeyParts) {
	x.Discrim = uint8(ikey >> 60)
	x.Shard = uint16((ikey << 4) >> 52)
	x.Local = uint32((ikey << 16) >> 32)
	x.Kind = uint8((ikey << 48) >> 56)
	x.Shape = uint8((ikey << 56) >> 59)
	x.Entry = uint8((ikey << 61) >> 61)
	return
}

//validate keys to ensure they are valid and appropriate.
//all components must be valid (else err).
//  - k.Local must > 0 and <= current locid (not necessary)
//  - k.Shard must be a valid shard
//  - k.Kind and k.Shape must match passed in typemeta of dst
//    (except dst is a primitive type, and if so, itype must be in reserved range).
//  - K.Discrim must be ENTITY
//  - k.Entry must be DATA
func ValidateEntityData(ctxId interface{}, dst interface{}, k KeyParts) (err error) {
	defer zerror.OnErrorf(1, &err, nil)
	logging.Trace(ctxId, "Validating: KeyParts: %#v", k)
	fnErr := func(tag string) {
		err = fmt.Errorf("%sInvalid Key: %#v", tag, k)
	}
	if k.Local == 0 || k.Discrim != D_ENTITY || k.Entry != E_DATA {
		fnErr("1. ")
		return
	}
	if dst == nil {
		return
	}
	rv := reflect.ValueOf(dst)
	var rvk reflect.Kind
	for {
		rvk = rv.Kind()
		if rvk == reflect.Ptr || rvk == reflect.Interface {
			rv = rv.Elem()
			continue
		}
		break
	}
	if rvk == reflect.Struct {
		tm, err2 := db.GetStructMeta(dst)
		if err2 != nil {
			err = err2
			return
		}
		if tm.KindId != k.Kind && tm.ShapeId != k.Shape {
			fnErr("3. ")
			return
		}
	} else {
		if k.Kind > 32 {
			fnErr("4. ")
			return
		}
	}
	return
}

func KindAndShapeId(types []*Type, kind, shape string) (ikind uint8, ishp uint8) {
	for _, ty := range types {
		if ty.Kind == kind && ty.Shape == shape {
			ikind, ishp = ty.KindId, ty.ShapeId
			break
		}
	}
	return
}

func UpdateEntityIdFromKey(d interface{}, intId int64) (err error) {
	defer zerror.OnErrorf(1, &err, nil)
	tm, err := db.GetStructMeta(d)
	if err != nil {
		return
	}
	rv := reflect.ValueOf(d).Elem()
	if rvk := rv.Kind(); rvk != reflect.Ptr {
		err = fmt.Errorf("Argument to UpdateEntityIdFromKey must be a pointer. Got: %v", rvk)
		return
	}
	f := rv.FieldByName(tm.KeyField)
	if !f.IsValid() {
		err = fmt.Errorf("No Field Named: %v found in %v", tm.KeyField, d)
		return
	}
	f.SetInt(intId) //f.Set(reflect.ValueOf(key.IntID()))
	return
}

// func Marshal(out *[]byte, v interface{}) (err error) {
// 	defer zerror.OnErrorf(1, &err, nil)
// 	return db.Codec.EncodeBytes(out, v)
// 	// return codec.NewEncoderBytes(out, encOpts).Encode(v)
// }

// func Unmarshal(in []byte, v interface{}) (err error) {
// 	defer zerror.OnErrorf(1, &err, nil)
// 	return db.Codec.DecodeBytes(in, v)
// 	// return msgpack.NewDecoderBytes(in, decOpts).Decode(v)
// }

func ReadConfig(cfgFile string) (cfg *Cfg, err error) {
	defer zerror.OnErrorf(1, &err, nil)
	if cfgFile == "" {
		cfgFile = "ndb.json"
	}
	f, err := os.Open(cfgFile)
	if err != nil {
		return
	}
	defer f.Close()
	cfg = new(Cfg)
	err = json.NewDecoder(f).Decode(cfg)
	return
}

func indexName(buf *bytes.Buffer, ci *Index, inclPropType bool) {
	buf.Reset()
	buf.WriteString(ci.Kind)
	if len(ci.Properties) == 0 {
		buf.WriteByte('_')
		buf.WriteByte('0')
	} else {
		for _, cip := range ci.Properties {
			buf.WriteByte('_')
			buf.WriteString(cip.Name)
			if inclPropType {
				buf.WriteByte('_')
				buf.WriteString(cip.Type)
			}
		}
	}
}

func readTypes() (v []*Type) {
	z := db.StructMetas.GetAll()
	for i := range z {
		tm := z[i][1].(*db.TypeMeta)
		cfgt := &Type{
			KindId:  tm.KindId,
			Kind:    tm.Kind,
			ShapeId: tm.ShapeId,
			Shape:   tm.Shape,
			tm:      tm,
		}
		v = append(v, cfgt)
	}
	return
}

func readTypesIndexes(indexes ...*Index) (t *typeInfo) {
	t = &typeInfo{Indexes: indexes}
	t.Types = readTypes()

	// for each index, sets the Id, Name, KindId and ReflectKinds if not set.
	var buf bytes.Buffer

	for i, ci := range indexes {
		if ci.Name == "" {
			indexName(&buf, ci, false)
			ci.Name = buf.String()
		}
		if ci.Id == 0 {
			// indexName(&buf, ci, true)
			// ci.Id = util.SipHash24(sipHashK0, sipHashK1, buf.Bytes())
			ci.Id = 101 + uint8(i) // TODO: Hacky
		}
		if ci.KindId == 0 {
			for _, ty := range t.Types {
				if ty.Kind == ci.Kind {
					ci.KindId = ty.KindId
					break
				}
			}
		}
		for _, cip := range ci.Properties {
			switch cip.Type {
			case "bool":
				cip.ReflectKind = reflect.Bool
			case "int8":
				cip.ReflectKind = reflect.Int8
			case "int16":
				cip.ReflectKind = reflect.Int16
			case "int32":
				cip.ReflectKind = reflect.Int32
			case "int64":
				cip.ReflectKind = reflect.Int64
			case "byte", "uint8":
				cip.ReflectKind = reflect.Uint8
			case "uint16":
				cip.ReflectKind = reflect.Uint16
			case "uint32":
				cip.ReflectKind = reflect.Uint32
			case "uint64":
				cip.ReflectKind = reflect.Uint64
			case "uintptr":
				cip.ReflectKind = reflect.Uintptr
			case "float32":
				cip.ReflectKind = reflect.Float32
			case "float64":
				cip.ReflectKind = reflect.Float64
			case "string":
				cip.ReflectKind = reflect.String
			}
		}
	}
	return
}

func (cfg *Cfg) FindServer(name string) *Server {
	for i := range cfg.Servers {
		if cfg.Servers[i].Name == name {
			return &cfg.Servers[i]
		}
	}
	return nil
}
