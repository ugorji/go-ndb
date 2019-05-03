package ndb

import (
	"context"
	"math"
	"sort"

	//"fmt"
	"reflect"

	"github.com/ugorji/go-common/combinationgen"
	"github.com/ugorji/go-serverapp/db"
	"github.com/ugorji/go-common/printf"
)

// var ptrBitSize = uint8(reflect.TypeOf(uint(0)).Bits())

type indexRowNameValueGen struct {
	//vdprops []db.Property
	//mnames []string
	names []string
	props []interface{}
	cg    *combinationgen.T
	done  bool
}

//This is stored in the datastore as the index information.
type dbIndex struct {
	P []string
	V []interface{}
}

func newDbIndex(ctxId context.Context, vdprops []db.Property) (li *dbIndex) {
	li = new(dbIndex)
	log.Debug(ctxId, "indexRowNameValueGen: vdprops: %#v", vdprops)

	var kmult []string
	var kall []string
	for _, kv9 := range vdprops {
		log.Debug(ctxId, "Looking for multiples: found: %v", kv9.Name)
		if i9, v9 := li.get(kv9.Name); v9 != nil {
			if v9s, ok2 := v9.([]interface{}); ok2 {
				v9s = append(v9s, kv9.Value)
			} else {
				kmult = append(kmult, kv9.Name)
				v9s = []interface{}{v9, kv9.Value}
				li.V[i9] = v9s
			}
		} else {
			li.V = append(li.V, kv9.Value)
			li.P = append(li.P, kv9.Name)
			kall = append(kall, kv9.Name)
		}
	}

	log.Debug(ctxId, "indexRowNameValueGen: li: %#v", li)
	sort.StringSlice(kall).Sort()
	kmultss := sort.StringSlice(kmult)
	kmultss.Sort()
	log.Debug(ctxId, "indexRowNameValueGen: kmult: %#v", kmult)
	log.Debug(ctxId, "indexRowNameValueGen: kall: %#v", kall)

	knames := make([]string, len(kall))
	kprops := make([]interface{}, len(kall))
	i := 0
	for _, nk := range kall {
		if j := kmultss.Search(nk); !(j < len(kmultss) && kmultss[j] == nk) {
			for _, nn := range vdprops {
				if nn.Name == nk {
					knames[i] = nk
					_, kprops[i] = li.get(nk)
					i++
					break
				}
			}
		}
	}
	for _, nk := range kmultss {
		knames[i] = nk
		_, kprops[i] = li.get(nk)
		i++
	}
	li.P = knames
	li.V = kprops
	return
}

func (li *dbIndex) get(k string) (i int, v interface{}) {
	i = -1
	for i0, k0 := range li.P {
		if k == k0 {
			return i0, li.V[i0]
		}
	}
	return
}

func (li *dbIndex) numSingles() (i int) {
	for i = 0; i < len(li.V); i++ {
		if _, ok := li.V[i].([]interface{}); ok {
			return
		}
	}
	return
}

func (li *dbIndex) subset(ctxId context.Context, c *Index) (li2 *dbIndex) {
	log.Debug(ctxId, "indexRowNameValueGen: indexprops: %s", printf.ValuePrintfer{c.Properties})
	li2 = new(dbIndex)
	for _, cp := range c.Properties {
		i, v := li.get(cp.Name)
		li2.P = append(li2.P, li.P[i])
		li2.V = append(li2.V, v)
	}
	return
}

func (li *dbIndex) newIndexRowNameValueGen(ctxId context.Context) (irnvg *indexRowNameValueGen) {
	var cg *combinationgen.T
	ns := li.numSingles()
	kprops := make([]interface{}, len(li.V))
	copy(kprops, li.V)
	if ns < len(li.V) {
		combo := make([][]interface{}, len(li.V)-ns)
		for i, j := 0, ns; j < len(li.V); i, j = i+1, j+1 {
			combo[i] = li.V[j].([]interface{})
			kprops[j] = combo[i][0]
		}
		cg, _ = combinationgen.New(kprops[ns:], combo)
	}
	irnvg = &indexRowNameValueGen{
		names: li.P,
		props: kprops,
		cg:    cg,
	}
	log.Debug(ctxId, "indexRowNameValueGen: 9. %#v", irnvg)
	return
}

func getIndexes(ctxId context.Context, cfg *typeInfo, c byte, ikind uint8, kind string, propnames []string) (
	ty *Type, tyins []*Index) {
	var indexNames []string
	for _, ty0 := range cfg.Types {
		if ikind != 0 && ty0.KindId != ikind {
			continue
		}
		if kind != "" && ty0.Kind != kind {
			continue
		}
		ty = ty0
		break
	}
	if ty == nil {
		return
	}
	kind = ty.Kind
	for _, tyin3 := range cfg.Indexes {
		if tyin3.Kind != kind {
			continue
		}
		if c == 's' && len(tyin3.Properties) > len(propnames) {
			continue
		}
		if c == 'q' && len(tyin3.Properties) != len(propnames) {
			continue
		}
		matched := false
		if len(tyin3.Properties) == 0 {
			matched = true
		} else {
			for _, inpr4 := range tyin3.Properties {
				matched = false
				for _, pname := range propnames {
					if pname == inpr4.Name {
						matched = true
						break
					}
				}
				if !matched {
					break
				}
			}
		}
		if matched {
			tyins = append(tyins, tyin3)
			indexNames = append(indexNames, tyin3.Name)
			if c == 'q' {
				break
			}
		}
	}
	log.Debug(ctxId, "(%q) For kind: %v, PropNames: %v, Returning Indexes: %v", c, kind, propnames, indexNames)
	return
}

func indexRowBytes(ctxId context.Context, tyin3 *Index, ikeybs []byte, ikind uint8,
	propnames []string, propvals []interface{},
) (irbs5 []byte) {
	ipfx := uint8(D_INDEX) << 4
	if ikeybs != nil {
		log.Debug(ctxId, ">>>>>>>> indexRowBytes: Adding index with prefix: %v, from ", ipfx)
	}
	// Cannot use this, because the prefix is used to do a scan.
	// switch len(ikeybs) {
	// case 8:
	// case 16:
	// 	ipfx = ipfx | 1
	// default:
	// 	panic(fmt.Errorf("indexRowBytes: Key Len (%v) must be 8 or 16 representing 0 or 1 parent.", len(ikeybs)))
	// }
	irbs5 = make([]byte, 0, 8+len(ikeybs)+(len(propnames)*len(tyin3.Properties)))
	irbs5 = append(irbs5, ipfx, ikind, tyin3.Id)
	for _, inpr4 := range tyin3.Properties {
		for i, pname := range propnames {
			if pname != inpr4.Name {
				continue
			}
			pvalue := propvals[i]
			switch inpr4.ReflectKind {
			case reflect.Bool:
				if pvalue.(bool) {
					irbs5 = append(irbs5, byte(1))
				} else {
					irbs5 = append(irbs5, byte(0))
				}
			case reflect.Uint8:
				irbs5 = append(irbs5, pvalue.(byte))
			case reflect.Int8:
				irbs5 = append(irbs5, byte(pvalue.(int8)))
			case reflect.Int16:
				irbs5 = append(irbs5, 0, 0)
				Benc.PutUint16(irbs5[len(irbs5)-2:], uint16(pvalue.(int16)))
			case reflect.Int32:
				irbs5 = append(irbs5, 0, 0, 0, 0)
				Benc.PutUint32(irbs5[len(irbs5)-4:], uint32(pvalue.(int32)))
			case reflect.Int64:
				irbs5 = append(irbs5, 0, 0, 0, 0, 0, 0, 0, 0)
				Benc.PutUint64(irbs5[len(irbs5)-8:], uint64(pvalue.(int64)))
			case reflect.Int:
				irbs5 = append(irbs5, 0, 0, 0, 0, 0, 0, 0, 0)
				Benc.PutUint64(irbs5[len(irbs5)-8:], uint64(pvalue.(int)))
			case reflect.Uint16:
				irbs5 = append(irbs5, 0, 0)
				Benc.PutUint16(irbs5[len(irbs5)-2:], pvalue.(uint16))
			case reflect.Uint32:
				irbs5 = append(irbs5, 0, 0, 0, 0)
				Benc.PutUint32(irbs5[len(irbs5)-4:], pvalue.(uint32))
			case reflect.Uint64:
				irbs5 = append(irbs5, 0, 0, 0, 0, 0, 0, 0, 0)
				Benc.PutUint64(irbs5[len(irbs5)-8:], pvalue.(uint64))
			case reflect.Uint:
				irbs5 = append(irbs5, 0, 0, 0, 0, 0, 0, 0, 0)
				Benc.PutUint64(irbs5[len(irbs5)-8:], uint64(pvalue.(uint)))
			case reflect.Uintptr:
				irbs5 = append(irbs5, 0, 0, 0, 0, 0, 0, 0, 0)
				Benc.PutUint64(irbs5[len(irbs5)-8:], uint64(pvalue.(uintptr)))
			case reflect.Float32:
				irbs5 = append(irbs5, 0, 0, 0, 0)
				Benc.PutUint32(irbs5[len(irbs5)-4:], math.Float32bits(pvalue.(float32)))
			case reflect.Float64:
				irbs5 = append(irbs5, 0, 0, 0, 0, 0, 0, 0, 0)
				Benc.PutUint64(irbs5[len(irbs5)-8:], math.Float64bits(pvalue.(float64)))
			case reflect.String:
				irbs5 = append(irbs5, []byte(pvalue.(string))...)
				irbs5 = append(irbs5, byte(0))
			}
			break
		}
	}
	if ikeybs != nil {
		irbs5 = append(irbs5, byte(0))
		irbs5 = append(irbs5, ikeybs...)
	}
	log.Debug(ctxId, "IndexRowBytes For Index: %v, Kind: %v, Props: %v, Values: %v. Returning: %v",
		tyin3.Name, tyin3.Kind, propnames, propvals, irbs5)
	return
}

//------------------------------------------------------------

func (x *indexRowNameValueGen) first() (n []interface{}) {
	if len(x.props) == 0 {
		n = []interface{}{}
		x.done = true
	} else {
		n = x.props
		if x.cg == nil {
			x.done = true
		} else {
			x.cg.First()
		}
	}
	return
}

func (x *indexRowNameValueGen) next() (n []interface{}) {
	if x.done {
		return
	}
	if err := x.cg.Next(); err != nil {
		x.done = true
		return
	}
	n = x.props
	return
}

func (x *indexRowNameValueGen) pnames() []string {
	return x.names
}
