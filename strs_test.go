package minidb

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"math"
	"math/rand"
	"minidb/logger"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestMiniDB_Get(t *testing.T) {

	t.Run("default", func(t *testing.T) {
		testMiniDBGet(t, FileIO, KeyOnlyMemMode)

	})
	t.Run("mmp", func(t *testing.T) {
		testMiniDBGet(t, MMap, KeyValueMemMode)

	})
	t.Run("kv-mode", func(t *testing.T) {
		testMiniDBGet(t, FileIO, KeyValueMemMode)
	})

}
func TestMiniDB_Set(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testMiniDBSet(t, FileIO, KeyOnlyMemMode)

	})
	t.Run("mmp", func(t *testing.T) {
		testMiniDBSet(t, MMap, KeyValueMemMode)

	})
	t.Run("kv-mode", func(t *testing.T) {
		testMiniDBSet(t, FileIO, KeyValueMemMode)
	})
}
func TestMiniDB_Set_logFileThreshold(t *testing.T) {
	path := filepath.Join("/tmp", "miniDB")
	opts := DefaultOptions(path)
	opts.IoType = MMap
	opts.LogFileSizeThreshold = 32 << 20
	db, err := Open(opts)
	assert.Nil(t, err)
	for i := 0; i < 600000; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}
}
func TestMiniDB_Get_logFileThreshold(t *testing.T) {
	path := filepath.Join("/tmp", "miniDB")
	opts := DefaultOptions(path)
	opts.IoType = MMap
	opts.LogFileSizeThreshold = 32 << 20
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	writeCount := 600000
	for i := 0; i < writeCount; i++ {
		err := db.Set(GetKey(i), GetValue128B())
		assert.Nil(t, err)
	}

	rand.Seed(time.Now().Unix())
	for i := 0; i < 10000; i++ {
		key := GetKey(rand.Intn(writeCount))
		val, err := db.Get(key)
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}

}

func testMiniDBSet(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	minidb, err := Open(options)
	defer destroyDB(minidb)
	assert.Nil(t, err)
	type args struct {
		key   []byte
		value []byte
	}
	tests := []struct {
		name    string
		db      *MiniDB
		args    args
		wantWrr bool
	}{
		{
			"nil-key", minidb, args{key: nil, value: []byte("val-1")}, false,
		},
		{
			"nil-val", minidb, args{key: []byte("key-1"), value: nil}, false,
		},
		{
			"mormal", minidb, args{key: []byte("hello"), value: []byte("nihao")}, false,
		},
	}
	for _, te := range tests {
		t.Run(te.name, func(t *testing.T) {
			if err := te.db.Set(te.args.key, te.args.value); (err != nil) != te.wantWrr {
				t.Errorf("set() err : %v wantErr：%v", err, te.wantWrr)
			}

		})

	}

}

func testMiniDBGet(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode
	miniDB, err := Open(options)
	defer destroyDB(miniDB)
	assert.Nil(t, err)
	_ = miniDB.Set(nil, []byte("value-nil"))
	_ = miniDB.Set([]byte("k1"), []byte("value-1"))
	_ = miniDB.Set([]byte("k2"), []byte("value-2"))
	_ = miniDB.Set([]byte("k3"), []byte("value-3"))
	_ = miniDB.Set([]byte("k4"), []byte("value-4"))
	_ = miniDB.Set([]byte("k4"), []byte("value-rewrite"))
	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		db      *MiniDB
		args    args
		want    []byte
		wantErr bool
	}{
		{
			"nil-key", miniDB, args{key: nil}, nil, true,
		},
		{
			"mormal", miniDB, args{key: []byte("k2")}, []byte("value-2"), false,
		},
		{
			"rewrite", miniDB, args{key: []byte("k4")}, []byte("value-rewrite"), false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			get, err := tt.db.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("get() err:%v wantErr :%v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(get, tt.want) {
				t.Errorf("get() got:%v want :%v", get, tt.want)

			}

		})
	}

}
func TestMiniDB_MGet(t *testing.T) {

	t.Run("default", func(t *testing.T) {
		testMiniDBMGet(t, FileIO, KeyOnlyMemMode)

	})
	t.Run("mmp", func(t *testing.T) {
		testMiniDBMGet(t, MMap, KeyValueMemMode)

	})
	t.Run("kv-mode", func(t *testing.T) {
		testMiniDBMGet(t, FileIO, KeyValueMemMode)
	})
}

func testMiniDBMGet(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	miniDB, err := Open(options)

	defer destroyDB(miniDB)
	assert.Nil(t, err)
	_ = miniDB.Set(nil, []byte("value-nil"))
	_ = miniDB.Set([]byte("k1"), []byte("value-1"))
	_ = miniDB.Set([]byte("k2"), []byte("value-2"))
	_ = miniDB.Set([]byte("k3"), []byte("value-3"))
	_ = miniDB.Set([]byte("k4"), []byte("value-4"))
	_ = miniDB.Set([]byte("k4"), []byte("value-rewrite"))
	type args struct {
		keys [][]byte
	}
	tests := []struct {
		name    string
		db      *MiniDB
		args    args
		want    [][]byte
		wantErr bool
	}{
		{
			"nil", miniDB, args{[][]byte{nil}}, [][]byte{nil}, false,
		},
		{
			"normal", miniDB, args{[][]byte{[]byte("k1")}}, [][]byte{[]byte("value-1")}, false,
		},
		{
			"normal-rewrite", miniDB, args{[][]byte{[]byte("k1"), []byte("k4")}}, [][]byte{[]byte("value-1"), []byte("value-rewrite")}, false,
		},
		{
			"multiple-key", miniDB, args{[][]byte{[]byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")}}, [][]byte{[]byte("value-1"), []byte("value-2"), []byte("value-3"), []byte("value-rewrite")}, false,
		},
		{
			"missed one key", miniDB, args{keys: [][]byte{[]byte("missed key")}}, [][]byte{nil}, false,
		},
		{
			"missed multiple keys", miniDB, args{[][]byte{[]byte("missed-key1"), []byte("missed-key1"), []byte("missed-key1")}}, [][]byte{nil, nil, nil}, false,
		},
		{
			"missed one key in multiple keys", miniDB, args{[][]byte{[]byte("k1"), []byte("missed-key"), []byte("k2")}}, [][]byte{[]byte("value-1"), nil, []byte("value-2")}, false,
		},
		{
			"nil key in multiple keys", miniDB, args{[][]byte{nil, []byte("k1"), []byte("k2")}}, [][]byte{nil, []byte("value-1"), []byte("value-2")}, false,
		},
		{
			"empty key", miniDB, args{[][]byte{}}, nil, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			get, err := tt.db.MGet(tt.args.keys)
			if (err != nil) != tt.wantErr {
				t.Errorf("MGet() err %v wantErr:%v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(get, tt.want) {
				t.Errorf("MGet() get:%v want:%v", get, tt.want)
			}
		})
	}

}
func TestMiniDB_MSet(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testMinDBMSet(t, FileIO, KeyOnlyMemMode)

	})
	t.Run("mmp", func(t *testing.T) {
		testMinDBMSet(t, MMap, KeyValueMemMode)

	})
	t.Run("kv-mode", func(t *testing.T) {
		testMinDBMSet(t, FileIO, KeyValueMemMode)
	})
}

func testMinDBMSet(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode
	miniDB, err := Open(options)
	defer destroyDB(miniDB)
	assert.Nil(t, err)
	tests := []struct {
		name    string
		db      *MiniDB
		args    [][]byte
		wantErr bool
	}{
		{
			"nil-key", miniDB, [][]byte{nil, []byte("hello")}, false,
		},
		{
			"nil-value", miniDB, [][]byte{[]byte("key1"), nil}, false,
		},
		{
			"empty pair", miniDB, [][]byte{}, true,
		},
		{
			"normal-one-pair", miniDB, [][]byte{[]byte("k1"), []byte("v1")}, false,
		},
		{
			"multiple-pair", miniDB, [][]byte{[]byte("k1"), []byte("v1"), []byte("k2"), []byte("v2"), []byte("k3"), []byte("v4")}, false,
		},
		{
			"wrong number of key value ", miniDB, [][]byte{[]byte("k1"), []byte("v1"), []byte("k2")}, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.MSet(tt.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("MSet() err:%v ,wannErr：%v", err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, ErrWrongNumberOfArgs) {
				t.Errorf("MSet() err:%v", err)
			}

		})
	}

}

func TestMiniDB_SetEX(t *testing.T) {
	t.Run("key-only", func(t *testing.T) {
		testMiniDBSetEX(t, KeyOnlyMemMode)
	})
	t.Run("key-value", func(t *testing.T) {
		testMiniDBSetEX(t, KeyValueMemMode)
	})

}
func testMiniDBSetEX(t *testing.T, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IndexMode = mode
	miniDB, err := Open(options)
	defer destroyDB(miniDB)
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		err := miniDB.SetEX(GetValue(i), GetValue128B(), time.Millisecond*200)
		assert.Nil(t, err)
		time.Sleep(time.Millisecond * 210)
		get, err := miniDB.Get(GetValue(i))
		assert.Equal(t, 0, len(get))
		assert.Equal(t, err, ErrKeyNotFound)
	}
}

func TestMiniDB_SetNX(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testMiniDBSetNX(t, FileIO, KeyOnlyMemMode)

	})
	t.Run("mmp", func(t *testing.T) {
		testMiniDBSetNX(t, MMap, KeyValueMemMode)

	})
	t.Run("kv-mode", func(t *testing.T) {
		testMiniDBSetNX(t, FileIO, KeyValueMemMode)
	})
}
func testMiniDBSetNX(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	miniDB, err := Open(options)

	defer destroyDB(miniDB)
	assert.Nil(t, err)
	type args struct {
		key     []byte
		value   []byte
		wantErr bool
	}
	tests := []struct {
		name string
		db   *MiniDB
		args []args
	}{
		{
			"nil key", miniDB, []args{{nil, []byte("nil value"), false}},
		},
		{
			"nil value", miniDB, []args{
				{[]byte("nil key"), nil, false},
			},
		},
		{
			"not exist in db", miniDB, []args{
				{[]byte("k1"), []byte("v1"), false},
			},
		},
		{
			"exist in db", miniDB,
			[]args{
				{[]byte("k1"), []byte("v2"), false},
				{[]byte("k1"), []byte("v2"), false},
			},
		},
		{
			"multiple key value not exist in db", miniDB,
			[]args{
				{[]byte("k1"), []byte("k1"), false},
				{[]byte("k2"), []byte("k2"), false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, arg := range tt.args {
				if err := tt.db.SetNX(arg.key, arg.value); (err != nil) != arg.wantErr {
					t.Errorf("SetNX() err :%v wantErr:%v", err, arg.wantErr)
				}
			}
		})
	}
}
func TestMiniDB_MSetNX(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testMiniDBMSetNX(t, FileIO, KeyOnlyMemMode)

	})
	t.Run("mmp", func(t *testing.T) {
		testMiniDBMSetNX(t, MMap, KeyValueMemMode)

	})
	t.Run("kv-mode", func(t *testing.T) {
		testMiniDBMSetNX(t, FileIO, KeyValueMemMode)
	})
}
func testMiniDBMSetNX(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	miniDB, err := Open(options)

	defer destroyDB(miniDB)
	assert.Nil(t, err)
	_ = miniDB.Set([]byte("key10"), []byte("value10"))
	tests := []struct {
		name              string
		db                *MiniDB
		args              [][]byte
		expDuplicateKey   []byte
		expDuplicateValue []byte
		wantErr           bool
	}{
		{name: "nil key", db: miniDB, args: [][]byte{nil, []byte("nil val")}, expDuplicateKey: nil, expDuplicateValue: nil, wantErr: false},
		{name: "nil value", db: miniDB, args: [][]byte{[]byte("nil key"), nil}, expDuplicateKey: nil, expDuplicateValue: nil, wantErr: false},
		{name: "empty pair", db: miniDB, args: [][]byte{}, expDuplicateKey: nil, expDuplicateValue: nil, wantErr: true},
		{name: "one pair", db: miniDB, args: [][]byte{[]byte("k1"), []byte("v1")}, expDuplicateKey: nil, expDuplicateValue: nil, wantErr: false},
		{name: "multiple pair-no duplicate key", db: miniDB, args: [][]byte{[]byte("k1"), []byte("v1"), []byte("k2"), []byte("v2")}, expDuplicateKey: nil, expDuplicateValue: nil, wantErr: false},
		{name: "multiple pair-with duplicate key", db: miniDB, args: [][]byte{
			[]byte("k3"), []byte("v3"),

			[]byte("k12"), []byte("v2"),
			[]byte("k12"), []byte("v3"),
		}, expDuplicateKey: []byte("k12"), expDuplicateValue: []byte("v2"), wantErr: false},
		{name: "multiple pair-already exist", db: miniDB, args: [][]byte{
			[]byte("k4"), []byte("v4"),
			[]byte("k6"), []byte("v6"),
			[]byte("key10"), []byte("value10"),
		}, expDuplicateKey: []byte("key10"), expDuplicateValue: []byte("value10"), wantErr: false},
		{name: "wrong number of key-value ", db: miniDB, args: [][]byte{
			[]byte("k1"), []byte("v1"),
			[]byte("k2"), []byte("v2"),
			[]byte("key10"),
		}, expDuplicateKey: nil, expDuplicateValue: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.MSetNX(tt.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("MSetNX() err:%v, wantErr:%v", err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrWrongNumberOfArgs) {
				t.Errorf("MSetNX() err:%v, wantErr:%v", err, ErrWrongNumberOfArgs)
			}
			if tt.expDuplicateValue != nil {
				get, _ := tt.db.Get(tt.expDuplicateKey)
				if !bytes.Equal(get, tt.expDuplicateValue) {
					t.Errorf("expected duplicate value = %v got= %v", string(tt.expDuplicateValue), string(get))
				}
			}
		})
	}
}
func TestMiniDB_Append(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testMiniDBAppend(t, FileIO, KeyOnlyMemMode)

	})
	t.Run("mmp", func(t *testing.T) {
		testMiniDBAppend(t, MMap, KeyValueMemMode)

	})
	t.Run("kv-mode", func(t *testing.T) {
		testMiniDBAppend(t, FileIO, KeyValueMemMode)
	})
}
func testMiniDBAppend(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	miniDB, err := Open(options)
	defer destroyDB(miniDB)
	assert.Nil(t, err)
	_ = miniDB.Set([]byte("key-1"), []byte("value-1"))
	type args struct {
		key   []byte
		value []byte
	}
	tests := []struct {
		name    string
		db      *MiniDB
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "nil-key", db: miniDB, args: args{key: nil, value: []byte("nil value")}, want: nil, wantErr: false,
		},
		{
			name: "nil-value", db: miniDB, args: args{key: []byte("nil-key"), value: nil}, want: nil, wantErr: false,
		},
		{
			name: "normal", db: miniDB, args: args{key: []byte("key-1"), value: []byte("hello")}, want: []byte("value-1hello"), wantErr: false,
		},
		{
			name: "key not exist", db: miniDB, args: args{key: []byte("key-2"), value: []byte("hello")}, want: []byte("hello"), wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.db.Append(tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Append() err:%v ,wantErr:%v", err, tt.wantErr)
			}
			if tt.want != nil {
				get, _ := tt.db.Get(tt.args.key)
				if !bytes.Equal(get, tt.want) {
					t.Errorf("Append() got:%v ,want:%v", get, tt.want)

				}
			}

		})
	}
}

func TestMiniDB_Incr(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testMiniDBIncr(t, FileIO, KeyOnlyMemMode)

	})
	t.Run("mmp", func(t *testing.T) {
		testMiniDBIncr(t, MMap, KeyValueMemMode)

	})
	t.Run("kv-mode", func(t *testing.T) {
		testMiniDBIncr(t, FileIO, KeyValueMemMode)
	})
}
func testMiniDBIncr(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode
	miniDB, err := Open(options)
	defer destroyDB(miniDB)
	assert.Nil(t, err)

	_ = miniDB.MSet(
		[]byte("nil-value"), nil,
		[]byte("ten"), []byte("10"),
		[]byte("max"), []byte(strconv.Itoa(math.MaxInt64)),
		[]byte("str"), []byte("hello"))
	tests := []struct {
		name     string
		db       *MiniDB
		key      []byte
		expValue int64
		expError error
		expByte  []byte
		wantErr  bool
	}{
		{
			"nil-value", miniDB, []byte("nil-value"), 1, nil, []byte("1"), false,
		},
		{
			"exist-key", miniDB, []byte("ten"), 11, nil, []byte("11"), false,
		},
		{
			"not exist key", miniDB, []byte("not exist"), 1, nil, []byte("1"), false,
		},
		{
			"overflow value-max", miniDB, []byte("max"), 0, ErrIntegerOverflow, nil, true,
		},
		{
			"wrong type", miniDB, []byte("str"), 0, ErrWrongValueType, nil, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			incr, err := tt.db.Incr(tt.key)
			if (err != nil) != tt.wantErr || err != tt.expError {
				t.Errorf("Incr() err:%v, wantErr:%v", err, tt.expError)
			}
			if tt.expValue != incr {
				t.Errorf("Incr() got:%v, want:%v", incr, tt.expValue)
			}
			get, _ := tt.db.Get(tt.key)
			if tt.expByte != nil && !bytes.Equal(get, tt.expByte) {
				t.Errorf("Incr() gotByte:%v, wantByte:%v", get, tt.expByte)
			}

		})
	}
}

func TestMiniDB_Decr(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testMiniDBDecr(t, FileIO, KeyOnlyMemMode)

	})
	t.Run("mmp", func(t *testing.T) {
		testMiniDBDecr(t, MMap, KeyValueMemMode)

	})
	t.Run("kv-mode", func(t *testing.T) {
		testMiniDBDecr(t, FileIO, KeyValueMemMode)
	})
}
func testMiniDBDecr(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode
	miniDB, err := Open(options)
	defer destroyDB(miniDB)
	assert.Nil(t, err)

	_ = miniDB.MSet(
		[]byte("nil-value"), nil,
		[]byte("ten"), []byte("10"),
		[]byte("min"), []byte(strconv.Itoa(math.MinInt64)),
		[]byte("str"), []byte("hello"))
	tests := []struct {
		name     string
		db       *MiniDB
		key      []byte
		expValue int64
		expError error
		expByte  []byte
		wantErr  bool
	}{
		{
			"nil-value", miniDB, []byte("nil-value"), -1, nil, []byte("-1"), false,
		},
		{
			"exist-key", miniDB, []byte("ten"), 9, nil, []byte("9"), false,
		},
		{
			"not exist key", miniDB, []byte("not exist"), -1, nil, []byte("-1"), false,
		},
		{
			"overflow value-max", miniDB, []byte("min"), 0, ErrIntegerOverflow, nil, true,
		},
		{
			"wrong type", miniDB, []byte("str"), 0, ErrWrongValueType, nil, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			incr, err := tt.db.Decr(tt.key)
			if (err != nil) != tt.wantErr || err != tt.expError {
				t.Errorf("Incr() err:%v, wantErr:%v", err, tt.expError)
			}
			if tt.expValue != incr {
				t.Errorf("Incr() got:%v, want:%v", incr, tt.expValue)
			}
			get, _ := tt.db.Get(tt.key)
			if tt.expByte != nil && !bytes.Equal(get, tt.expByte) {
				t.Errorf("Incr() gotByte:%v, wantByte:%v", get, tt.expByte)
			}

		})
	}
}
func TestMiniDB_GetDel(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testMiniDBGetDel(t, FileIO, KeyOnlyMemMode)

	})
	t.Run("mmp", func(t *testing.T) {
		testMiniDBGetDel(t, MMap, KeyValueMemMode)

	})
	t.Run("kv-mode", func(t *testing.T) {
		testMiniDBGetDel(t, FileIO, KeyValueMemMode)
	})
}
func testMiniDBGetDel(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode
	miniDB, err := Open(options)
	defer destroyDB(miniDB)
	assert.Nil(t, err)

	_ = miniDB.MSet(
		[]byte("nil-value"), nil,
		[]byte("key-1"), []byte("value-1"),
		[]byte("key-2"), []byte("value-2"),
		[]byte("key-3"), []byte("value-3"),
		[]byte("key-4"), []byte("value-4"),
	)
	tests := []struct {
		name     string
		db       *MiniDB
		key      []byte
		expValue []byte
		expError error
	}{
		{"nil-value", miniDB, []byte("nil-value"), nil, nil},
		{"not exist in db", miniDB, []byte("not exist"), nil, nil},
		{" exist in db", miniDB, []byte("key-1"), []byte("value-1"), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			del, err := tt.db.GetDel(tt.key)
			if err != tt.expError {
				t.Errorf("GetDel() err:%v ,wantErr :%v", err, tt.expError)

			}
			if !bytes.Equal(del, tt.expValue) {
				t.Errorf("GetDel() got:%v ,want :%v", del, tt.expValue)

			}

		})
	}
}
func TestMiniDB_StrLen(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testMiniDBStrLen(t, FileIO, KeyOnlyMemMode)

	})
	t.Run("mmp", func(t *testing.T) {
		testMiniDBStrLen(t, MMap, KeyValueMemMode)

	})
	t.Run("kv-mode", func(t *testing.T) {
		testMiniDBStrLen(t, FileIO, KeyValueMemMode)
	})
}
func testMiniDBStrLen(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode
	miniDB, err := Open(options)
	defer destroyDB(miniDB)
	assert.Nil(t, err)
	_ = miniDB.MSet(
		[]byte("nil-val"), nil,
		[]byte("key-1"), []byte("val-1"),
		[]byte("key-2"), []byte("val-2"),
	)
	tests := []struct {
		name      string
		db        *MiniDB
		key       []byte
		expectLen int
	}{
		{
			"nil value", miniDB, []byte("nil-val"), 0,
		},
		{
			"not exist in db", miniDB, []byte("nil"), 0,
		},
		{
			"exist in db", miniDB, []byte("key-1"), 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strLen := tt.db.StrLen(tt.key)
			if strLen != tt.expectLen {
				t.Errorf("StrLen() got:%d,expect:%d", strLen, tt.expectLen)
			}
		})
	}

}

func TestMiniDB_Count(t *testing.T) {
	options := DefaultOptions("/tmp/miniDB")
	miniDB, err := Open(options)
	defer destroyDB(miniDB)
	assert.Nil(t, err)
	count := miniDB.Count()
	assert.Equal(t, 0, count)

	for i := 0; i < 100; i++ {
		_ = miniDB.Set(GetKey(i), GetValue128B())
	}
	count = miniDB.Count()
	assert.Equal(t, count, 100)
}
func TestMiniDB_Scan(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testMiniDBScan(t, FileIO, KeyOnlyMemMode)

	})
	t.Run("mmp", func(t *testing.T) {
		testMiniDBScan(t, MMap, KeyValueMemMode)

	})
	t.Run("kv-mode", func(t *testing.T) {
		testMiniDBScan(t, FileIO, KeyValueMemMode)
	})
}
func testMiniDBScan(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode
	miniDB, err := Open(options)
	defer destroyDB(miniDB)
	assert.Nil(t, err)
	for i := 0; i < 100; i++ {
		_ = miniDB.Set(GetKey(i), GetValue128B())
	}
	_ = miniDB.Set([]byte("aba"), GetValue16B())
	_ = miniDB.Set([]byte("abc"), GetValue16B())
	_ = miniDB.Set([]byte("abd"), GetValue16B())
	_ = miniDB.Set([]byte("asa"), GetValue16B())
	_ = miniDB.Set([]byte("aba"), GetValue16B())

	_ = miniDB.Set([]byte("122"), GetValue16B())
	_ = miniDB.Set([]byte("1212"), GetValue16B())
	_ = miniDB.Set([]byte("32131"), GetValue16B())
	scan, err := miniDB.Scan(nil, "", 10)
	assert.Nil(t, err)
	assert.Equal(t, 20, len(scan))
	scan, err = miniDB.Scan([]byte("ab"), "", 20)
	assert.Nil(t, err)
	assert.Equal(t, 6, len(scan))
	scan, err = miniDB.Scan(nil, "^[0-9]*$", 3)
	assert.Nil(t, err)
	assert.Equal(t, 6, len(scan))
}
func destroyDB(db *MiniDB) {
	if db != nil {
		_ = db.Close()
		if runtime.GOOS == "windows" {
			time.Sleep(time.Millisecond * 100)
		}
		err := os.RemoveAll(db.opts.DBPath)
		if err != nil {
			logger.Errorf("destroy db err: %v", err)
		}
	}
}
