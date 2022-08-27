package minidb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMiniDB_LPush(t *testing.T) {
	t.Run("mmp", func(t *testing.T) {

		testMiniDBLPush(t, MMap, KeyValueMemMode)
	})

	t.Run("fio", func(t *testing.T) {
		testMiniDBLPush(t, MMap, KeyOnlyMemMode)

	})

}
func testMiniDBLPush(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	miniDB, err := Open(options)

	defer destroyDB(miniDB)
	assert.Nil(t, err)
	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		db      *MiniDB
		args    args
		wantErr bool
	}{
		{"nil-value", miniDB, args{key: []byte("key-nil"), values: nil}, false},
		{"one-value", miniDB, args{key: []byte("key-nil"), values: [][]byte{GetValue16B()}}, false},
		{"multiple-value", miniDB, args{key: []byte("key-nil"), values: [][]byte{GetValue16B(), GetValue128B(), GetValue4K()}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.LPush(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
				t.Logf("LPush err :%v, wantErr:%v", err, tt.wantErr)
			}
		})
	}
}
func TestMiniDB_LPushX(t *testing.T) {
	t.Run("mmp", func(t *testing.T) {

		testMiniDBLPushX(t, MMap, KeyValueMemMode)
	})

	t.Run("fio", func(t *testing.T) {
		testMiniDBLPushX(t, MMap, KeyOnlyMemMode)

	})

}
func testMiniDBLPushX(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	miniDB, err := Open(options)

	defer destroyDB(miniDB)
	assert.Nil(t, err)
	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		db      *MiniDB
		args    args
		wantErr bool
	}{
		{"nil-value", miniDB, args{key: []byte("key-nil"), values: nil}, false},
		{"one-value", miniDB, args{key: []byte("key-1"), values: [][]byte{GetValue16B()}}, false},
		{"exist-value", miniDB, args{key: []byte("key-nil"), values: [][]byte{GetValue16B()}}, true},
		{"multiple-value", miniDB, args{key: []byte("key-nil2"), values: [][]byte{GetValue16B(), GetValue128B(), GetValue4K()}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.LPushX(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr && err != ErrKeyNotFound {
				t.Errorf("LPush err :%v, wantErr:%v", err, tt.wantErr)
			}
		})
	}
}
func TestMiniDB_RPush(t *testing.T) {
	t.Run("mmp", func(t *testing.T) {

		testMiniDBRPush(t, MMap, KeyValueMemMode)
	})

	t.Run("fio", func(t *testing.T) {
		testMiniDBRPush(t, MMap, KeyOnlyMemMode)

	})

}
func testMiniDBRPush(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	miniDB, err := Open(options)

	defer destroyDB(miniDB)
	assert.Nil(t, err)
	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		db      *MiniDB
		args    args
		wantErr bool
	}{
		{"nil-value", miniDB, args{key: []byte("key-nil"), values: nil}, false},
		{"one-value", miniDB, args{key: []byte("key-nil"), values: [][]byte{GetValue16B()}}, false},
		{"multiple-value", miniDB, args{key: []byte("key-nil"), values: [][]byte{GetValue16B(), GetValue128B(), GetValue4K()}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.RPush(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
				t.Logf("RPush err :%v, wantErr:%v", err, tt.wantErr)
			}
		})
	}
}
func TestMiniDB_LPop(t *testing.T) {
	t.Run("mmp", func(t *testing.T) {

		testMiniDBLPop(t, MMap, KeyValueMemMode)
	})

	t.Run("fio", func(t *testing.T) {
		testMiniDBLPop(t, MMap, KeyOnlyMemMode)

	})

}
func testMiniDBLPop(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	miniDB, err := Open(options)

	defer destroyDB(miniDB)
	assert.Nil(t, err)
	for i := 0; i < 5; i++ {
		_ = miniDB.LPush([]byte("key"), GetKey(i))
	}
	for i := 4; i >= 0; i-- {
		pop, err := miniDB.LPop([]byte("key"))
		assert.Nil(t, err)
		assert.Equal(t, pop, GetKey(i))
	}
	pop, err := miniDB.LPop([]byte("key"))
	assert.Nil(t, err)
	assert.Nil(t, pop)
}

func TestMiniDB_LMove(t *testing.T) {
	t.Run("mmp", func(t *testing.T) {

		testMiniDBLMove(t, MMap, KeyValueMemMode)
	})

	t.Run("fio", func(t *testing.T) {
		testMiniDBLMove(t, MMap, KeyOnlyMemMode)

	})
}
func testMiniDBLMove(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	miniDB, err := Open(options)

	defer destroyDB(miniDB)
	assert.Nil(t, err)
	_ = miniDB.LPush([]byte("key-1"), GetKey(1), GetKey(2), GetKey(3), GetKey(4))
	move, err := miniDB.LMove([]byte("key-1"), []byte("key-2"), true, true)
	assert.Nil(t, err)
	assert.Equal(t, move, GetKey(4))
	pop, err := miniDB.LPop([]byte("key-2"))
	assert.Nil(t, err)
	assert.Equal(t, pop, GetKey(4))

}
func TestMiniDB_LLen(t *testing.T) {
	t.Run("mmp", func(t *testing.T) {

		testMiniDBLLen(t, MMap, KeyValueMemMode)
	})

	t.Run("fio", func(t *testing.T) {
		testMiniDBLLen(t, MMap, KeyOnlyMemMode)

	})
}
func testMiniDBLLen(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	miniDB, err := Open(options)

	defer destroyDB(miniDB)
	var values [][]byte
	assert.Nil(t, err)
	key := []byte("key")
	for i := 0; i < 10; i++ {
		val := GetValue16B()
		values = append(values, val)
	}
	_ = miniDB.RPush(key, values...)

	lLen := miniDB.LLen(key)
	assert.Equal(t, lLen, 10)
	_ = miniDB.RPush(key, GetValue128B())
	lLen = miniDB.LLen(key)
	assert.Equal(t, lLen, 11)
	_, _ = miniDB.LPop(key)
	lLen = miniDB.LLen(key)

	assert.Equal(t, lLen, 10)

}
func TestMiniDB_LRange(t *testing.T) {
	t.Run("mmp", func(t *testing.T) {

		testMiniDBLRange(t, MMap, KeyValueMemMode)
	})

	t.Run("fio", func(t *testing.T) {
		testMiniDBLRange(t, MMap, KeyOnlyMemMode)

	})
}
func testMiniDBLRange(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	miniDB, err := Open(options)

	defer destroyDB(miniDB)
	var values [][]byte
	assert.Nil(t, err)
	key := []byte("key")
	for i := 0; i < 10; i++ {
		val := GetValue16B()
		values = append(values, val)
	}
	_ = miniDB.RPush(key, values...)

	lRange, err := miniDB.LRange(key, 0, 7)
	assert.Nil(t, err)
	assert.Equal(t, values[:8], lRange)
}

func TestMiniDB_LSet(t *testing.T) {
	t.Run("mmp", func(t *testing.T) {

		testMiniDBLSet(t, MMap, KeyValueMemMode)
	})

	t.Run("fio", func(t *testing.T) {
		testMiniDBLSet(t, MMap, KeyOnlyMemMode)

	})
}
func testMiniDBLSet(t *testing.T, ioType IOType, mode DataIndexMode) {
	options := DefaultOptions("/tmp/miniDB")
	options.IoType = ioType
	options.IndexMode = mode

	miniDB, err := Open(options)

	defer destroyDB(miniDB)
	assert.Nil(t, err)

	key := []byte("key")
	for i := 0; i < 10; i++ {
		val := GetValue16B()
		_ = miniDB.RPush(key, val)
	}
	err = miniDB.LSet(key, []byte("test"), 2)
	assert.Nil(t, err)

	_, _ = miniDB.LPop(key)
	_, _ = miniDB.LPop(key)
	val, _ := miniDB.LPop(key)
	assert.Equal(t, val, []byte("test"))

}
