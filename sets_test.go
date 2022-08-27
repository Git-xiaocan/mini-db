package minidb

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestMiniDB_SAdd(t *testing.T) {
	t.Run("fio", func(t *testing.T) {
		testMiniDBSAdd(t, FileIO, KeyValueMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testMiniDBSAdd(t, MMap, KeyOnlyMemMode)
	})

}
func testMiniDBSAdd(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "miniDb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)
	type args struct {
		key    []byte
		member [][]byte
	}
	tests := []struct {
		name    string
		db      *MiniDB
		args    args
		wantErr bool
	}{
		{"normal-1", db, args{key: []byte("k1"), member: [][]byte{[]byte("value1")}}, false},
		{"key exist", db, args{key: []byte("k1"), member: [][]byte{[]byte("value1")}}, false},
		{"normal-1", db, args{key: []byte("k2"), member: [][]byte{[]byte("value2")}}, false},
		{"nil-key", db, args{key: nil, member: [][]byte{[]byte("value2")}}, false},
		{"nil-value", db, args{key: []byte("kk"), member: nil}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.SAdd(tt.args.key, tt.args.member...); (err != nil) != tt.wantErr {
				t.Logf("sadd() err:%+v,wantErr:%+v", err, tt.wantErr)
			}
		})
	}

}
func TestMiniDB_SPop(t *testing.T) {
	t.Run("fio", func(t *testing.T) {
		testMiniDBSPop(t, FileIO, KeyValueMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testMiniDBSPop(t, MMap, KeyOnlyMemMode)
	})
}
func testMiniDBSPop(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "miniDb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	key := []byte("Set-key")
	err = db.SAdd(key, GetKey(1), GetKey(2), GetKey(3), GetKey(4))
	assert.Nil(t, err)
	card := db.SCard(key)
	assert.Equal(t, card, 4)
	pop, err := db.SPop(key, 5)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(pop))
	sPop, err := db.SPop(key, 1)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(sPop))
	sCard := db.SCard(key)
	assert.Equal(t, 0, sCard)

}
func TestMiniDB_SRem(t *testing.T) {
	t.Run("fio", func(t *testing.T) {
		testMiniDBSRem(t, FileIO, KeyValueMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testMiniDBSRem(t, MMap, KeyOnlyMemMode)
	})
}
func testMiniDBSRem(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "miniDb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	key := []byte("Set-key")
	err = db.SAdd(key, GetKey(1), GetKey(2), GetKey(3), GetKey(4))
	assert.Nil(t, err)
	card := db.SCard(key)
	assert.Equal(t, card, 4)
	err = db.SRem(key, GetKey(1))
	assert.Nil(t, err)

	is := db.SIsMember(key, GetKey(2))
	assert.True(t, is)
	is = db.SIsMember(key, GetKey(3))
	assert.True(t, is)
	is = db.SIsMember(key, GetKey(4))
	assert.True(t, is)
	is = db.SIsMember(key, GetKey(1))
	assert.False(t, is)

}

func TestMiniDB_SMembers(t *testing.T) {
	t.Run("fio", func(t *testing.T) {
		testMiniDBSMembers(t, FileIO, KeyValueMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testMiniDBSMembers(t, MMap, KeyOnlyMemMode)
	})
}
func testMiniDBSMembers(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "miniDb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	key := []byte("Set-key")
	err = db.SAdd(key, GetKey(1), GetKey(2), GetKey(3), GetKey(4))
	assert.Nil(t, err)
	members, err := db.sMembers(key)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(members))

}
func TestMiniDB_SDiff(t *testing.T) {
	t.Run("fio", func(t *testing.T) {
		testMiniDBSDiff(t, FileIO, KeyValueMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testMiniDBSDiff(t, MMap, KeyOnlyMemMode)
	})
}
func testMiniDBSDiff(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "miniDb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	key := []byte("Set-key1")
	err = db.SAdd(key, GetKey(1), GetKey(2), GetKey(3), GetKey(9))
	key2 := []byte("Set-key2")

	err = db.SAdd(key2, GetKey(1), GetKey(2), GetKey(8), GetKey(6))
	diff, err := db.SDiff(key, key2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(diff))
}
func TestMiniDB_SUnion(t *testing.T) {
	t.Run("fio", func(t *testing.T) {
		testMiniDBSUnion(t, FileIO, KeyValueMemMode)
	})
	t.Run("mmap", func(t *testing.T) {
		testMiniDBSUnion(t, MMap, KeyOnlyMemMode)
	})
}
func testMiniDBSUnion(t *testing.T, ioType IOType, mode DataIndexMode) {
	path := filepath.Join("/tmp", "miniDb")
	opts := DefaultOptions(path)
	opts.IoType = ioType
	opts.IndexMode = mode
	db, err := Open(opts)
	assert.Nil(t, err)
	defer destroyDB(db)

	key := []byte("Set-key1")
	err = db.SAdd(key, GetKey(1), GetKey(2), GetKey(3), GetKey(9))
	key2 := []byte("Set-key2")

	err = db.SAdd(key2, GetKey(1), GetKey(2), GetKey(8), GetKey(6))
	diff, err := db.SUnion(key, key2)
	assert.Nil(t, err)
	assert.Equal(t, 6, len(diff))
	union, err := db.SUnion([]byte("k3"), []byte("k4"))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(union))
}
