package logfile

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestOpenLogFile(t *testing.T) {
	t.Run("mmap", func(t *testing.T) {
		testOpenLogFile(t, MMap)

	})
	t.Run("fileIo", func(t *testing.T) {
		testOpenLogFile(t, FileIO)
	})
}

func testOpenLogFile(t *testing.T, ioType IOType) {
	type args struct {
		path   string
		fid    uint32
		fsize  int64
		ftype  FileType
		iotype IOType
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"zero-size", args{path: "/tmp", fid: 0, fsize: 0, ftype: List, iotype: ioType}, true,
		},
		{
			"normal", args{path: "/tmp", fid: 1, fsize: 100, ftype: List, iotype: ioType}, false,
		},
		{
			"big-szie", args{path: "/tmp", fid: 2, fsize: 1024 << 10, ftype: List, iotype: ioType}, false,
		},
	}
	for _, vv := range tests {
		t.Run(vv.name, func(t *testing.T) {
			lf, err := OpenLogFile(vv.args.path, vv.args.fid, vv.args.fsize, vv.args.ftype, vv.args.iotype)
			assert.Nil(t, err)
			defer func() {
				if lf != nil && lf.IoSelector != nil {
					_ = lf.Delete()
				}
			}()
			if (err != nil) != vv.wantErr {
				t.Errorf("OpenLogFile() error:%v wanErr:%v", err, vv.wantErr)
			}
			if !vv.wantErr && lf == nil {
				t.Errorf("OpenLogFile() lf == nil but want not nil")
			}

		})
	}
}

func TestLogFile_Write(t *testing.T) {
	t.Run("mmap", func(t *testing.T) {
		testLogFileWrite(t, FileIO)
	})
	t.Run("fileio", func(t *testing.T) {
		testLogFileWrite(t, MMap)
	})
}
func testLogFileWrite(t *testing.T, ioType IOType) {
	lf, err := OpenLogFile("/tmp", 1, 1024<<10, List, ioType)
	assert.Nil(t, err)
	defer func() {
		if lf != nil {
			_ = lf.Delete()
		}
	}()

	type fields struct {
		lf *LogFile
	}
	type args struct {
		buf []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"nil", fields{lf: lf}, args{nil}, false},
		{"no-value", fields{lf: lf}, args{buf: []byte{}}, false},
		{"normal-1", fields{lf: lf}, args{buf: []byte("hello world")}, false},
		{"normal-2", fields{lf: lf}, args{buf: []byte("I am super man")}, false},
	}

	for _, vv := range tests {
		t.Run(vv.name, func(t *testing.T) {
			if err := vv.fields.lf.Write(vv.args.buf); (err != nil) != vv.wantErr {
				t.Errorf("Write() error:%v wanErr:%v", err, vv.wantErr)
			}
		})
	}
}
func TestLogFile_Read(t *testing.T) {
	t.Run("mmap", func(t *testing.T) {
		testLogFileRead(t, MMap)
	})
	t.Run("fileio", func(t *testing.T) {
		testLogFileRead(t, FileIO)
	})
}
func testLogFileRead(t *testing.T, ioType IOType) {
	lf, err := OpenLogFile("/tmp", 1, 1<<20, List, ioType)
	assert.Nil(t, err)
	defer func() {
		if lf != nil {
			_ = lf.Delete()
		}
	}()
	data := [][]byte{
		[]byte(""),
		[]byte("0"),
		[]byte("hello"),
		[]byte("world"),
	}
	offsets := writeSomeData(lf, data)

	type fields struct {
		lf *LogFile
	}
	type args struct {
		offset int64
		size   uint32
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{"read-0", fields{lf}, args{offset: offsets[0], size: 0}, data[0], false},
		{"read-1", fields{lf}, args{offset: offsets[1], size: 1}, data[1], false},
		{"read-2", fields{lf}, args{offset: offsets[2], size: 5}, data[2], false},
		{"read-3", fields{lf}, args{offset: offsets[3], size: 5}, data[3], false},
	}
	for _, vv := range tests {
		t.Run(vv.name, func(t *testing.T) {
			buf, err := vv.fields.lf.Read(vv.args.offset, vv.args.size)
			if (err != nil) != vv.wantErr {
				t.Errorf("Read() err:%v wanfErr:%v", err, vv.wantErr)
			}
			if !reflect.DeepEqual(buf, vv.want) {
				t.Errorf("Read() got:%v want :%v", buf, vv.want)
			}
		})
	}

}

func TestLogFile_Sync(t *testing.T) {
	sync := func(ioType IOType) {
		lf, err := OpenLogFile("/tmp", 0, 1024<<10, List, ioType)
		assert.Nil(t, err)
		defer func() {
			if lf != nil {
				_ = lf.Delete()
			}
		}()
		err = lf.Sync()
		assert.Nil(t, err)

	}
	t.Run("mmap", func(t *testing.T) {
		sync(MMap)
	})
	t.Run("fileio", func(t *testing.T) {
		sync(FileIO)
	})
}
func TestLogFile_Close(t *testing.T) {
	close := func(ioType IOType) {
		lf, err := OpenLogFile("/tmp", 0, 1024<<10, List, ioType)
		assert.Nil(t, err)
		defer func() {
			if lf != nil {
				_ = lf.Delete()
			}
		}()
		err = lf.Close()
		assert.Nil(t, err)

	}
	t.Run("mmap", func(t *testing.T) {
		close(MMap)
	})
	t.Run("fileio", func(t *testing.T) {
		close(FileIO)
	})
}

func writeSomeData(lf *LogFile, buf [][]byte) []int64 {
	var offset []int64
	for _, vv := range buf {
		off := atomic.LoadInt64(&lf.WriteAt)
		offset = append(offset, off)
		if err := lf.Write(vv); err != nil {
			panic(fmt.Sprintf("write data:%v error: %v", string(vv), err))
		}

	}
	return offset
}

func TestLogFile_ReadLogEntry(t *testing.T) {
	t.Run("mmap", func(t *testing.T) {
		testLogFileReadLogEntry(t, MMap)
	})
	t.Run("fileio", func(t *testing.T) {
		testLogFileReadLogEntry(t, FileIO)
	})
}
func testLogFileReadLogEntry(t *testing.T, ioType IOType) {
	lf, err := OpenLogFile("/tmp", 1, 1024<<20, List, ioType)
	assert.Nil(t, err)
	defer func() {
		if lf != nil {

			_ = lf.Delete()
		}
	}()
	// write some entries
	entries := []*LogEntry{
		{ExpiredAt: 123456, Type: 0},
		{ExpiredAt: 123456, Type: TypeDelete},
		{Key: []byte(""), Value: []byte(""), Type: TypeDelete, ExpiredAt: 123456},
		{Key: []byte("k1"), Value: nil, ExpiredAt: 123456},
		{Key: nil, Value: []byte("hello world"), ExpiredAt: 123456},
		{Key: []byte("k1"), Value: []byte("hello world"), ExpiredAt: 123456},
		{Key: []byte("k1"), Value: []byte("hello world"), ExpiredAt: 123456, Type: TypeDelete},
	}
	var vals [][]byte
	for _, e := range entries {
		entry, _ := EncodeEntry(e)
		vals = append(vals, entry)
	}
	offsets := writeSomeData(lf, vals)
	type fields struct {
		lf *LogFile
	}
	type args struct {
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *LogEntry
		want1   int64
		wantErr bool
	}{
		{"read-0", fields{lf: lf}, args{offset: offsets[0]}, entries[0], int64(len(vals[0])), false},
		{"read-1", fields{lf: lf}, args{offset: offsets[1]}, &LogEntry{ExpiredAt: 123456, Type: TypeDelete}, int64(len(vals[1])), false},
		{"read-2", fields{lf: lf}, args{offset: offsets[2]}, &LogEntry{Type: TypeDelete, ExpiredAt: 123456}, int64(len(vals[2])), false},
		{"read-3", fields{lf: lf}, args{offset: offsets[3]}, &LogEntry{Key: []byte("k1"), Value: []byte{}, ExpiredAt: 123456}, int64(len(vals[3])), false},
		{"read-4", fields{lf: lf}, args{offset: offsets[4]}, &LogEntry{Key: []byte{}, Value: []byte("hello world"), ExpiredAt: 123456}, int64(len(vals[4])), false},
		{"read-5", fields{lf: lf}, args{offset: offsets[5]}, entries[5], int64(len(vals[5])), false},
		{"read-6", fields{lf: lf}, args{offset: offsets[6]}, entries[6], int64(len(vals[6])), false},
	}
	for x, vv := range tests {
		entry, i, err := vv.fields.lf.ReadLogEntry(vv.args.offset)
		if (err != nil) != vv.wantErr {
			t.Errorf("ReadLogEntry() err:%v wantErr :%v", err, vv.wantErr)
			return
		}

		if !reflect.DeepEqual(vv.want, entry) {
			//assert.Equal(t, vv.want, entry)
			t.Errorf("%d:ReadLogEntry() got:%v want :%v", x, entry, vv.want)

		}
		if i != vv.want1 {
			t.Errorf("ReadLogEntry() got:%v want1 :%v", i, vv.want1)
		}

	}

}
