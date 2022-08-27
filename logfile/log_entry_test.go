package logfile

import (
	"log"
	"reflect"
	"testing"
)

func Test_encode(t *testing.T) {
	le := LogEntry{ExpiredAt: 123456789, Type: TypeDelete}
	//le := LogEntry{}
	entry, i := EncodeEntry(&le)
	t.Log(i)
	t.Log(entry)

	le.Value = []byte("hello world")
	le.Key = []byte("kv")
	log.Println(getEntryCrc(&le, entry))

}

func TestEncodeEntry(t *testing.T) {
	type args struct {
		e *LogEntry
	}

	tests := []struct {
		name  string
		args  args
		want  []byte
		want1 int
	}{
		{
			"nil", args{e: nil}, nil, 0,
		},
		{
			"no-fields", args{e: &LogEntry{}}, []byte{28, 223, 68, 33, 0, 0, 0, 0}, 8,
		},
		{
			"no-key-and-value", args{e: &LogEntry{ExpiredAt: 123456789}}, []byte{241, 29, 57, 66, 0, 0, 0, 170, 180, 222, 117}, 11,
		},
		{
			"with-key-value", args{e: &LogEntry{Key: []byte("kv"), Value: []byte("hello world")}}, []byte{7, 23, 198, 63, 0, 4, 22, 0, 107, 118, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100}, 21,
		},
		{
			"with-kv-exp", args{e: &LogEntry{Key: []byte("kv"), Value: []byte("hello world"), ExpiredAt: 123456789}}, []byte{125, 132, 148, 30, 0, 4, 22, 170, 180, 222, 117, 107, 118, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100}, 24,
		}, {
			"type-delete", args{e: &LogEntry{Key: []byte("kv"), Value: []byte("hello world"), ExpiredAt: 123456789, Type: TypeDelete}}, []byte{1, 4, 22, 170, 180, 222, 117, 107, 118, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100}, 24,
		},
	}
	for _, k := range tests {

		t.Run(k.name, func(t *testing.T) {
			got, gotn := EncodeEntry(k.args.e)
			if !reflect.DeepEqual(got, k.want) {
				t.Errorf("encodeEntry() got:%v want:%v", got, k.want)
			}
			if gotn != k.want1 {
				t.Errorf("EncodeEntry() got1 = %v, want %v", gotn, k.want1)
			}

		})
	}

}

func Test_decodeHeader(t *testing.T) {

	type args struct {
		buf []byte
	}

	tests := []struct {
		name  string
		args  args
		want  *entryHeader
		want1 int64
	}{
		{
			"nil", args{buf: nil}, nil, 0,
		},
		{
			"no-enough-bytes", args{buf: []byte{1, 2, 3, 4}}, nil, 0,
		},
		{
			"no-fields", args{buf: []byte{28, 223, 68, 33, 0, 0, 0, 0}}, &entryHeader{crc32: 558161692}, 8,
		},
		{
			"normal", args{buf: []byte{59, 191, 243, 123, 14, 22, 170, 180, 222, 117}}, &entryHeader{crc32: 2079571771, typ: 1, kSize: 2, vSize: uint32(len("hello world")), expiredAt: 123456789}, 11,
		},
	}
	for _, v := range tests {
		t.Run(v.name, func(t *testing.T) {
			got1, got2 := DecodeHeader(v.args.buf)

			if !reflect.DeepEqual(got1, v.want) {
				t.Errorf("DecodeHeader() got: %v want %v", got1, v.want)
			}
			if got2 != v.want1 {
				t.Errorf("DecodeHeader() got: %v want %v", got2, v.want1)
			}
		})
	}

}
func Test_EntryCrc(t *testing.T) {
	type args struct {
		e *LogEntry
		h []byte
	}

	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			"nil", args{e: nil, h: nil}, 0,
		},
		{
			"no-fields", args{e: &LogEntry{}, h: []byte{0, 0, 0, 0}}, 558161692,
		},
		{
			"normal", args{e: &LogEntry{Key: []byte("kv"), Value: []byte("hello world")}, h: []byte{69, 22, 78, 228, 1, 0, 0, 170, 180, 222, 117}}, 2122864740,
		},
	}
	for _, v := range tests {

		t.Run(v.name, func(t *testing.T) {
			crc := getEntryCrc(v.args.e, v.args.h)
			if crc != v.want {
				t.Errorf("getEntryCrc()= %v want:%v", crc, v.want)
			}
		})
	}

}
