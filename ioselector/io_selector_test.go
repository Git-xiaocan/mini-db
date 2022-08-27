package ioselector

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"path/filepath"
	"testing"
)

func TestNewFileIOSelector(t *testing.T) {
	testNewIOSelector(t, 0)
}
func TestNewMMapSelector(t *testing.T) {
	testNewIOSelector(t, 1)
}

func TestFileIOSelector_Write(t *testing.T) {
	testIOSelectorWrite(t, 0)
}
func TestMMapSelector_Write(t *testing.T) {
	testIOSelectorWrite(t, 1)
}

func TestFileIOSelector_Read(t *testing.T) {
	testIOSelectorRead(t, 0)
}
func TestMMapSelector_Read(t *testing.T) {
	testIOSelectorRead(t, 1)
}

func testNewIOSelector(t *testing.T, ioType uint8) {
	type arg struct {
		fName string
		fsize int64
	}
	tests := []struct {
		name string
		args arg
	}{
		{
			name: "size zero",
			args: arg{fName: "000000001.wal", fsize: 0},
		},
		{
			name: "size negative",
			args: arg{fName: "000000002.wal", fsize: -1},
		},
		{
			name: "size-big",
			args: arg{fName: "000000003.wal", fsize: 1024 << 20},
		},
	}
	for _, v := range tests {
		t.Run(v.name, func(t *testing.T) {
			absPath, err := filepath.Abs(filepath.Join("/tmp", v.args.fName))
			assert.Nil(t, err)
			var got IOSelector
			if ioType == 0 {
				got, err = NewFileIOSelector(absPath, v.args.fsize)
			}
			if ioType == 1 {
				got, err = NewMMapSelector(absPath, v.args.fsize)

			}
			defer func() {
				if got != nil {
					err = got.Delete()
					assert.Nil(t, err)
				}
			}()
			if v.args.fsize > 0 {
				assert.Nil(t, err)
				assert.NotNil(t, got)
			} else {
				assert.Equal(t, err, ErrInvalidFsize)
			}
		})
	}
}

func testIOSelectorWrite(t *testing.T, ioType uint8) {

	absPath, err := filepath.Abs(filepath.Join("/tmp", "00000001.vlog"))
	assert.Nil(t, err)

	var size int64 = 1048576

	var selector IOSelector

	if ioType == 0 {
		selector, err = NewFileIOSelector(absPath, size)
	}
	if ioType == 1 {
		selector, err = NewMMapSelector(absPath, size)
	}
	assert.Nil(t, err)
	defer func() {
		if selector != nil {
			if err := selector.Delete(); err != nil {
				log.Fatalln(err)
			}
		}
	}()
	type fields struct {
		selector IOSelector
	}
	type args struct {
		b      []byte
		offset int64
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"nil-byte", fields{selector: selector}, args{b: nil, offset: 0}, 0, false,
		},
		{
			"one-byte", fields{selector: selector}, args{[]byte("0"), 0}, 1, false,
		},
		{
			"many-bytes", fields{selector: selector}, args{[]byte("hello world"), 0}, 11, false,
		},
		{
			"big-value", fields{selector: selector}, args{[]byte(fmt.Sprintf("%01048576d", 123)), 0}, 1048576, false,
		},
		{
			"exceed-value", fields{selector: selector}, args{[]byte(fmt.Sprintf("%01048577d", 123)), 0}, 1048577, false,
		},
		{
			"EOF-error", fields{selector: selector}, args{b: []byte("hello world"), offset: -1}, 0, true,
		},
	}

	for _, v := range tests {
		t.Run(v.name, func(t *testing.T) {
			got, err := v.fields.selector.Write(v.args.b, v.args.offset)
			// io.EOF err in mmmap.
			if v.want == 1048577 && ioType == 1 {
				v.wantErr = true
				v.want = 0
			}
			if (err != nil) != v.wantErr {
				t.Errorf("write() error: %v wantErr:%v", err, v.wantErr)
				return
			}
			if got != v.want {
				t.Errorf("write() got = %v want:%v", got, v.want)
			}

		})
	}
}

func testIOSelectorRead(t *testing.T, ioType uint8) {

	absPath, err := filepath.Abs(filepath.Join("/tmp", "00000001_mini.wal"))
	var ioselector IOSelector
	if ioType == 0 {
		ioselector, err = NewFileIOSelector(absPath, 100)
	}

	if ioType == 1 {
		ioselector, err = NewMMapSelector(absPath, 100)
	}
	assert.Nil(t, err)
	defer func() {
		if ioselector != nil {
			_ = ioselector.Delete()

		}
	}()

	type fields struct {
		ioSelector IOSelector
	}
	type args struct {
		b      []byte
		offset int64
	}
	offsets := writeData(ioselector, t)
	result := [][]byte{
		[]byte(""),
		[]byte("1"),
		[]byte("12345678"),
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{"nil", fields{ioSelector: ioselector}, args{b: make([]byte, 0), offset: offsets[0]}, 0, false},
		{"one-byte", fields{ioSelector: ioselector}, args{b: make([]byte, 1), offset: offsets[1]}, 1, false},
		{"many-byte", fields{ioSelector: ioselector}, args{b: make([]byte, 8), offset: offsets[2]}, 8, false},
		{"EOF-1", fields{ioSelector: ioselector}, args{b: make([]byte, 100), offset: -1}, 0, true},
		{"EOF-2", fields{ioSelector: ioselector}, args{b: make([]byte, 100), offset: 1024}, 0, true},
	}

	for i, v := range tests {
		t.Run(v.name, func(t *testing.T) {
			got, err := v.fields.ioSelector.Read(v.args.b, v.args.offset)
			if (err != nil) != v.wantErr {
				t.Errorf("Read() error error: %v wantErr:%v", err, v.wantErr)
				return
			}
			if got != v.want {
				t.Errorf("Read() want %v got %v", v.want, got)
			}
			if !v.wantErr {
				assert.Equal(t, v.args.b, result[i])
			}

		})
	}
}

func writeData(selector IOSelector, t *testing.T) []int64 {
	tests := [][]byte{
		[]byte(""),
		[]byte("1"),
		[]byte("12345678"),
	}
	var offsets []int64
	var offset int64
	for _, tt := range tests {
		offsets = append(offsets, offset)
		n, err := selector.Write(tt, offset)
		assert.Nil(t, err)
		offset += int64(n)

	}
	return offsets
}
func TestFileIOSelector_Sync(t *testing.T) {
	testIOSelectorSync(t, 0)
}
func TestMMapSelector_Sync(t *testing.T) {
	testIOSelectorSync(t, 1)
}
func testIOSelectorSync(t *testing.T, ioType uint8) {

	sync := func(id int, fsize int64) {
		absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("000000%d", id)))
		assert.Nil(t, err)
		var selector IOSelector
		defer func() {
			if selector != nil {
				_ = selector.Delete()
			}
		}()
		if ioType == 0 {
			selector, err = NewFileIOSelector(absPath, fsize)
		}
		if ioType == 1 {
			selector, err = NewMMapSelector(absPath, fsize)
		}
		assert.Nil(t, err)
		writeData(selector, t)
		err = selector.Sync()
		assert.Nil(t, err)

	}
	for i := 1; i < 4; i++ {
		sync(i, int64(i*100))
	}

}

func TestFileIOSelector_Close(t *testing.T) {
	testIOSelectorSync(t, 0)
}
func TestMMapSelector_Close(t *testing.T) {
	testIOSelectorClose(t, 1)
}

func TestFileIOSelector_Delete(t *testing.T) {
	testIOSelectorDelete(t, 0)
}
func TestMMapSelector_Delete(t *testing.T) {
	testIOSelectorDelete(t, 1)
}
func testIOSelectorClose(t *testing.T, ioType uint8) {
	close := func(id int, fsize int64) {
		absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("000000%d", id)))
		assert.Nil(t, err)
		var selector IOSelector
		defer func() {
			if selector != nil {
				_ = os.Remove(absPath)
			}
		}()
		if ioType == 0 {
			selector, err = NewFileIOSelector(absPath, fsize)
		}
		if ioType == 1 {
			selector, err = NewMMapSelector(absPath, fsize)
		}
		assert.Nil(t, err)
		defer func() {
			if selector != nil {
				err := selector.Close()
				assert.Nil(t, err)
			}
		}()
		writeData(selector, t)
		err = selector.Sync()
		assert.Nil(t, err)

	}
	for i := 1; i < 4; i++ {
		close(i, int64(i*100))
	}
}
func testIOSelectorDelete(t *testing.T, ioType uint8) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"1", false},
		{"2", false},
		{"3", false},
		{"4", false},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", i)))
			assert.Nil(t, err)
			var selector IOSelector
			if ioType == 0 {
				selector, err = NewFileIOSelector(absPath, int64((i+1)*100))
			}
			if ioType == 1 {
				selector, err = NewFileIOSelector(absPath, int64((i+1)*100))
			}
			assert.Nil(t, err)

			if err := selector.Delete(); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
