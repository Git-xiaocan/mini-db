package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMurHash128_EncodeSum128(t *testing.T) {
	str := "abc123444444"
	hash128 := NewMurHash128()
	err := hash128.Write([]byte(str))
	assert.Nil(t, err)
	sum128 := hash128.EncodeSum128()
	t.Logf("encode:%v", sum128)
	hash128.Reset()

}
