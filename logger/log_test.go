package logger

import "testing"

func TestDebug(t *testing.T) {
	Infof("%s", "test debug info ")
}

func TestFatal(t *testing.T) {
	Fatal("test fatal info")
}
func TestError(t *testing.T) {
	Error("test Error info ")
}

func TestInfo(t *testing.T) {
	Info("test info ")
}
