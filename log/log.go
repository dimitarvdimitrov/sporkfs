package log

import (
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var l *zap.Logger

func init() {
	highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})
	lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.DebugLevel && lvl < zapcore.ErrorLevel
	})
	consoleInfos := zapcore.Lock(os.Stdout)
	consoleErrors := zapcore.Lock(os.Stderr)
	ecfg := zap.NewProductionEncoderConfig()
	ecfg.EncodeTime = func(time time.Time, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString(time.Format("2006-01-02T15:04:05.000"))
	}
	consoleEncoder := zapcore.NewConsoleEncoder(ecfg)

	core := zapcore.NewTee(
		zapcore.NewCore(consoleEncoder, consoleErrors, highPriority),
		zapcore.NewCore(consoleEncoder, consoleInfos, lowPriority),
	)
	logger := zap.New(core)
	zap.RedirectStdLog(logger)
	l = logger
}

func Sync() {
	_ = l.Sync()
}

func Id(id uint64) zap.Field {
	return zap.Uint64("id", id)
}

func Hash(h uint64) zap.Field {
	return zap.Uint64("hash", h)
}

func Name(n string) zap.Field {
	return zap.String("name", n)
}

func Debug(msg string, args ...zap.Field) {
	l.Debug(msg, args...)
}

func Info(msg string, args ...zap.Field) {
	l.Info(msg, args...)
}

func Warn(msg string, args ...zap.Field) {
	l.Warn(msg, args...)
}

func Error(msg string, args ...zap.Field) {
	l.Error(msg, args...)
}

func Panic(msg string, args ...zap.Field) {
	l.Panic(msg, args...)
}

func Fatal(msg string, args ...zap.Field) {
	l.Fatal(msg, args...)
}

type logger struct {
	*zap.SugaredLogger
}

func Logger() *logger {
	return &logger{l.Sugar()}
}

func (logger *logger) Warning(args ...interface{}) {
	logger.Warn(args...)
}

func (logger *logger) Warningf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}
