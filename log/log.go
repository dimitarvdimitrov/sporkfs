package log

import (
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var l *zap.SugaredLogger

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
	l = logger.Sugar()
}

func Sync() {
	_ = l.Sync()
}

func Debug(args ...interface{}) {
	l.Debug(args...)
}

func Debugf(format string, args ...interface{}) {
	l.Debugf(format, args...)
}

func Info(args ...interface{}) {
	l.Info(args...)
}

func Infof(format string, args ...interface{}) {
	l.Infof(format, args...)
}

func Warn(args ...interface{}) {
	l.Warn(args...)
}

func Error(args ...interface{}) {
	l.Error(args...)
}

func Errorf(format string, args ...interface{}) {
	l.Errorf(format, args...)
}

func Fatal(args ...interface{}) {
	l.Fatal(args...)
}

func Fatalf(format string, args ...interface{}) {
	l.Fatalf(format, args...)
}
