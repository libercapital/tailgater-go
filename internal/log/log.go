package log

import (
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
)

const (
	FatalLevel = "fatal"
	ErrorLevel = "error"
	WarnLevel  = "warn"
	InfoLevel  = "info"
	DebugLevel = "debug"
)

func Config(logLevel string) {
	//Line to config the stackTrace error logging
	var zeroLogLevel zerolog.Level
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	level := strings.ToLower(logLevel)

	switch level {
	case FatalLevel:
		zeroLogLevel = zerolog.FatalLevel
	case ErrorLevel:
		zeroLogLevel = zerolog.ErrorLevel
	case WarnLevel:
		zeroLogLevel = zerolog.WarnLevel
	case DebugLevel:
		zeroLogLevel = zerolog.DebugLevel
	case InfoLevel:
	default:
		zeroLogLevel = zerolog.InfoLevel
	}

	zerolog.SetGlobalLevel(zeroLogLevel)
}
