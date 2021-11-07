package main

import (
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
	logger "gitlab.com/tailgater/internal/log"
	"gitlab.com/tailgater/internal/tail"
)

func main() {

	rootdir, err := os.Getwd()
	if err != nil {
		log.Fatal().Stack().Err(err).Msg("failure to get rootdir")
	}
	tail.LoadEnv(filepath.Join(rootdir, ".env"))
	logger.Config(tail.Env.LogLevel)
	err = tail.StartFollowing()
	if err != nil {
		log.Fatal().Stack().Err(err)
	}
}
