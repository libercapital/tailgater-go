package tail

import (
	"github.com/rs/zerolog/log"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/envloader.git"
)

type EnvVars struct {
	AmqpHost     string `env:"AMQP_HOST"`
	AmqpUser     string `env:"AMQP_USER"`
	AmqpPassword string `env:"AMQP_PSWD"`
	AmqpVHost    string `env:"AMQP_VHOST,optional"`
	AmqpProtocol string `env:"AMQP_PROTOCOL,optional"`
	DbHost       string `env:"DB_HOST"`
	DbUser       string `env:"DB_USER"`
	DbPassword   string `env:"DB_PSWD"`
	DbName       string `env:"DB_NAME"`
	DbPort       string `env:"DB_PORT"`
	LogLevel     string `env:"LOG_LEVEL,optional"`
}

var Env EnvVars

func LoadEnv(filenames ...string) {
	log.Info().Msg("Loading env variables")
	err := envloader.Load(&Env, filenames...)
	if err != nil {
		log.Fatal().Stack().Err(err).Msg("Failed to load env variables")
	}
	log.Debug().Interface("env", Env).Msg("Successfully loaded env variables")
}
