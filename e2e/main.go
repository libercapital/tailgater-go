package main

import (
	"gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/internal/amqp"
	tg_models "gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/models"
)

func main() {

	amqpClient := amqp.NewClient(
		tg_models.AmqpConfig{
			Host:     "localhost",
			User:     "guest",
			Password: "guest",
			Protocol: "AMQP",
			VHost:    "/",
		},
	)

	err := tailgater.StartFollowing(
		tg_models.DatabaseConfig{
			DbHost:     "localhost",
			DbDatabase: "postgres",
			DbUser:     "postgres",
			DbPassword: "password",
			DbPort:     "5432",
		},
		amqpClient,
	)

	if err != nil {
		panic(err)
	}

	ch := make(chan bool, 1)
	<-ch
}
