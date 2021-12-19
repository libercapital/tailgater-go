package main

import (
	"gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git"
	tg_models "gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/models"
)

func main() {

	err := tailgater.StartFollowing(
		tg_models.DatabaseConfig{
			DbHost:     "localhost",
			DbDatabase: "postgres",
			DbUser:     "postgres",
			DbPassword: "password",
			DbPort:     "5432",
		},
		tg_models.AmqpConfig{
			Host:     "localhost",
			User:     "guest",
			Password: "guest",
			Protocol: "AMQP",
			VHost:    "/",
		},
	)

	if err != nil {
		panic(err)
	}

	ch := make(chan bool, 1)
	<-ch
}
