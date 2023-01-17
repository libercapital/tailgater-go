package main

import (
	"fmt"

	"gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/v2"
)

func main() {
	amqpConnTailgater := amqpConn()

	err := tailgater.StartFollowing(
		tailgater.DatabaseConfig{
			DbHost:     "localhost",
			DbDatabase: "payment-link",
			DbUser:     "postgres",
			DbPassword: "postgres",
			DbPort:     "5432",
		},
		amqpConnTailgater.(tailgater.Tailgater),
	)

	if err != nil {
		panic(err)
	}

	ch := make(chan bool, 1)
	<-ch
}

type AMQP interface {
	PostMessage()
}

type amqp struct{}

// PostMessage implements AMQP
func (*amqp) PostMessage() {
	panic("unimplemented")
}

// Send implements AMQP
func (*amqp) Tail(message tailgater.TailMessage) {
	fmt.Printf("mensagem outbox %+v", message)
}

func amqpConn() AMQP {
	return &amqp{}
}
