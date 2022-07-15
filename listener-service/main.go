package main

import (
	"fmt"
	"listener-service/event"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// try to connect RabbitMQ
	rabbitConn, err := connect()

	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	defer rabbitConn.Close()

	// Start Listening messages
	log.Println("Listening for and consuming RabbitMQ messages...")

	// create consumer
	consumer, err := event.NewConsumer(rabbitConn)
	if err != nil {
		panic(err)
	}
	// watch queue and consume
	err = consumer.Listen([]string{"log.INFO", "log.WARNING", "log.ERROR"})
	if err != nil {
		log.Println(err)
	}
}

func connect() (*amqp.Connection, error) {
	var counts int64
	var backOff = 1 * time.Second
	var connection *amqp.Connection

	for {
		c, err := amqp.Dial("amqp://guest:guest@rabbitmq")

		if err != nil {
			fmt.Println("rabbitmq is not yet ready")
			counts++
		} else {
			connection = c
			log.Println("connected to rabbit mq")
			break
		}

		if counts > 5 {
			fmt.Println(err)
			return nil, err
		}

		log.Println("backing off..")

		time.Sleep(backOff)
		continue
	}

	return connection, nil
}
