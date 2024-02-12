package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
)

var (
	queue_name     = "none"
	message_broker = "none"
)

func init() {

	// Message broker RabbitMQ
	if env_message_broker_url := os.Getenv("message_broker_url"); env_message_broker_url != "" {
		message_broker = env_message_broker_url
	}

	// Get Message Broker Queue name
	if env_rabbitmq_queue_name := os.Getenv("rabbitmq_queue_name"); env_rabbitmq_queue_name != "" {
		queue_name = env_rabbitmq_queue_name
	}

}

func failOnError(err error, msg string) {
	if err != nil {
			log.Panicf("%s: %s", msg, err)
	}
}


func main() {

	if message_broker != "none" || queue_name != "none" {

		// Create a new RabbitMQ connection.
		conn, err := amqp.Dial(message_broker)
		failOnError(err, "Failed to connect to RabbitMQ")
		defer conn.Close()

		ch, err := conn.Channel()
		failOnError(err, "Failed to open a channel")
		defer ch.Close()

		q, err := ch.QueueDeclare(
			queue_name, // name
			true,       // durable
			false,      // delete when unused
			false,      // exclusive
			false,      // no-wait
			nil,        // arguments
		)
		failOnError(err, "Failed to declare a queue")

		err = ch.Qos(
			1,     // prefetch count
			0,     // prefetch size
			false, // global
		)
		failOnError(err, "Failed to set QoS")

		msgs, err := ch.Consume(
			q.Name, // queue
			"",     // consumer
			false,  // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		failOnError(err, "Failed to register a consumer")

		var forever chan struct{}

		go func() {
			for d := range msgs {

				// Byte message
				message_body := string(d.Body)
				log.Printf("Received message: %s", message_body)

				d.Ack(false)
			}
		}()

		log.Printf("*** Waiting for messages ***")
		<-forever

	} else {
		fmt.Printf("No Message Broker in use!")
	}
}
