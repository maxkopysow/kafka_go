package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"math/rand"
)

import (
	"strconv"
	"time"
)

const (
	topic          = "my-kafka-topic"
	broker1Address = "localhost:9093"
	broker2Address = "localhost:9094"
	broker3Address = "localhost:9095"
)

func Produce(ctx context.Context) {
	// a counter
	i := 0

	//the writer
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address, broker2Address, broker3Address},
		Topic:   topic,
	})

	for {
		ine := getRandomString()
		err := w.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(ine),
		})

		if err != nil {
			panic("could not write message " + err.Error())
		}

		fmt.Println("writes: ", ine)
		i++

		time.Sleep(time.Second)
	}

}

func getRandomString() string {
	//length := 8
	chars := [10]string{"-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5"}
	//var b = ""
	//for i := 0; i < length; i++ {
	//	b += chars[rand.Intn(2)]
	//}
	//fmt.Println(b)
	return chars[rand.Intn(10)]
}

func Consume(ctx context.Context, number string) {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker1Address, broker2Address, broker3Address},
		Topic:       topic,
		GroupID:     "my-group",
		MinBytes:    5,
		MaxBytes:    1e6,
		MaxWait:     3 * time.Second,
		StartOffset: kafka.FirstOffset,
	})

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}

		var recNumber, _ = strconv.Atoi(string(msg.Value))
		//fmt.Println(string(msg.Value))

		if recNumber > 0 && number == "1" {
			fmt.Println("reader "+number+" received : ", string(msg.Value))
		}
		if recNumber < 0 && number == "2" {
			fmt.Println("reader "+number+" received : ", string(msg.Value))
		}
	}
}

func main() {

	//getRandomString()
	ctx := context.Background()

	go Produce(ctx)
	go Consume(ctx, "1")
	Consume(ctx, "2")

}
