package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

import (
	"github.com/firejh/common_go/gxkafka"
)

// usage will print out the flag options for the server.
var usageStr = `
options:
	-brokers kafak broker l`

func usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

func main() {

	var (
		brokers string
		topic   string
		times   int
		resend  int
		sleep   int
	)

	//parse flag
	flag.StringVar(&brokers, "brokers", "127.0.0.1:9091", "Print kafka brokers: -brokers")
	flag.StringVar(&topic, "topic", "test_topic", "Print kafka topic: -topic")
	flag.IntVar(&times, "times", 10, "Print send times: -times")
	flag.IntVar(&resend, "resend", 1, "Print resend times: -resend")
	flag.IntVar(&sleep, "sleep", 1000, "Print usleep time: -sleep")
	flag.Usage = usage
	flag.Parse()

	brokerList := strings.Split(brokers, ",")
	fmt.Println(brokerList)

	producer, err := gxkafka.NewAsyncProducer("1", brokerList, 0, false, 0, gxkafka.CompressionNone)
	if err != nil {
		panic(err.Error())
	}

	sendData := "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
	i := 0
	go func(i *int) {
		last := 0
		for {
			time.Sleep(time.Second)
			qps := *i - last
			last = *i
			fmt.Println("send " + strconv.Itoa(qps) + "/s")
		}
	}(&i)

LOOP:
	for {
		i++
		err := producer.SendBytes(topic, []byte(strconv.Itoa(i)), []byte(sendData))
		if err != nil {
			fmt.Print(err.Error())
		}
		if i%100000 == 0 {
			fmt.Println("send...\n")
		}

		if i > times {
			break LOOP
		}
	}

}
