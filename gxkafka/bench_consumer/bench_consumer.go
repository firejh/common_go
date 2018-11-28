package main

import (
	"flag"
	"fmt"
	"github.com/firejh/kafka-go"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

import (
	"github.com/firejh/common_go/gxkafka"
)

// usage will print out the flag options for the server.
var usageStr = `
options:
	-brokers kafak broker l`

var consumer gxkafka.Consumer

func usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

func initSignal() {
	var (
		// signal.Notify的ch信道是阻塞的(signal.Notify不会阻塞发送信号), 需要设置缓冲
		signals = make(chan os.Signal, 1)
		ticker  = time.NewTicker(1 * time.Second)
	)
	// It is not possible to block SIGKILL or syscall.SIGSTOP
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case sig := <-signals:
			switch sig {
			case syscall.SIGHUP:

			default:

				return
			}
			// case <-time.After(time.Duration(1e9)):
		case <-ticker.C:
			//fmt.Println("tick...")
		}
	}
}

var i = 0

func msgDeal(msg *kafka.Message) {
	consumer.Commit(msg)
	i++
	//fmt.Println(string(msg.Value))
}

func errDeal(err error) {
	fmt.Println(err.Error())
}

func main() {

	var (
		brokers string
		topic   string
		err     error
	)

	//parse flag
	flag.StringVar(&brokers, "brokers", "127.0.0.1:9091", "Print kafka brokers: -brokers")
	flag.StringVar(&topic, "topic", "test_topic", "Print kafka topic: -topic")
	flag.Usage = usage
	flag.Parse()

	brokerList := strings.Split(brokers, ",")
	topicList := strings.Split(topic, ",")

	consumer, err = gxkafka.NewConsumer("0", brokerList, topicList, "test", msgDeal, nil)
	if err != nil {
		panic(err.Error())
	}

	go func(i *int) {
		last := 0
		for {
			time.Sleep(time.Second)
			qps := *i - last
			last = *i
			fmt.Println("recv " + strconv.Itoa(qps) + "/s")
		}
	}(&i)

	consumer.Start()

	initSignal()

}
