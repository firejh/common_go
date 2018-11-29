package gxkafka

import (
	"fmt"
)
import (
	"github.com/firejh/kafka-go"
	"github.com/wvanbergen/kazoo-go"
)

// call back
type (

	// Consumer will invoke ProduceMessageCallback when got message
	ConsumerMessageCallback func(msg *kafka.Message)
	// Consumer will invoke ConnsumerErrorCallbackunc when got message failed and got unexpected err
	ConsumerErrorCallback func(error)
)

func dftConsumerErrorCallback(err error) {
	fmt.Println(err.Error())
}

func dftSyncProducerCallback(msg kafka.Message, err error) {
	if nil != err {
		fmt.Println("producer err: " + err.Error() + ", key: " + string(msg.Key))
	} else {
		fmt.Println("producer success" + ", key: " + string(msg.Key))
	}
}

func GetBrokerList(zkHosts string) ([]string, error) {
	var (
		config  = kazoo.NewConfig()
		zkNodes []string
	)

	// fmt.Println("zkHosts:", zkHosts)
	zkNodes, config.Chroot = kazoo.ParseConnectionString(zkHosts)
	kz, err := kazoo.NewKazoo(zkNodes, config)
	if err != nil {
		return nil, err
	}
	defer kz.Close()

	brokerList, err := kz.BrokerList()
	// fmt.Printf("broker list:%#v\n", brokerList)
	if err != nil {
		return nil, err
	}

	return brokerList, nil
}
