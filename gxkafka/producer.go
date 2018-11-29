/*
description:	kafka-go 封装
objective:		与现有的接口统一；添加回调功能；支持多topic生产
*/
package gxkafka

import (
	"encoding/json"
	"fmt"
	"sync"
)

import (
	"github.com/firejh/kafka-go"
	"golang.org/x/net/context"
)

func getMsg(msg *kafka.Message) {
	fmt.Println(msg)
}

type (
	Producer interface {
		SendMessage(topic string, key interface{}, message interface{}) error
		SendBytes(topic string, key []byte, message []byte) error
		Stop()
	}

	producer struct {
		producerMap      map[string]*kafka.Writer
		producerMapMutex sync.Mutex
		clientID         string
		brokers          []string
		compressionType  CompressionCodec
		ctx              context.Context
		syncProduceResultCallback func(msg kafka.Message, err error)
	}

	//压缩类型
	CompressionCodec int8
)

//压缩格式
const (
	CompressionNone   CompressionCodec = 0
	CompressionGZIP   CompressionCodec = 1
	CompressionSnappy CompressionCodec = 2
	CompressionLZ4    CompressionCodec = 3
)

func NewAsyncProducer(
	clientID string,
	brokers []string,
	partitionMethod int,
	waitForAllAck bool,
	updateMetaDataInterval int,
	compressionType CompressionCodec,
	syncProduceResultCallback func(msg kafka.Message, err error),
) (Producer, error) {
	if clientID == "" || brokers == nil || len(brokers) == 0 ||
		compressionType < CompressionNone || CompressionLZ4 < compressionType {
		return &producer{}, fmt.Errorf("@clientID:%s, @brokers:%s", clientID, brokers)
	}

	return &producer{
		clientID:        clientID,
		brokers:         brokers,
		compressionType: compressionType,
		producerMap:     make(map[string]*kafka.Writer),
		ctx:             context.Background(),
		syncProduceResultCallback:	syncProduceResultCallback,
	}, nil
}

func (p *producer) SendMessage(topic string, key interface{}, message interface{}) (err error) {
	kMsg := kafka.Message{}
	msg, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("cannot marshal message %v: %v", message, err)
	}
	kMsg.Value = msg
	if key != nil {
		keyByte, err := json.Marshal(key)
		if err != nil {
			return fmt.Errorf("cannot marshal key%v: %v", key, err)
		}

		kMsg.Key = keyByte
	}

	var writer *kafka.Writer
	p.producerMapMutex.Lock()
	if v, ok := p.producerMap[topic]; ok {
		writer = v
	} else {
		writer = kafka.NewWriter(kafka.WriterConfig{
			Brokers:  p.brokers,
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
			Async:    true,
			SyncResultCallBack: p.syncProduceResultCallback,
		})
		p.producerMap[topic] = writer
	}
	writer.WriteMessages(p.ctx, kMsg)
	p.producerMapMutex.Unlock()

	return nil
}

//写入加锁可以优化
func (p *producer) SendBytes(topic string, key []byte, message []byte) (err error) {
	kMsg := kafka.Message{}
	kMsg.Key = key
	kMsg.Value = message

	var writer *kafka.Writer
	p.producerMapMutex.Lock()
	if v, ok := p.producerMap[topic]; ok {
		writer = v
	} else {
		writer = kafka.NewWriter(kafka.WriterConfig{
			Brokers:  p.brokers,
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
			Async:    true,
		})
		fmt.Println("create new topic writer " + topic)
		p.producerMap[topic] = writer
	}
	writer.WriteMessages(p.ctx, kMsg)
	p.producerMapMutex.Unlock()

	return nil
}

func (p *producer) Stop() {
	for _, v := range p.producerMap {
		v.Close()
	}
}
