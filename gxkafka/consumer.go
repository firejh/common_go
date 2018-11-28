/*
description:	kafka-go 封装
objective:		与现有的接口统一；添加回调功能；支持多topic消费
*/
package gxkafka

import (
	"context"
	"fmt"
	"sync"
	"time"
)

import (
	"github.com/firejh/kafka-go"
	"github.com/pkg/errors"
)

// MessageCallback is a short notation of a callback function for incoming Kafka message.
type (
	Consumer interface {
		Start() error
		Commit(*kafka.Message)
		Stop()
	}

	consumer struct {
		consumerGroup string //组id
		brokers       []string
		topics        []string
		clientID      string                  //待确定，暂时不使用
		msgCb         ConsumerMessageCallback //消息回调
		errCb         ConsumerErrorCallback   //错误回调
		//ntfCb         ConsumerNotificationCallback

		readerMap       map[string]*kafka.Reader
		readerMapRWLock sync.RWMutex
		wg              sync.WaitGroup
		ctx             context.Context
	}
)

func NewConsumer(
	clientID string,
	brokers []string,
	topicList []string,
	consumerGroup string,
	msgCb ConsumerMessageCallback,
	errCb ConsumerErrorCallback,
) (Consumer, error) {
	if consumerGroup == "" || len(brokers) == 0 || len(topicList) == 0 || msgCb == nil {
		return nil, fmt.Errorf("@consumerGroup:%s, @brokers:%s, @topicList:%s, msgCb:%v",
			consumerGroup, brokers, topicList, msgCb)
	}

	return &consumer{
		consumerGroup: consumerGroup,
		brokers:       brokers,
		topics:        topicList,
		clientID:      clientID,
		msgCb:         msgCb,
		errCb:         errCb,
		readerMap:     make(map[string]*kafka.Reader),
		ctx:           context.Background(),
	}, nil
}

func (c *consumer) Start() error {
	conf := kafka.ReaderConfig{
		Brokers:        c.brokers,
		GroupID:        c.consumerGroup,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		MaxWait:        time.Millisecond,
		CommitInterval: time.Millisecond * 10,
	}

	for _, v := range c.topics {
		if _, ok := c.readerMap[v]; !ok {
			fmt.Println("creat reader " + v)
			c.readerMapRWLock.Lock()
			conf.Topic = v
			c.readerMap[v] = kafka.NewReader(conf) //自动run后后台启动启动grouting去server拉取msg到本地队列
			c.readerMapRWLock.Unlock()
		}
	}

	c.run()

	return nil
}

func (c *consumer) Commit(message *kafka.Message) {
	if v, ok := c.readerMap[message.Topic]; ok {
		v.CommitMessages(c.ctx, *message)
	} else {
		if nil != c.errCb {
			err := errors.New("unknow topic {" + message.Topic + "}")
			c.errCb(err)
		}
	}
}

func (c *consumer) Stop() {

	for _, v := range c.readerMap {
		go v.Close()
	}

	c.wg.Wait()
}

func (c *consumer) run() {
	for k, v := range c.readerMap {
		fmt.Println("run " + k)
		go func() {
			for {
				m, err := v.FetchMessage(c.ctx) //内部是阻塞，且调用Close后会返回err
				if err != nil {
					if c.errCb != nil {
						c.errCb(err)
					}
					time.Sleep(time.Millisecond)
					//break
					continue
				} else {
					c.msgCb(&m)
				}
			}
		}()
	}
}
