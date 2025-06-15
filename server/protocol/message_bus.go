package protocol

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/logger"
	"sync"
	"time"
)

// DefaultMessageBus 默认消息总线实现
type DefaultMessageBus struct {
	handlers map[MessageType][]MessageHandler
	mutex    sync.RWMutex
}

// NewDefaultMessageBus 创建默认消息总线
func NewDefaultMessageBus() *DefaultMessageBus {
	return &DefaultMessageBus{
		handlers: make(map[MessageType][]MessageHandler),
	}
}

// Subscribe 订阅消息类型
func (bus *DefaultMessageBus) Subscribe(msgType MessageType, handler MessageHandler) {
	bus.mutex.Lock()
	defer bus.mutex.Unlock()

	if bus.handlers[msgType] == nil {
		bus.handlers[msgType] = make([]MessageHandler, 0)
	}
	bus.handlers[msgType] = append(bus.handlers[msgType], handler)
}

// Unsubscribe 取消订阅消息类型
func (bus *DefaultMessageBus) Unsubscribe(msgType MessageType, handler MessageHandler) {
	bus.mutex.Lock()
	defer bus.mutex.Unlock()

	handlers := bus.handlers[msgType]
	if handlers == nil {
		return
	}

	// 查找并移除handler
	for i, h := range handlers {
		if h == handler {
			bus.handlers[msgType] = append(handlers[:i], handlers[i+1:]...)
			break
		}
	}
}

// Publish 同步发布消息
func (bus *DefaultMessageBus) Publish(msg Message) error {
	bus.mutex.RLock()
	handlers := bus.handlers[msg.Type()]
	bus.mutex.RUnlock()

	if len(handlers) == 0 {
		return fmt.Errorf("no handlers for message type: %d", msg.Type())
	}

	// 依次调用所有处理器
	for _, handler := range handlers {
		if handler.CanHandle(msg.Type()) {
			_, err := handler.HandleMessage(msg)
			if err != nil {
				return fmt.Errorf("handler error: %v", err)
			}
		}
	}

	return nil
}

// PublishAsync 异步发布消息
func (bus *DefaultMessageBus) PublishAsync(msg Message) <-chan Message {
	resultChan := make(chan Message, 1)

	go func() {
		defer close(resultChan)

		bus.mutex.RLock()
		handlers := bus.handlers[msg.Type()]
		bus.mutex.RUnlock()

		if len(handlers) == 0 {
			// 发送错误消息
			errorMsg := &ErrorMessage{
				BaseMessage: NewBaseMessage(MSG_ERROR, msg.SessionID(), nil),
				Code:        1064,
				State:       "42000",
				Message:     fmt.Sprintf("no handlers for message type: %d", msg.Type()),
			}
			resultChan <- errorMsg
			return
		}

		// 依次调用所有处理器
		for _, handler := range handlers {
			if handler.CanHandle(msg.Type()) {
				response, err := handler.HandleMessage(msg)
				if err != nil {
					// 发送错误消息
					errorMsg := &ErrorMessage{
						BaseMessage: NewBaseMessage(MSG_ERROR, msg.SessionID(), nil),
						Code:        1064,
						State:       "42000",
						Message:     err.Error(),
					}
					resultChan <- errorMsg
					return
				}
				if response != nil {
					resultChan <- response
				}
			}
		}
	}()

	return resultChan
}

// AsyncMessageBus 异步消息总线（支持缓冲）
type AsyncMessageBus struct {
	*DefaultMessageBus
	messageQueue chan Message
	workers      int
	stopChan     chan struct{}
	wg           sync.WaitGroup
}

// NewAsyncMessageBus 创建异步消息总线
func NewAsyncMessageBus(bufferSize, workers int) *AsyncMessageBus {
	bus := &AsyncMessageBus{
		DefaultMessageBus: NewDefaultMessageBus(),
		messageQueue:      make(chan Message, bufferSize),
		workers:           workers,
		stopChan:          make(chan struct{}),
	}

	// 启动工作协程
	for i := 0; i < workers; i++ {
		bus.wg.Add(1)
		go bus.worker()
	}

	return bus
}

// worker 工作协程
func (bus *AsyncMessageBus) worker() {
	defer bus.wg.Done()

	for {
		select {
		case msg := <-bus.messageQueue:
			bus.processMessage(msg)
		case <-bus.stopChan:
			return
		}
	}
}

// processMessage 处理消息
func (bus *AsyncMessageBus) processMessage(msg Message) {
	bus.mutex.RLock()
	handlers := bus.handlers[msg.Type()]
	bus.mutex.RUnlock()

	for _, handler := range handlers {
		if handler.CanHandle(msg.Type()) {
			_, err := handler.HandleMessage(msg)
			if err != nil {
				// 记录错误日志
				logger.Debugf("Error processing message: %v\n", err)
			}
		}
	}
}

// PublishAsync 异步发布消息（非阻塞）
func (bus *AsyncMessageBus) PublishAsync(msg Message) <-chan Message {
	resultChan := make(chan Message, 1)

	select {
	case bus.messageQueue <- msg:
		// 消息已入队，立即返回
		go func() {
			defer close(resultChan)
			// 等待一段时间后返回成功响应
			time.Sleep(10 * time.Millisecond)
			// 这里可以根据需要返回适当的响应
		}()
	default:
		// 队列已满，返回错误
		go func() {
			defer close(resultChan)
			errorMsg := &ErrorMessage{
				BaseMessage: NewBaseMessage(MSG_ERROR, msg.SessionID(), nil),
				Code:        1040,
				State:       "08004",
				Message:     "message queue is full",
			}
			resultChan <- errorMsg
		}()
	}

	return resultChan
}

// Stop 停止异步消息总线
func (bus *AsyncMessageBus) Stop() {
	close(bus.stopChan)
	bus.wg.Wait()
	close(bus.messageQueue)
}
