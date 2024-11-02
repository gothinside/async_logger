package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// Некоторая бизнеслогика
type BizLogic struct {
	UnimplementedBizServer
}

func CreateNewBiz() *BizLogic {

	return &BizLogic{}
}

func (biz *BizLogic) Add(ctx context.Context, in *Nothing) (*Nothing, error) {
	return in, nil
}

func (biz *BizLogic) Check(ctx context.Context, in *Nothing) (*Nothing, error) {
	return in, nil
}

func (biz *BizLogic) Test(ctx context.Context, in *Nothing) (*Nothing, error) {
	return in, nil
}

type admin struct {
	CM *ChanManager
	UnimplementedAdminServer
}

func CreateNewAdmin(CM *ChanManager) *admin {
	adm := &admin{CM: CM}
	return adm
}

// Сервис логгирования
func (adm *admin) Logging(in *Nothing, ALStream Admin_LoggingServer) error {
	ThisGoNum, ch := adm.CM.OpenChan()

	for {
		select {
		case <-ALStream.Context().Done():
			adm.CM.CloseChan(ThisGoNum)
			return nil
		case c := <-ch:
			ALStream.Send(c)
		}
	}
}

// Создает канал с уникальным номером
func (CM *ChanManager) OpenChan() (int, chan *Event) {
	CM.mu.Lock()
	defer CM.mu.Unlock()
	ThisGoNum := CM.OpenConns
	ch := make(chan *Event)

	CM.ConnChan[ThisGoNum] = ch
	CM.OpenConns++

	return ThisGoNum, ch
}

// Закрывает канал
func (CM *ChanManager) CloseChan(ThisGoNum int) {
	CM.mu.Lock()
	delete(CM.ConnChan, ThisGoNum)
	CM.mu.Unlock()
}

// Сервис сбора статистики
func (adm *admin) Statistics(SI *StatInterval, ALStream Admin_StatisticsServer) error {
	methods := make(map[string]uint64)
	consumers := make(map[string]uint64)

	ThisGoNum, ch := adm.CM.OpenChan()

	t := time.NewTicker(time.Duration(SI.IntervalSeconds) * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ALStream.Context().Done():
			adm.CM.CloseChan(ThisGoNum)
			return nil
		case event := <-ch:
			consumer := event.Consumer
			method := event.Method
			methods[method]++
			consumers[consumer]++
		case <-t.C:
			stat := &Stat{Timestamp: 0, ByMethod: methods, ByConsumer: consumers}
			//fmt.Println(stat, ThisGoNum)
			ALStream.Send(stat)
			methods = make(map[string]uint64)
			consumers = make(map[string]uint64)
		}
	}
}

type ChanManager struct {
	OpenConns int
	ConnChan  map[int]chan *Event
	mu        *sync.RWMutex
}

func CreateConnInfo() *ChanManager {
	return &ChanManager{0, make(map[int]chan *Event), &sync.RWMutex{}}
}

// Посылает объект типа ивент всем открытым каналам
func (CM *ChanManager) SendMessages(event *Event) {
	CM.mu.RLock()
	defer CM.mu.RUnlock()
	for _, ch := range CM.ConnChan {
		ch <- event
	}
}

type middleware struct {
	UsersAllowMethods map[string][]string
	CM                *ChanManager
}

func CreateMiddleware(UsersAllowMethods map[string][]string, cons *ChanManager) *middleware {
	return &middleware{UsersAllowMethods, cons}
}

func CreateNewEvent(md metadata.MD, info string) *Event {
	event := &Event{}
	consumer := md.Get("consumer")
	if consumer == nil {
		event.Consumer = ""
	} else {
		event.Consumer = consumer[0]
	}
	event.Host = "127.0.0.1:" //заглушка
	event.Timestamp = 0       //заглушка
	event.Method = info

	return event
}

// Проверяет права отправителя
func (middl *middleware) CheckConsumerData(ctx context.Context, info string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	event := CreateNewEvent(md, info)
	middl.CM.SendMessages(event)

	if event.Consumer == "" {
		return grpc.Errorf(codes.Unauthenticated, "not recv consumer")
	}

	methods, ok := middl.UsersAllowMethods[md.Get("consumer")[0]]
	if !ok {
		return grpc.Errorf(codes.Unauthenticated, "unknow user")
	}

	RecvServiceName := strings.Split(strings.Trim(info, "/"), "/")[0]
	for _, method := range methods {
		MethodParts := strings.Split(strings.Trim(method, "/"), "/")
		service, servMethod := MethodParts[0], MethodParts[1]
		if (method == info) || (service == RecvServiceName && servMethod == "*") {
			return nil
		}
	}

	return grpc.Errorf(codes.Unauthenticated, "forbiden for this user")

}

// Перехватчик потоковых запросов
func (middl *middleware) streamInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	err := middl.CheckConsumerData(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}

	return handler(srv, ss)
}

// Перехватчик обычных запросов
func (middl *middleware) authInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	err := middl.CheckConsumerData(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}

	reply, err := handler(ctx, req)
	return reply, err

}

func AsyncServing(ctx context.Context, server *grpc.Server, lis net.Listener) {
	go server.Serve(lis)
	<-ctx.Done()
	server.Stop()
}

func StartMyMicroservice(ctx context.Context, LisADDR string, AlC string) error {
	lis, err := net.Listen("tcp", LisADDR)
	if err != nil {
		lis.Close()
		log.Println("can't listen port", err)
	}

	m := make(map[string][]string)
	err = json.Unmarshal([]byte(AlC), &m)
	if err != nil {
		lis.Close()
		return err
	}

	cons := CreateConnInfo()
	midl := CreateMiddleware(m, cons)
	server := grpc.NewServer(grpc.UnaryInterceptor(midl.authInterceptor), grpc.StreamInterceptor(midl.streamInterceptor))
	RegisterBizServer(server, CreateNewBiz())
	RegisterAdminServer(server, CreateNewAdmin(cons))

	go AsyncServing(ctx, server, lis)

	return nil
}
