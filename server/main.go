package main

import (
	"context"
	"log"
	"net"
	"os"
	"sync"

	"github.com/akshatm/grpc-chat/server/chat"
	"google.golang.org/grpc"
	glog "google.golang.org/grpc/grpclog"
)

var grpcLog glog.LoggerV2

func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)
}

type Connection struct {
	stream chat.Broadcast_CreateStreamServer
	id     string
	active bool
	error  chan error
}

type Server struct {
	Connection []*Connection
}

func (s *Server) CreateStream(pconn *chat.Connect, stream chat.Broadcast_CreateStreamServer) error {
	conn := &Connection{
		stream: stream,
		id:     pconn.User.Id,
		active: true,
		error:  make(chan error),
	}
	s.Connection = append(s.Connection, conn)
	return <-conn.error
}

func (s *Server) BroadcastMessage(ctx context.Context, msg *chat.Message) (*chat.Close, error) {
	wait := sync.WaitGroup{}
	done := make(chan int)

	for _, conn := range s.Connection {
		wait.Add(1)

		go func(msg *chat.Message, conn *Connection) {
			defer wait.Done()
			grpcLog.Info("Sending message to: ", conn.stream)
			if err := conn.stream.Send(msg); err != nil {
				grpcLog.Errorf("Error with stream: %s - Error: %v", conn.stream, err)
				conn.active = false
				conn.error <- err
			}
		}(msg, conn)
	}

	go func() {
		wait.Wait()
		close(done)
	}()
	<-done
	return &chat.Close{}, nil
}

func main() {
	var connections []*Connection

	server := &Server{connections}

	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("error creating 	the server %v", err)
	}

	grpcLog.Info("Starting server at port :8080")

	chat.RegisterBroadcastServer(grpcServer, server)
	grpcServer.Serve(listener)
}
