package main

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
)

func main() {
	ctx, finish := context.WithCancel(context.Background())
	var listenAddr string = "127.0.0.1:8082"
	var ACLData string = `{
		"logger1":          ["/main.Admin/Logging"],
		"logger2":          ["/main.Admin/Logging"],
		"stat1":            ["/main.Admin/Statistics"],
		"stat2":            ["/main.Admin/Statistics"],
		"biz_user":         ["/main.Biz/Check", "/main.Biz/Add"],
		"biz_admin":        ["/main.Biz/*"],
		"after_disconnect": ["/main.Biz/Add"]`
	go StartMyMicroservice(ctx, listenAddr, ACLData)
	fmt.Println("Server start")
	defer finish()
	grpcConn, err := grpc.Dial(listenAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
	}
	defer grpcConn.Close()
}
