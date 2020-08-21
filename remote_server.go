package gactor2

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)
import (
	"gactor/pb"
)

type remoteServiceImpl struct{}

// todo: actor之前接收远程消息的地方
func (service *remoteServiceImpl) Request(ctx context.Context, req *pb.RequestMsg) (resp *pb.ResponseMsg, err error) {
	defer printPanicStack()

	var senderId, targetId int64
	var sender, target *Pid

	if req.Sender != nil {
		sender = &Pid{id: req.Sender.Id, systemName: req.Sender.System}
		senderId = sender.id
	} else {
		sender = NoSender
		senderId = NoSenderId
	}

	target = &Pid{id: req.Target.Id, systemName: req.Target.System}
	targetId = target.id

	system := target.System()

	// actor系统不存在
	if system == nil {
		err = fmt.Errorf("actor system not exist, %v", req.Target)
		return
	}

	// 解包消息
	msg, err := req.Data.UnPack()
	if err != nil {
		logrus.Errorf("Request req.Data.UnPack() req(%v) err %v", req, err)
		return
	}

	logrus.Debugf("Request Begin Exec req(%v), msg(%v)", req, msg)

	var (
		respMsg interface{}
	)
	if req.IsRespond {
		logrus.Debugf("Request Redirect(%v) system.Ask senderId(%d) targetId(%d) msg(%v)", req.Redirect, senderId, targetId, msg)
		respMsg, err = system.ask(senderId, targetId, msg)
	} else {
		logrus.Debugf("Request Redirect(%v) system.Tell...senderId(%d) targetId(%d) msg(%v)", req.Redirect, senderId, targetId, msg)
		err = system.tell(senderId, targetId, msg)
	}

	if err != nil {
		logrus.Errorf("Request Tell msg req(%v) err %v", req, err)
		return
	}

	if req.IsRespond {
		protoRespMsg, ok := respMsg.(proto.Message)
		if !ok {
			err = fmt.Errorf("resp msg not proto.message[actor.id=%d,system=%s] req=%v, resp=%v", target.id, target.systemName, req, respMsg)
			logrus.Errorf("Request ask msg receive err %v", err)
			return
		}

		var wrapMsg *pb.WrapMsg
		wrapMsg, err = pb.NewWrapMsg(protoRespMsg)
		if err != nil {
			logrus.Errorf("Request wrap msg err %v req(%v)", err, req)
			return nil, err
		}

		resp = &pb.ResponseMsg{
			Resper: req.Target,
			ReqId:  req.ReqId,
			Data:   wrapMsg,
		}
	} else {
		resp = &pb.ResponseMsg{
			Resper: req.Target,
			ReqId:  req.ReqId,
		}
	}
	logrus.Debugf("Request Execute successfully respmsg(%v)", resp)
	return

}

func startRemoteServer(grpcServer *grpc.Server) {
	pb.RegisterRemoteServiceServer(grpcServer, &remoteServiceImpl{})

	//reflection.Register(grpcServer)
	//
	//err = grpcServer.Serve(lis)
	//if err != nil {
	//	return err
	//}
	//
	//logrus.Infof("开始监听远程Actor....%s", address)
	//return nil
}
