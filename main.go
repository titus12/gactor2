package main

import (
	"fmt"
	"gactor/actor"
	testmsg "gactor/test_msg"
	"log"
	"time"
)

var actorSystem *actor.ActorSystem

type Player struct {
	id int64
}

// 玩家actor启动
func (p *Player) OnStart(ref *actor.Ref) {
	fmt.Printf("id %d start /n", p.id)
}

// 玩家actor销毁
func (p *Player) OnDestroy(ref *actor.Ref) {
	fmt.Printf("id %d destroy /n", p.id)
}

// 玩家actor处理消息超时
func (p *Player) OnTimeout(ref *actor.Ref) {
	fmt.Printf("id %d timeout /n", p.id)
}

// 玩家消息的处理 handler
func (p *Player) OnProcess(ctx actor.Context) {
	fmt.Printf("id %d process msg %v /n", p.id, ctx.Msg())
}

func buildPlayer(id int64) actor.Handler {
	return &Player{id: id}
}

func main() {
	/*if err := Initialize(WithService(grpcServer));err != nil{
		log.Fatal("actor initialization failed")
	}*/
	if err := actor.Initialize(); err != nil {
		log.Fatal("actor initialization failed")
	}

	actorSystem = actor.NewActorSystem("gameActor", buildPlayer,
		//WithCluster(service),
		actor.WithTimeout(5*time.Minute))
	actorSystem.Tell(100, 100, &testmsg.RunMsg{ReqId: 1000, TargetId: 100})

}
