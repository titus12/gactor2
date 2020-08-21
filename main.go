package gactor2

import (
	"fmt"
	"time"
)

var actorSystem *ActorSystem

type Player struct {
	id int64
}

// 玩家actor启动
func (p *Player) OnStart(ref *Ref) {
	fmt.Printf("id %d start /n", p.id)
}

// 玩家actor销毁
func (p *Player) OnDestroy(ref *Ref) {
	fmt.Printf("id %d destroy /n", p.id)
}

// 玩家actor处理消息超时
func (p *Player) OnTimeout(ref *Ref) {
	fmt.Printf("id %d timeout /n", p.id)
}

// 玩家消息的处理 handler
func (p *Player) OnProcess(ctx Context) {
	fmt.Printf("id %d process msg %v /n", p.id, ctx.Msg())
}

func buildPlayer(id int64) Handler {
	return &Player{id: id}
}

func main() {
	actorSystem = NewActorSystem("game", buildPlayer,
		//WithCluster(service),
		WithTimeout(5*time.Minute))
	actorSystem.Tell(100, 101)

}
