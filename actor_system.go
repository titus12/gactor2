package gactor2

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"math/rand"
	"sync"
	"time"
)

const DefaultQueueSize = 1024                  //默认每个actor的队列长度
const DefaultRequestTimeout = 10 * time.Second //默认的请求超时时间
const DefaultWaitTimeout = 10 * time.Second

const (
	Runnable = iota
	Running
	Destroying
	Destroyed
)

var (
	// 重定向错误，网络层在投递消息，返回这个错误时，说明需要网络层重定向
	//RedirectErr = errors.New("")
	// 根上下文
	rootContext context.Context
	rootCancel  context.CancelFunc

	// 存放所有System的容器
	systemMap sync.Map
	//grpcServer *grpc.Server
)

type RedirectErr struct {
	err  string
	node string
}

func NewRedirectErr(err string, node string) error {
	return &RedirectErr{err: err, node: node}
}

func (e *RedirectErr) Error() string {
	return fmt.Sprintf("%s,%s", e.err, e.node)
}

// 构建方法
type BuildFunc func(id int64) Handler

const NoSenderId = -1

var (
	NoSender = (*Pid)(nil)
)

// 一个actor的方法消息，消息本身就是方法(只适应于本地调用)
type ProcessMsgFunc func(self *Ref)

// 处理者，一个结构体实现这些接口方法，就是一个有效的actor
type Handler interface {
	OnStart(ref *Ref)   //actor创建后触发
	OnDestroy(ref *Ref) //acotr摧毁前触发
	OnTimeout(ref *Ref) //actor超时后触发，超时后立即会触发OnDestroy
	// 所有非系统消息，非方法消息(ProcessMsgFunc)都会在这个接口中被接收到
	OnProcess(ctx Context)
}

// 一个system表示一个对actor进行管理的组织，这个组织在一个进程中可以有多个
// 为什么会有多个？ 我们通常把一组有相同行为方式的结构组织成一个system
// 比如所有玩家,比如所有公会等.....
// 一个系统中会有一个常驻
type ActorSystem struct {
	//actor系统的名称
	name string

	//存放actorRef的
	container sync.Map

	//系统actor(常驻), 常驻actor会每隔20秒收到一次ping消息
	systemActor *Ref

	ctx    context.Context
	cancel context.CancelFunc

	//超时时间
	timeout time.Duration

	// 处理消息花费时间
	alarm time.Duration

	// 构建建actor的方法
	builder BuildFunc

	// actor在集群中的位置信息
	cluster Cluster
}

// 新建一个actor系统，如果存在相同名字的actor系统，则会抛出panic,这会使得整个进程崩溃
// 一般Actor系统都是在系统启动时构建，所以这里如果一个进程中准备包含多个Actor系统，则要
// 编程人员自行保证不会有重复.....
// 系统actor是没有超时消息的......
// 调用 WithPingInterval 会开启actor的ping消息，如果想系统actor也存在ping消息，
// 请在WithSystemHandler之前调用WithPingInterval
func NewActorSystem(name string, builder BuildFunc, opts ...systemOptFunc) *ActorSystem {
	_, ok := systemMap.Load(name)
	if ok {
		logrus.Panicf("不能存在重复的ActorSystem....")
	}

	if rootContext == nil {
		logrus.Panicf("没有进行初始化......")
	}

	if builder == nil {
		logrus.Panicf("builder: 不能为空.....")
	}

	// 做进一步的检查，尽最大可能保证传进来的参数是正确的。
	h := builder(rand.Int63n(5000))
	if h == nil {
		logrus.Panicf("builder: 有问题，构建出来的handler为空....")
	}

	// actor系统上下文，通过进程中actor体系生成
	systemCtx, systemCancel := context.WithCancel(rootContext)

	system := &ActorSystem{
		name:    name,
		ctx:     systemCtx,
		cancel:  systemCancel,
		builder: builder,
		alarm:   time.Millisecond * 5,
	}

	for _, opt := range opts {
		opt(system)
	}

	systemMap.Store(name, system)
	return system
}

// 为进程初始化actor体系
func Initialize(opts ...optFunc) error {
	rootContext, rootCancel = context.WithCancel(context.Background())
	for _, opt := range opts {
		opt()
	}
	return nil
}

// 为进程关闭actor体系
func Close() {
	rootCancel()
}

// 获取系统
func GetActorSystem(name string) *ActorSystem {
	a, ok := systemMap.Load(name)
	if !ok {
		return nil
	}
	return a.(*ActorSystem)
}

type optFunc func()

// 开启远程服务
func WithService(grpcServer *grpc.Server) optFunc {
	return func() {
		startRemoteServer(grpcServer)
	}
}

// 系统选项
type systemOptFunc func(*ActorSystem)

// 设置超时时间，如果不设置，则没有超时事件
func WithTimeout(timeout time.Duration) systemOptFunc {
	return func(s *ActorSystem) {
		s.timeout = timeout
	}
}

func WithAlarm(time time.Duration) systemOptFunc {
	return func(s *ActorSystem) {
		s.alarm = time
	}
}

// 设置系统handler，如果不设置，则没有系统actor
func WithHandler(handler Handler) systemOptFunc {
	return func(s *ActorSystem) {
		actorCtx, actorCancel := context.WithCancel(s.ctx)
		s.systemActor = newActor(actorCtx, actorCancel, s, 0, handler)
		s.systemActor.start()
	}
}

// 设置集群托管-保存actor所在节点位置信息
func WithCluster(cluster Cluster) systemOptFunc {
	return func(s *ActorSystem) {
		s.cluster = cluster
	}
}

// 构建一个新的ref,如果存在，则返回原来的，如果不存在，则新建
func (system *ActorSystem) NewRef(id int64) (*Ref, bool) {
	val, ok := system.container.Load(id)
	if ok {
		return val.(*Ref), ok
	}

	actorCtx, actorCancel := context.WithCancel(system.ctx)
	handler := system.builder(id)
	actorRef := newActor(actorCtx, actorCancel, system, id, handler)

	actual, loaded := system.container.LoadOrStore(id, actorRef)
	if loaded {
		// 存在相同的actor,停止掉声明的，然后返回原来的
		actorRef.cancel()
		return actual.(*Ref), loaded
	}
	//不存在，存储到map里了。
	actorRef.start()
	return actorRef, false
}

// 把actor容器中的所有id克隆一份出来
func (system *ActorSystem) Ids() []int64 {
	var ids []int64
	system.container.Range(func(key, value interface{}) bool {
		ids = append(ids, key.(int64))
		return true
	})
	return ids
}

// 根据id拿到actor的引用
// 这里会返回空引用, 如果不存在....
func (system *ActorSystem) Ref(id int64) *Ref {
	ref, ok := system.container.Load(id)
	if ok {
		return ref.(*Ref)
	}
	return nil
}

func (system *ActorSystem) NewPid(id int64) *Pid {
	if id < 0 {
		return NoSender
	}
	return &Pid{
		systemName: system.name,
		id:         id,
		system:     system,
	}
}

func NewPid(system *ActorSystem, id int64) *Pid {
	return system.NewPid(id)
}

// 1. 通过网络向actor投送消息(无响应) - 重定向交回网关 (msg = context)
// 2. 内部发送消息（无响应） - 重定向（由gs直接定向到gs) (msg != context)
// 3. 内部发送消息（有响应） - 重定向（由gs直接定向到gs) (msg != context)

// 投递消息的工作流程
func workflow(system *ActorSystem, target int64, localProcess func(),
	remote func(node string, _conn *grpc.ClientConn), errHandler func(_err error)) {

	logrus.Debugf("workflow begin system(%s) target(%d) net(%v) localProcess(%v) redirect(%v) errHandler(%v)",
		system.name, target, localProcess, remote, errHandler)

	ref := system.Ref(target)
	if ref == nil {
		//需要从etcd或redis获取自己的目标节点信息
		local, node, conn, err := system.cluster.ActorInfo(target)
		if err != nil {
			logrus.WithError(err).Errorf("workflow: 获取玩家所在节点信息错误... target: %d", target)
			errHandler(err)
			return
		}
		if !local {
			remote(node, conn)
		}

	}
	localProcess()
}

func (system *ActorSystem) checkRedirect(sender int64) error {
	self := system.Ref(sender)
	if self == nil {
		local, node, _, err := system.cluster.ActorInfo(sender)
		if err != nil {
			logrus.WithError(err).Errorf("workflow: sender 获取玩家所在节点信息错误... target: %d", sender)
			return err
		}
		if !local {
			// 重定向
			logrus.Debugf("workflow actor(%d) redirect to %s[status=%d]", sender, node)
			return &RedirectErr{"redirect err", node}
		}
	}
	return nil
}

func (system *ActorSystem) ask(sender, target int64, msg interface{}) (resp interface{}, err error) {
	senderPid := system.NewPid(sender)
	targetPid := system.NewPid(target)
	workflow(system, target, func() {
		proxy := newProxyContext(system, senderPid, targetPid, msg, true, nil)
		resp, err = proxy.request()
	}, func(node string, _conn *grpc.ClientConn) {
		proxy := newProxyContext(system, senderPid, targetPid, msg, true, _conn)
		resp, err = proxy.request()
	}, func(_err error) {
		err = _err
	})
	return
}

// 内部使用的消息投递，消息投递实际上是投递一个Context接口的实现
func (system *ActorSystem) tell(sender, target int64, msg interface{}) (err error) {
	workflow(system, target, func() {
		ref, _ := system.NewRef(target)
		err = ref.pushMsg(msg)
	}, func(node string, _conn *grpc.ClientConn) {
		senderPid := system.NewPid(sender)
		targetPid := system.NewPid(target)
		proxy := newProxyContext(system, senderPid, targetPid, msg, false, _conn)
		_, err = proxy.request()
	}, func(_err error) {
		err = _err
	})
	return
}

// 发送消息带响应
func (system *ActorSystem) Ask(sender, target int64, msg interface{}) (resp interface{}, err error) {
	logrus.Debugf("Ask begin sender(%d) target(%d) msg(%v)", sender, target, msg)
	if err := system.checkRedirect(sender); err != nil {
		return nil, err
	}
	return system.ask(sender, target, msg)
}

// 发送消息
func (system *ActorSystem) Tell(sender, target int64, msg interface{}) error {
	if err := system.checkRedirect(sender); err != nil {
		return err
	}
	return system.tell(sender, target, msg)
	/*
		switch v := msg.(type) {
		case Context:
			logrus.Debugf("Tell msg is Context sender: %d, target: %d, msg: %v", sender, target, msg)
			return system.tell(sender, target, v)
		case ProcessMsgFunc:
			return errors.New("nonsupport ProcessMsgFunc type, need Ref type")
		default:
			logrus.Debugf("Tell ctx is defaultContext sender: %d, target: %d, msg: %v", sender, target, msg)
			ctx := newDefaultContext(system.NewPid(sender), system.NewPid(target), system, msg, nil)
			return system.tell(sender, target, ctx)
		}*/
}
