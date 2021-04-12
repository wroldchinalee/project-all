### Akka笔记02

从前面的学习，我们已经简单地了解了Actors。在这篇笔记中，我们将介绍Actor的消息传递。作为例子，我们将继续使用前面介绍的 Student-Teacher模型。
　　在Actor消息传递的前部分，我们将创建Teacher Actor而不是Student Actor，我们将用一个称为StudentSimulatorApp的主程序。

#### 详细回顾Student-Teacher模型

我们现在考虑消息从StudentSimulatorApp单独发送到TeacherActor。这里所说的StudentSimulatorApp只不过是一个简单的主程序。

![img](https://www.iteblog.com/pic/akka/TeacherRequestFlowSimulatedApp.png)

这副图片解释如下：
　　1、Student创建了一些东西，称为ActorSystem；
　　2、他用ActorSystem来创建一个ActorRef，并将QuoteRequest message发送到ActorRef 中（到达TeacherActor的一个代理）；
　　3、ActorRef 将message单独传输到Dispatcher；
　　4、Dispatcher将message按照顺序保存到目标Actor的MailBox中；
　　5、然后Dispatcher将Mailbox放在一个Thread 中（更多详情将会在下节中进行介绍）；
　　6、MailBox按照队列顺序取出消息，并最终将它递给真实的TeacherActor接受方法中。
就像我所说的，不用担心。我们现在来一步一步地了解详情，当了解完详情之后，你可以返回来回顾上面六步。



#### StudentSimulatorApp程序

我们将通过StudentSimulatorApp程序来启动JVM，并且初始化ActorSystem。

![img](https://www.iteblog.com/pic/akka/StudentSimulatorApp.png)

正如我们从图中了解到，StudentSimulatorApp程序
　　1、创建一个ActorSystem；
　　2、用创建好的ActorSystem来创建一个通往Teacher Actor的代理 (ActorRef)
　　3、将QuoteRequest message 发送到这个代理中。
　　让我们单独探讨这三点。



##### 1、创建一个ActorSystem

　　ActorSystem是进入Actor世界的切入点，通过ActorSystem你可以创建和停止Actors，甚至关掉整个Actor环境！
　　另一方面，Actor是一个体系，ActorSystem类似于java.lang.Object or scala.Any，能够容纳所有的Actor!它是所有的Actor的父类。当你创建一个Actor，你可以用ActorSystem的actorOf方法。

![img](https://www.iteblog.com/pic/akka/ActorSystemActorCreation.png)

　初始化ActorSystem的代码类似下面：

```scala
val system=ActorSystem("UniversityMessageSystem")
```

UniversityMessageSystem就是你给ActorSystem取得名字。



##### 2、为Teacher Actor创建代理

　　让我们看下下面的代码片段：

```scala
val teacherActorRe:ActorRef=actorSystem.actorOf(Props[TeacherActor])
```

　　 actorOf是ActorSystem中创建Actor的方法。但是正如你所看到的，它并不返回我们所需要的TeacherActor对象，它的返回类型为ActorRef。
　　 ActorRef为真实Actor的充当代理，客户端并不直接和Actor进行通信。这就是Actor Model中的处理方式，该方式避免直接进入TeacherActor或者任何Actor中的任何custom/private方法或者变量。
　　你仅仅将消息发送到ActorRef ，该消息最终发送到你的Actor中，你无法直接和你的Actor进行直接通信。如果你那样做，有人会恨你到死！！（这么严重？）

![img](https://www.iteblog.com/pic/akka/ActorRef.png)



##### 3、将QuoteRequest message 发送到代理中

　　 你仅仅告诉(tell)QuoteRequest message到ActorRef中，Actor中的tell方法是! 如下：

```scala
//send a message to the Teacher Actor
  teacherActorRef!QuoteRequest
```

StudentSimulatorApp 的完整代码如下：

```scala
package me.rerun.akkanotes.messaging.actormsg1
 
import akka.actor.ActorSystem  
import akka.actor.Props  
import akka.actor.actorRef2Scala  
import me.rerun.akkanotes.messaging.protocols.TeacherProtocol._
 
 
object StudentSimulatorApp extends App{
 
  //Initialize the ActorSystem
  val actorSystem=ActorSystem("UniversityMessageSystem")
 
  //construct the Teacher Actor Ref
  val teacherActorRef=actorSystem.actorOf(Props[TeacherActor])
 
  //send a message to the Teacher Actor
  teacherActorRef!QuoteRequest
 
  //Let's wait for a couple of seconds before we shut down the system
  Thread.sleep (2000) 
 
  //Shut down the ActorSystem.
  actorSystem.shutdown()
 
} 
```

你需要调用shutdown方法来shutdownActorSystem ，否则，JVM会继续运行！我在程序中调用了 Thread.sleep (2000) 来休眠，这看起来很傻，但是你不用担心，在后面我将会创建许多的测试用例来避免这些。



#### 消息

我们在前面仅仅讨论了ActorRef的QuoteRequest，并没有看到message的类！这里将介绍，代码如下：

```scala
package me.rerun.akkanotes.messaging.protocols
 
object TeacherProtocol{
 
  case class QuoteRequest()
  case class QuoteResponse(quoteString:String)
 
}
```

正如你说知，QuoteRequest是用来给TeacherActor发送消息的；而Actor将会用QuoteResponse来响应。



#### DISPATCHER AND A MAILBOX

ActorRef取出消息并放到Dispatcher中。在这种模式下，当我们创建了ActorSystem 和ActorRef,Dispatcher和MailBox也将会创建。让我们来看看这到底是什么：

![img](https://www.iteblog.com/pic/akka/MessageDispatcherMailbox.png)

##### 1、MailBox

　　 每个Actor都有一个MailBox（后面我们将看到一个特殊情况）。在我们之前的模型中，每个Teacher也有一个MailBox。Teacher需要检查MailBox并处理其中的message。MailBox中有个队列并以FIFO方式储存和处理消息。

##### 2、Dispatcher

　　 Dispatcher做一些很有趣的事。从图中可以看到，Dispatcher好像只是仅仅将message从ActorRef 传递到MailBox中。但是在这背后有件很奇怪的事：Dispatcher 包装了一个 ExecutorService (ForkJoinPool 或者 ThreadPoolExecutor).它通过ExecutorService运行 MailBox。代码片段如下：

```scala
protected[akka] override def registerForExecution(mbox: Mailbox, ...): Boolean = {  
    ...
    try {
        executorService execute mbox
    ...
}
```

 什么？你说是你来运行Mailbox？是的，我们前面已经看到Mailbox的队列中持有所有的消息。用executorService 运行Mailbox也一样。Mailbox必须是一个线程。代码中有大量的Mailbox的声明和构造函数，代码片段如下：

```scala
private[akka] abstract class Mailbox(val messageQueue: MessageQueue) 
                          extends SystemMessageQueue with Runnable
```



#### Teacher Actor

![img](https://www.iteblog.com/pic/akka/TeacherActor.png)

当MailBox的run方法被运行，它将从队列中取出消息，并传递到Actor进行处理。该方法最终在你将消息tell到ActorRef 中的时候被调用，在目标Actor其实是个receive 方法。TeacherActor 是基本的类，并且拥有一系列的quote，很明显，receive 方法是用来处理消息的。代码片段如下：

```scala
package me.rerun.akkanotes.messaging.actormsg1
 
import scala.util.Random
 
import akka.actor.Actor  
import me.rerun.akkanotes.messaging.protocols.TeacherProtocol._
 
/*
 * Your Teacher Actor class. 
 * 
 * The class could use refinement by way of  
 * using ActorLogging which uses the EventBus of the Actor framework
 * instead of the plain old System out
 * 
 */
 
class TeacherActor extends Actor {
 
  val quotes = List(
    "Moderation is for cowards",
    "Anything worth doing is worth overdoing",
    "The trouble is you think you have time",
    "You never gonna know if you never even try")
 
  def receive = {
 
    case QuoteRequest => {
 
      import util.Random
 
      //Get a random Quote from the list and construct a response
      val quoteResponse=QuoteResponse(quotes(Random.nextInt(quotes.size)))
 
      println (quoteResponse)
 
    }
 
  }
 
}
```

TeacherActor的receive只匹配一种消息：QuoteRequest ，receive方法主要做以下几件事：
　　1、匹配QuoteRequest；
　　2、从quotes中随机取出一个quote；
　　3、构造一个QuoteResponse；
　　4、在控制台打印QuoteResponse