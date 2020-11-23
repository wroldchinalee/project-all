#### 传统IO

服务端	ServerSocket

客户端	Socket

阻塞点有两个：

1. serversocket.accept()
2. inputStream.read()

单线程的情况下只能有一个客户端

用线程池可以有多个客户端连接，但是非常耗性能

------

#### NIO

ServerSocketChannel			ServerSocket

SocketChannel						Socket

Selector

SelectionKey

NIO的一些常见问题：

1.客户端关闭的时候会抛出异常，死循环

解决方法：

```java
int length = channel.read(byteBuffer);
if (length > 0) {
    String message = new String(byteBuffer.array(), "UTF-8");
    System.out.printf("服务端接收消息:%s\n", message);
    ByteBuffer outBuffer = ByteBuffer.wrap("success".getBytes("UTF-8"));
    channel.write(outBuffer);
} else {
    // 如果不判断长度小于0，客户端关闭后，会一直读就绪，然后抛异常
    System.out.println("客户端关闭");
    selectionKey.cancel();
}
```



2.selector.select()方法是阻塞的，为什么说NIO是非阻塞的？

阻塞和非阻塞是指当读取数据的时候，是否可以立即返回，即使没有数据的时候

而且select()方法也有不阻塞的，比如：selectNow()立即返回，select(100)阻塞指定时间

selector.wake()方法可以唤醒阻塞的线程



3.为什么不注册OP_WRITE事件也可以写数据？

OP_WRITE表示底层缓冲区是否有空间，如果有空间则TRUE

当channel没有注册OP_WRITE事件时，selectionKey.isWritable()返回false

但是还是可以写数据

