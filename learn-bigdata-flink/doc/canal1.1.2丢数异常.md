

https://github.com/alibaba/canal/pull/1857

https://github.com/alibaba/canal/pull/1858

1.1.5版本修改后的代码：

```
static class SimpleFatalExceptionHandler implements ExceptionHandler {

        @Override
        public void handleEventException(final Throwable ex, final long sequence, final Object event) {
            //异常上抛，否则processEvents的逻辑会默认会mark为成功执行，有丢数据风险
            throw new CanalParseException(ex);
        }

        @Override
        public void handleOnStartException(final Throwable ex) {
        }

        @Override
        public void handleOnShutdownException(final Throwable ex) {
        }
    }

```

NPE问题，1.1.3修改后的代码: