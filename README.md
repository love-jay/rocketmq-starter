# rocketmq-starter
rocketmq-starter
 
 rocket mq 结合springboot封装的starter
 
 监听类继承AbstractRocketMqConsumer类即可实现监听，注意生成和消费组配置。
  yml中配置生产组
  rocketmq:
    namesrv-addr: localhost:xxxx
    producer-group-name: xxxxProducer
    retry-times-when-send-async-failed: 4
    retry-times-when-send-failed: 4
    compress-msg-body-over-how-much: 5120
   监听类配置topic和tag以及消费组合其他配置，通过RocketListenerHandler注解
