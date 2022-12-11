package mao.rocketmq_spring_boot.consumer;


import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Project name(项目名称)：rocketMQ_spring_boot
 * Package(包名): mao.rocketmq_spring_boot.consumer
 * Class(类名): RocketCustomer
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/11
 * Time(创建时间)： 19:25
 * Version(版本): 1.0
 * Description(描述)： 消费者
 */

@Component
@RocketMQMessageListener(consumerGroup = "mao_group", topic = "test_topic")
public class RocketCustomer implements RocketMQListener<Object>
{

    /**
     * 日志
     */
    private static final Logger log = LoggerFactory.getLogger(RocketCustomer.class);

    @Override
    public void onMessage(Object o)
    {
        //直接调用toString方法打印
        log.info("消费者监听到一条消息：" + o.toString());
    }
}
