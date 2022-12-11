package mao.rocketmq_spring_boot.controller;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Project name(项目名称)：rocketMQ_spring_boot
 * Package(包名): mao.rocketmq_spring_boot.controller
 * Class(类名): RocketController
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/12/11
 * Time(创建时间)： 18:29
 * Version(版本): 1.0
 * Description(描述)： 无
 */

@RestController
public class RocketController
{
    /**
     * 日志
     */
    private static final Logger log = LoggerFactory.getLogger(RocketController.class);

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    /**
     * 发送消息
     *
     * @param data 数据
     * @return {@link String}
     */
    @GetMapping("/save/{data}")
    public String save(@PathVariable String data)
    {
        log.info("开始发送消息");
        rocketMQTemplate.convertAndSend("test_topic", data);
        return data;
    }

    /**
     * 发送同步消息
     *
     * @param data 数据
     * @return {@link SendResult}
     */
    @GetMapping("/save2/{data}")
    public SendResult save2(@PathVariable String data)
    {
        log.info("开始发送同步消息");
        Message message = new Message();
        message.setBody(("同步消息:" + data).getBytes(StandardCharsets.UTF_8));
        return rocketMQTemplate.syncSend("test_topic", message);
    }

    /**
     * 发送批量消息
     *
     * @param data 数据
     * @return {@link SendResult}
     */
    @GetMapping("/save3/{data}")
    public SendResult save3(@PathVariable String data)
    {
        log.info("开始发送批量消息");
        List<Message> messageList = new ArrayList<>(10);
        for (int i = 0; i < 10; i++)
        {
            Message message = new Message();
            message.setBody(("批量消息:" + data + " -" + i).getBytes(StandardCharsets.UTF_8));
            messageList.add(message);
        }
        return rocketMQTemplate.syncSend("test_topic", messageList);
    }


    /**
     * 发送带tag的消息
     *
     * @param data 数据
     * @return {@link String}
     */
    @GetMapping("/save4/{data}")
    public String save4(@PathVariable String data)
    {
        log.info("开始发送带tag的消息");
        //发送带tag的消息，直接在topic后面加上":tag"
        rocketMQTemplate.convertAndSend("test_topic:tag1", data);
        return data;
    }

    /**
     * 发送消息
     *
     * @param data 数据
     * @return {@link String}
     */
    @GetMapping("/save5/{data}")
    public String save5(@PathVariable String data)
    {
        log.info("开始发送消息");
        Message message = new Message();
        message.setBody(("同步消息:" + data).getBytes(StandardCharsets.UTF_8));
        SendResult sendResult = rocketMQTemplate.syncSendOrderly("test_topic", message, "1");
        log.info("发送结果：" + sendResult);
        return data;
    }

    /**
     * 发送异步消息
     *
     * @param data 数据
     * @return {@link String}
     */
    @GetMapping("/save6/{data}")
    public String save6(@PathVariable String data)
    {
        log.info("开始发送异步消息");
        rocketMQTemplate.asyncSend("test_group", data, new SendCallback()
        {
            @Override
            public void onSuccess(SendResult sendResult)
            {
                log.info("异步消息发送成功：" + sendResult);
            }

            @Override
            public void onException(Throwable throwable)
            {
                log.error("异步消息发送失败：" + throwable);
            }
        });
        return data;
    }


    /**
     * 发送单向消息
     *
     * @param data 数据
     * @return {@link String}
     */
    @GetMapping("/save7/{data}")
    public String save7(@PathVariable String data)
    {
        log.info("开始发送单向消息");
        rocketMQTemplate.sendOneWay("test_topic", data);
        return data;
    }
}
