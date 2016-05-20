package demo.rabbitmq.producter;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import demo.rabbitmq.Config;

/**
 * 消息生产者示例
 * 
 * @author hbm
 *
 */
public class Productor {

	final static int DELIVERY_MODE_DURABLE = 2; // 消息持久化

	public static void main(String[] args) {
		try {
			/* 初始化工作 */
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(Config.RABBITMQ_SERVER_IP);
			factory.setUsername(Config.RABBITMQ_USERNAME);
			factory.setPassword(Config.RABBITMQ_PASSWORD);
			factory.setVirtualHost(Config.RABBITMQ_VIRTUAL_HOST);
			factory.setAutomaticRecoveryEnabled(true);
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			/* 声明exchange */
			String exchange = "exchange.test1";// 交换器名
			String type = "topic";// direct,fanout,topic
			boolean durable = true;// 持久化
			channel.exchangeDeclare(exchange, type, durable);// 声明交换器，如果交换器不存在则创建之

			/* 发布消息 */
			String routingKey = "hbm"; // 如果是fanout类型会忽略routingKey
			BasicProperties props = new BasicProperties.Builder().deliveryMode(DELIVERY_MODE_DURABLE).build(); // 设置消息持久化
			String msg = new Date().toString() + " 测试";// 消息内容
			channel.basicPublish(exchange, routingKey, props, msg.getBytes("utf-8"));// 发布消息

			/* 收尾 */
			channel.close();
			connection.close(); // connection不关闭，则不退出进程
			System.out.println("finish");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}

	}

}
