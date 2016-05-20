package demo.rabbitmq.comsumer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;

import demo.rabbitmq.Config;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * 消息消费者
 * 
 * @author hbm
 *
 */
public class MQConsumer implements Runnable, Consumer {

	static Logger logger = LoggerFactory.getLogger(MQConsumer.class);

	protected Connection connection;
	protected Channel channel;
	protected String routingKey;
	protected ConsumerExecutor executor;// 执行器

	public MQConsumer(String routingKey, ConsumerExecutor executor) {
		this.routingKey = routingKey;
		this.executor = executor;
	}

	@Override
	public void run() {
		try {
			init();
			channel.basicConsume(routingKey, true, this);
		} catch (Exception e) {
			logger.error("mq init() error", e);
		}
	}

	protected void init() throws IOException, TimeoutException {
		/* 初始化工作 */
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(Config.RABBITMQ_SERVER_IP);
		factory.setUsername(Config.RABBITMQ_USERNAME);
		factory.setPassword(Config.RABBITMQ_PASSWORD);
		factory.setVirtualHost(Config.RABBITMQ_VIRTUAL_HOST);
		factory.setAutomaticRecoveryEnabled(true);
		connection = factory.newConnection();
		channel = connection.createChannel();

		/* 声明队列 */
		String queue = "queue.test1";// 队列名
		boolean durable = true;// 持久化
		boolean exclusive = false;// 排他性，只对首次声明它的连接（Connection）可见，会在其连接断开的时候自动删除。
		boolean autoDelete = false;// 自动删除
		Map<String, Object> arguments = null; // 其他配置参数
		channel.queueDeclare(queue, durable, exclusive, autoDelete, arguments);// 声明队列，如果队列不存在则创建之

		/* 把队列绑定到交换器上 */
		String exchange = "exchange.test1";// 交换器名
		String routingKey = "hbm"; // 如果是fanout类型会忽略routingKey
		channel.queueBind(queue, exchange, routingKey);// 绑定

	}

	@Override
	public void handleDelivery(String consumerTag, Envelope env, BasicProperties props, byte[] body) throws IOException {
		String msg = new String(body, "utf-8");
		logger.info("从队列[" + routingKey + "] 接收消息: " + msg);
		try {
			executor.consume(msg);
		} catch (Exception e) {
			logger.error("handleDelivery error:", e);
		}
	}

	@Override
	public void handleCancel(String consumerTag) {
		logger.info("handleCancel:" + consumerTag);
	}

	@Override
	public void handleCancelOk(String consumerTag) {
		logger.info("handleCancelOk:" + consumerTag);
	}

	@Override
	public void handleConsumeOk(String consumerTag) {
		logger.info("handleConsumeOk:" + consumerTag);
	}

	@Override
	public void handleRecoverOk(String consumerTag) {
		logger.info("handleRecoverOk:" + consumerTag);
	}

	@Override
	public void handleShutdownSignal(String consumerTag, ShutdownSignalException e) {
		logger.info("handleShutdownSignal:{},{}" + consumerTag,e);
		logger.info("服务器关闭");
	}

	public static void main(String[] args) {

		ConsumerExecutor exec = new ConsumerExecutor() {

			@Override
			public void consume(String message) {
				System.out.println(message);
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
		MQConsumer consumer = new MQConsumer("queue.test1", exec);
		new Thread(consumer).start();
	}
}
