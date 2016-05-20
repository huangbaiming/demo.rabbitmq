package demo.rabbitmq.comsumer;

/**
 * 消费者执行器
 * 
 * @author hbm
 *
 */
public interface ConsumerExecutor {
	void consume(String message);
}
