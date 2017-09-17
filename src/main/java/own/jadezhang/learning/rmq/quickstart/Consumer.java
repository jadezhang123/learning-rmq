/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package own.jadezhang.learning.rmq.quickstart;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * This example shows how to subscribe and consume messages using providing
 * {@link DefaultMQPushConsumer}.
 */
public class Consumer {

	public static void main(String[] args) throws InterruptedException,
			MQClientException {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
				"CG_QUICK_START");
		consumer.setNamesrvAddr("192.168.230.128:9876");
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		consumer.setConsumeMessageBatchMaxSize(3);
		consumer.subscribe("TopicTest_B", "TagA");
		
		consumer.registerMessageListener(new MessageListenerConcurrently() {
			public ConsumeConcurrentlyStatus consumeMessage(
					List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				/**
				 * 消息重试 1：由于消息本身原因造成：应定时重试，即返回ConsumeConcurrentlyStatus.
				 * RECONSUME_LATER 2：由于下游服务原因造成：应睡眠一定时间（30S）再消费下一条消息，减少broker压力
				 */
				try {
					System.out.println("消息个数：" + msgs.size());

					for (MessageExt msgExt : msgs) {
						System.out
								.printf(" Receive Messages: " + msgExt + "%n");
					}
					System.out.println("######################消费结束");
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

				} catch (Exception e) {

					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
			}

		});

		consumer.start();

		System.out.printf("Consumer Started.%n");

		TimeUnit.DAYS.sleep(1);

	}
}
