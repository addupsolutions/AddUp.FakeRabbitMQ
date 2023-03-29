using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace AddUp.RabbitMQ.Fakes
{
    internal sealed class FakeModel : IModel
    {
        private readonly ConcurrentDictionary<ulong, RabbitMessage> workingMessages = new ConcurrentDictionary<ulong, RabbitMessage>();
        private readonly ConcurrentDictionary<string, ConsumerData> consumers = new ConcurrentDictionary<string, ConsumerData>();

        private readonly ConsumerDeliveryQueue deliveryQueue;
        private readonly RabbitServer server;
        private long lastDeliveryTag;

        public FakeModel(RabbitServer rabbitServer)
        {
            server = rabbitServer;
            deliveryQueue = ConsumerDeliveryQueue.Create(
                this,
                deliveryExceptionHandler: args => CallbackException(this, args),
                rabbitServer.BlockingDeliveryMode);
        }

#pragma warning disable 67
        public event EventHandler<BasicAckEventArgs> BasicAcks;
        public event EventHandler<BasicNackEventArgs> BasicNacks;
        public event EventHandler<EventArgs> BasicRecoverOk;
        public event EventHandler<BasicReturnEventArgs> BasicReturn;
        public event EventHandler<CallbackExceptionEventArgs> CallbackException;
        public event EventHandler<FlowControlEventArgs> FlowControl;
        public event EventHandler<ShutdownEventArgs> ModelShutdown;
#pragma warning restore 67

        public int ChannelNumber { get; }
        public IBasicConsumer DefaultConsumer { get; set; }
        public ulong NextPublishSeqNo { get; set; }
        public string CurrentQueue { get; private set; }
        public TimeSpan ContinuationTimeout { get; set; }
        public ShutdownEventArgs CloseReason { get; private set; }
        public bool IsOpen => CloseReason == null;
        public bool IsClosed => !IsOpen;
        internal ConcurrentDictionary<ulong, RabbitMessage> WorkingMessagesForUnitTests => workingMessages;

        public void Abort() => Abort(200, "Goodbye");
        public void Abort(ushort replyCode, string replyText) => Close(replyCode, replyText, abort: true);

        public void BasicAck(ulong deliveryTag, bool multiple)
        {
            _ = workingMessages.TryRemove(deliveryTag, out var message);
            if (message != null)
            {
                _ = server.Queues.TryGetValue(message.Queue, out var queue);
                if (queue != null)
                    _ = queue.Messages.TryDequeue(out _);
            }
        }

        public void BasicCancel(string consumerTag)
        {
            _ = consumers.TryRemove(consumerTag, out var consumerData);
            if (consumerData == null) return;

            consumerData.Queue.MessagePublished -= consumerData.QueueMessagePublished;

            // In async mode, IBasicConsumer may 'hide' an IAsyncBasicConsumer...
            // See https://github.com/StephenCleary/AsyncEx/blob/e637035c775f99b50c458d4a90e330563ecfd07b/src/Nito.AsyncEx.Tasks/Synchronous/TaskExtensions.cs#L50
            // For why .GetAwaiter().GetResult()
            if (consumerData.Consumer is IAsyncBasicConsumer asyncBasicConsumer)
                asyncBasicConsumer
                    .HandleBasicCancelOk(consumerTag)
                    .GetAwaiter()
                    .GetResult();
            else consumerData.Consumer
                    .HandleBasicCancelOk(consumerTag);
        }

        public void BasicCancelNoWait(string consumerTag) => BasicCancel(consumerTag);

        public string BasicConsume(string queue, bool autoAck, string consumerTag, bool noLocal, bool exclusive, IDictionary<string, object> arguments, IBasicConsumer consumer)
        {
            void notifyConsumerOfMessage(RabbitMessage message)
            {
                _ = Interlocked.Increment(ref lastDeliveryTag);

                var deliveryTag = Convert.ToUInt64(lastDeliveryTag);
                const bool redelivered = false;
                var exchange = message.Exchange;
                var routingKey = message.RoutingKey;
                var basicProperties = message.BasicProperties ?? CreateBasicProperties();
                var body = message.Body;

                RabbitMessage updateFunction(ulong key, RabbitMessage existingMessage) => existingMessage;
                _ = workingMessages.AddOrUpdate(deliveryTag, message, updateFunction);

                // In async mode, IBasicConsumer may 'hide' an IAsyncBasicConsumer...
                // See https://github.com/StephenCleary/AsyncEx/blob/e637035c775f99b50c458d4a90e330563ecfd07b/src/Nito.AsyncEx.Tasks/Synchronous/TaskExtensions.cs#L50
                // For why .GetAwaiter().GetResult()
                if (consumer is IAsyncBasicConsumer asyncBasicConsumer)
                    asyncBasicConsumer
                        .HandleBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey, basicProperties, body)
                        .GetAwaiter()
                        .GetResult();
                else consumer
                        .HandleBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey, basicProperties, body);
            }

            // Deliberately check for empty string here, latest RabbitMQ client accepts ""
            // but will throw on null and kill the channel.
            if (consumerTag == "")
            {
                var guidString = Guid.NewGuid();
                // https://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume.consumer-tag
                // If this field is empty the server will generate a unique tag.
                consumerTag = $"amq.{guidString:N}";
            }

            _ = server.Queues.TryGetValue(queue, out var queueInstance);
            if (queueInstance != null)
            {
                void publishedAction(object sender, RabbitMessage message) =>
                    deliveryQueue.Deliver(() => notifyConsumerOfMessage(message));

                var consumerData = new ConsumerData(consumer, queueInstance, publishedAction);

                // https://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume.consumer-tag
                // The client MUST NOT specify a tag that refers to an existing consumer. Error code: not-allowed
                ConsumerData updateFunction(string s, ConsumerData _) =>
                    throw new OperationInterruptedException(
                        new ShutdownEventArgs(ShutdownInitiator.Peer, 530, $"NOT_ALLOWED - attempt to reuse consumer tag '{s}'"));
                _ = consumers.AddOrUpdate(consumerTag, consumerData, updateFunction);

                foreach (var message in queueInstance.Messages)
                    consumerData.QueueMessagePublished(this, message);

                queueInstance.MessagePublished += consumerData.QueueMessagePublished;

                if (consumer is IAsyncBasicConsumer asyncBasicConsumer)
                    asyncBasicConsumer.HandleBasicConsumeOk(consumerTag).GetAwaiter().GetResult();
                else
                    consumer.HandleBasicConsumeOk(consumerTag);
            }

            return consumerTag;
        }

        public BasicGetResult BasicGet(string queue, bool autoAck)
        {
            _ = server.Queues.TryGetValue(queue, out var queueInstance);
            if (queueInstance == null) return null;

            _ = autoAck ?
                queueInstance.Messages.TryDequeue(out var message) :
                queueInstance.Messages.TryPeek(out message);

            if (message == null) return null;

            _ = Interlocked.Increment(ref lastDeliveryTag);
            var deliveryTag = Convert.ToUInt64(lastDeliveryTag);
            const bool redelivered = false;
            var exchange = message.Exchange;
            var routingKey = message.RoutingKey;
            var messageCount = Convert.ToUInt32(queueInstance.Messages.Count);
            var basicProperties = message.BasicProperties ?? CreateBasicProperties();
            var body = message.Body;

            if (autoAck)
                _ = workingMessages.TryRemove(deliveryTag, out _);
            else
            {
                RabbitMessage updateFunction(ulong key, RabbitMessage existingMessage) => existingMessage;
                _ = workingMessages.AddOrUpdate(deliveryTag, message, updateFunction);
            }

            return new BasicGetResult(deliveryTag, redelivered, exchange, routingKey, messageCount, basicProperties, body);
        }

        public void BasicNack(ulong deliveryTag, bool multiple, bool requeue)
        {
            if (requeue) return;

            foreach (var queue in workingMessages.Select(m => m.Value.Queue))
            {
                _ = server.Queues.TryGetValue(queue, out var queueInstance);
                if (queueInstance != null)
                    queueInstance.ClearMessages();
            }

            _ = workingMessages.TryRemove(deliveryTag, out var message);
            if (message == null) return;

            foreach (var workingMessage in workingMessages.Select(m => m.Value))
            {
                _ = server.Queues.TryGetValue(workingMessage.Queue, out var queueInstance);
                queueInstance?.PublishMessage(workingMessage);
            }
        }

        public void BasicPublish(string exchange, string routingKey, bool mandatory, IBasicProperties basicProperties, ReadOnlyMemory<byte> body)
        {
            var parameters = new RabbitMessage
            {
                Exchange = exchange,
                RoutingKey = routingKey,
                Mandatory = mandatory,
                BasicProperties = basicProperties,
                Body = body.ToArray()
            };

            RabbitExchange addExchange(string s)
            {
                var newExchange = new RabbitExchange(ExchangeType.Direct, server)
                {
                    Name = exchange,
                    Arguments = null,
                    AutoDelete = false,
                    IsDurable = false
                };

                newExchange.PublishMessage(parameters);
                return newExchange;
            }

            RabbitExchange updateExchange(string s, RabbitExchange existingExchange)
            {
                existingExchange.PublishMessage(parameters);
                return existingExchange;
            }

            _ = server.Exchanges.AddOrUpdate(exchange, addExchange, updateExchange);

            if (NextPublishSeqNo != 0ul)
                NextPublishSeqNo++;
        }

        public void BasicQos(uint prefetchSize, ushort prefetchCount, bool global)
        {
            // Fake implementation. Nothing to do here.
        }

        public void BasicRecover(bool requeue)
        {
            if (requeue)
            {
                foreach (var message in workingMessages.Select(m => m.Value))
                {
                    _ = server.Queues.TryGetValue(message.Queue, out var queueInstance);
                    if (queueInstance != null)
                        queueInstance.PublishMessage(message);
                }
            }

            workingMessages.Clear();
        }

        public void BasicRecoverAsync(bool requeue) => BasicRecover(requeue);

        public void BasicReject(ulong deliveryTag, bool requeue) =>
            BasicNack(deliveryTag, false, requeue);

        public void Close() => Close(200, "Goodbye");
        public void Close(ushort replyCode, string replyText) => Close(replyCode, replyText, abort: false);
        private void Close(ushort replyCode, string replyText, bool abort)
        {
            if (CloseReason == null)
            {
                var reason = new ShutdownEventArgs(ShutdownInitiator.Application, replyCode, replyText);
                try
                {
                    CloseReason = reason;

                    var consumerTags = consumers.Keys.ToList();
                    foreach (var consumerTag in consumerTags)
                        BasicCancel(consumerTag);

                    deliveryQueue.Complete();
                    ModelShutdown?.Invoke(this, reason);
                }
                catch
                {
                    if (!abort) throw;
                }
            }

            deliveryQueue.WaitForCompletion();
        }

        public void ConfirmSelect()
        {
            if (NextPublishSeqNo == 0ul)
                NextPublishSeqNo = 1ul;
        }

        public uint ConsumerCount(string queue) => QueueDeclarePassive(queue).ConsumerCount;

        public IBasicProperties CreateBasicProperties() => new FakeBasicProperties();

        public IBasicPublishBatch CreateBasicPublishBatch() => new FakeBasicPublishBatch(this);

        public void Dispose()
        {
            if (IsOpen) Abort();
        }

        public void ExchangeBind(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            _ = server.Exchanges.TryGetValue(source, out var exchange);
            _ = server.Queues.TryGetValue(destination, out var queue);

            var binding = new RabbitExchangeQueueBinding { Exchange = exchange, Queue = queue, RoutingKey = routingKey };
            if (exchange != null)
                _ = exchange.Bindings.AddOrUpdate(binding.Key, binding, (k, v) => binding);
            if (queue != null)
                _ = queue.Bindings.AddOrUpdate(binding.Key, binding, (k, v) => binding);
        }

        public void ExchangeBindNoWait(string destination, string source, string routingKey, IDictionary<string, object> arguments) =>
            ExchangeBind(destination, source, routingKey, arguments);

        public void ExchangeDeclare(string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object> arguments)
        {
            var exchangeInstance = new RabbitExchange(type, server)
            {
                Name = exchange,
                IsDurable = durable,
                AutoDelete = autoDelete,
                Arguments = arguments
            };

            RabbitExchange updateFunction(string name, RabbitExchange existing) => existing;
            _ = server.Exchanges.AddOrUpdate(exchange, exchangeInstance, updateFunction);
        }

        public void ExchangeDeclareNoWait(string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object> arguments) =>
            ExchangeDeclare(exchange, type, durable, false, arguments);

        public void ExchangeDeclarePassive(string exchange)
        {
            if (server.Exchanges.ContainsKey(exchange)) return;

            var shutdownArgs = new ShutdownEventArgs(initiator: ShutdownInitiator.Peer,
                replyText: $"NOT_FOUND - no exchange '{exchange}' in vhost '/'",
                replyCode: 404,
                classId: 40,
                methodId: 10);

            throw new OperationInterruptedException(shutdownArgs);
        }

        public void ExchangeDelete(string exchange, bool ifUnused) =>
            server.Exchanges.TryRemove(exchange, out _);

        public void ExchangeDeleteNoWait(string exchange, bool ifUnused) =>
            ExchangeDelete(exchange, false);

        public void ExchangeUnbind(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            _ = server.Exchanges.TryGetValue(source, out var exchange);
            _ = server.Queues.TryGetValue(destination, out var queue);

            var binding = new RabbitExchangeQueueBinding { Exchange = exchange, Queue = queue, RoutingKey = routingKey };
            if (exchange != null)
                _ = exchange.Bindings.TryRemove(binding.Key, out _);
            if (queue != null)
                _ = queue.Bindings.TryRemove(binding.Key, out _);
        }

        public void ExchangeUnbindNoWait(string destination, string source, string routingKey, IDictionary<string, object> arguments) =>
            ExchangeUnbind(destination, source, routingKey, arguments);

        public uint MessageCount(string queue) => QueueDeclarePassive(queue).MessageCount;

        public void QueueBind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments) =>
            ExchangeBind(queue, exchange, routingKey, arguments);

        public void QueueBindNoWait(string queue, string exchange, string routingKey, IDictionary<string, object> arguments) =>
            QueueBind(queue, exchange, routingKey, arguments);

        public QueueDeclareOk QueueDeclare(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object> arguments)
        {
            // This handles 'default' queues creations with constructs such as:
            // var queueName = Channel.QueueDeclare(); // temporary anonymous queue
            var q = string.IsNullOrEmpty(queue) ? Guid.NewGuid().ToString() : queue;

            var queueInstance = new RabbitQueue
            {
                Name = q,
                IsDurable = durable,
                IsExclusive = exclusive,
                IsAutoDelete = autoDelete,
                Arguments = arguments
            };

            RabbitQueue updateFunction(string name, RabbitQueue existing) => existing;
            _ = server.Queues.AddOrUpdate(q, queueInstance, updateFunction);

            // RabbitMQ automatically binds queues to the default exchange.
            // https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-default
            QueueBind(q, "", q, null);

            CurrentQueue = queue;

            return new QueueDeclareOk(q, 0, 0);
        }

        public void QueueDeclareNoWait(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object> arguments) =>
            QueueDeclare(queue, durable, exclusive, autoDelete, arguments);

        public QueueDeclareOk QueueDeclarePassive(string queue)
        {
            if (server.Queues.TryGetValue(queue, out var rabbitQueue))
                return new QueueDeclareOk(queue, (uint)unchecked(rabbitQueue.Messages.Count), (uint)unchecked(rabbitQueue.ConsumerCount));

            var shutdownArgs = new ShutdownEventArgs(initiator: ShutdownInitiator.Peer,
                    replyText: $"NOT_FOUND - no queue '{queue}' in vhost '/'",
                    replyCode: 404,
                    classId: 50,
                    methodId: 10);

            throw new OperationInterruptedException(shutdownArgs);
        }

        public uint QueueDelete(string queue, bool ifUnused, bool ifEmpty)
        {
            _ = server.Queues.TryRemove(queue, out var instance);
            return instance != null ? 1u : 0u;
        }

        public void QueueDeleteNoWait(string queue, bool ifUnused, bool ifEmpty) => QueueDelete(queue, false, false);

        public uint QueuePurge(string queue)
        {
            _ = server.Queues.TryGetValue(queue, out var instance);
            if (instance == null)
                return 0u;

            var count = 0u;
            while (!instance.Messages.IsEmpty)
            {
                _ = instance.Messages.TryDequeue(out _);
                count++;
            }

            return count;
        }

        public void QueueUnbind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments) =>
            ExchangeUnbind(queue, exchange, routingKey, arguments);

        public void TxCommit()
        {
            // Fake implementation. Nothing to do here.
        }

        public void TxRollback()
        {
            // Fake implementation. Nothing to do here.
        }

        public void TxSelect()
        {
            // Fake implementation. Nothing to do here.
        }

        public bool WaitForConfirms() => WaitForConfirms(Timeout.InfiniteTimeSpan);
        public bool WaitForConfirms(TimeSpan timeout) => WaitForConfirms(timeout, out _);
        public bool WaitForConfirms(TimeSpan timeout, out bool timedOut)
        {
            if (NextPublishSeqNo == 0ul)
                throw new InvalidOperationException("Confirms not selected");

            timedOut = false;
            return true;
        }

        public void WaitForConfirmsOrDie() => WaitForConfirmsOrDie(Timeout.InfiniteTimeSpan);
        public void WaitForConfirmsOrDie(TimeSpan timeout) => _ = WaitForConfirms(timeout);
    }
}