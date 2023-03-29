using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace AddUp.RabbitMQ.Fakes
{
    internal sealed class FakeConnection : IAutorecoveringConnection
    {
        private readonly RabbitServer server;

        public FakeConnection(RabbitServer rabbitServer) : this(rabbitServer, null) { }
        public FakeConnection(RabbitServer rabbitServer, string name)
        {
            server = rabbitServer ?? throw new ArgumentNullException(nameof(rabbitServer));
            ClientProvidedName = name ?? string.Empty;
            Models = new List<IModel>();
        }

#pragma warning disable 67
        public event EventHandler<CallbackExceptionEventArgs> CallbackException;
        public event EventHandler<ConnectionBlockedEventArgs> ConnectionBlocked;
        public event EventHandler<ShutdownEventArgs> ConnectionShutdown;
        public event EventHandler<EventArgs> ConnectionUnblocked;
        // The 4 events below come from IAutorecoveringConnection; everything else is from IConnection
        public event EventHandler<EventArgs> RecoverySucceeded;
        public event EventHandler<ConnectionRecoveryErrorEventArgs> ConnectionRecoveryError;
        public event EventHandler<ConsumerTagChangedAfterRecoveryEventArgs> ConsumerTagChangeAfterRecovery;
        public event EventHandler<QueueNameChangedAfterRecoveryEventArgs> QueueNameChangeAfterRecovery;
        public event EventHandler<RecoveringConsumerEventArgs> RecoveringConsumer;
#pragma warning restore 67

        public string ClientProvidedName { get; }
        public int LocalPort { get; }
        public int RemotePort { get; }
        public AmqpTcpEndpoint Endpoint { get; }
        public IProtocol Protocol { get; }
        public ushort ChannelMax { get; }
        public uint FrameMax { get; }
        public TimeSpan Heartbeat { get; }
        public AmqpTcpEndpoint[] KnownHosts { get; }
        public ShutdownEventArgs CloseReason { get; private set; }
        public bool IsOpen => CloseReason == null;
        public IDictionary<string, object> ServerProperties { get; } = new Dictionary<string, object>();
        public IList<ShutdownReportEntry> ShutdownReport { get; } = new List<ShutdownReportEntry>();
        public IDictionary<string, object> ClientProperties { get; } = new Dictionary<string, object>();

        private List<IModel> Models { get; }

        internal List<IModel> GetModelsForUnitTests() => Models;

        public void Dispose()
        {
            if (IsOpen) Abort(); // Abort rather than Close because we do not want Dispose to throw
        }

        public IModel CreateModel()
        {
            var model = new FakeModel(server);
            model.ModelShutdown += OnModelShutdown;
            Models.Add(model);

            return model;
        }
        
        public void HandleConnectionBlocked(string reason)
        {
            // Fake implementation. Nothing to do here.
        }

        public void HandleConnectionUnblocked()
        {
            // Fake implementation. Nothing to do here.
        }

        public void UpdateSecret(string newSecret, string reason)
        {
            // Fake implementation. Nothing to do here.
        }

        // Close and Abort (implementation inspired by RabbitMQ.Client)

        public void Abort() => Abort(Timeout.InfiniteTimeSpan);
        public void Abort(TimeSpan timeout) => Abort(200, "Connection close forced", timeout);
        public void Abort(ushort reasonCode, string reasonText) => Abort(reasonCode, reasonText, Timeout.InfiniteTimeSpan);
        public void Abort(ushort reasonCode, string reasonText, TimeSpan timeout) =>
            Close(new ShutdownEventArgs(ShutdownInitiator.Application, reasonCode, reasonText), abort: true);

        public void Close() => Close(200, "Goodbye", Timeout.InfiniteTimeSpan);
        public void Close(TimeSpan timeout) => Close(200, "Goodbye", timeout);
        public void Close(ushort reasonCode, string reasonText) => Close(reasonCode, reasonText, Timeout.InfiniteTimeSpan);
        public void Close(ushort reasonCode, string reasonText, TimeSpan timeout) =>
            Close(new ShutdownEventArgs(ShutdownInitiator.Application, reasonCode, reasonText), abort: false);

        private void Close(ShutdownEventArgs reason, bool abort)
        {
            try
            {
                if (!IsOpen)
                    throw new AlreadyClosedException(reason);

                CloseReason = reason;
                var modelsCopy = Models.ToList();
                modelsCopy.ForEach(m =>
                {
                    if (abort)
                        m.Abort();
                    else
                        m.Close();
                });

                ConnectionShutdown?.Invoke(this, reason);
            }
            catch
            {
                if (!abort) throw;
            }
        }

        private void OnModelShutdown(object sender, ShutdownEventArgs e)
        {
            var model = (IModel)sender;
            model.ModelShutdown -= OnModelShutdown;
            _ = Models.Remove(model);
        }
    }
}
