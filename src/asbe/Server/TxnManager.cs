using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

// Tracks open AMQP transactions and the operations staged inside them.
//
// Wire mechanics: clients attach a sender to a target of `Coordinator`, send a
// `Declare` to allocate a txn-id, then ride that id on subsequent transfers
// (via `TransactionalState` delivery state). On `Discharge` we either run all
// staged operations (commit) or drop them (rollback).
//
// Each staged operation is just a closure: `Action<bool fail>`. The sink/source
// that received the in-txn frame captures whatever it needs to apply later.
//
// Txns are scoped per coordinator link via `TxnScope`: when a link drops without
// an explicit Discharge, only that scope's txns roll back, not every open txn
// across all connections.
sealed class TxnManager
{
    private readonly Dictionary<int, Transaction> _transactions = new();
    private int _nextId;
    private readonly ILogger<TxnManager> _logger;

    public TxnManager(ILoggerFactory? loggerFactory = null)
    {
        _logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<TxnManager>();
    }

    public TxnScope CreateScope() => new(this);

    public Transaction GetTransaction(byte[] txnId)
    {
        var id = BitConverter.ToInt32(txnId, 0);
        lock (_transactions)
        {
            if (!_transactions.TryGetValue(id, out var txn))
                throw new AmqpException(new Error(ErrorCode.NotFound) { Description = $"Transaction {id} not found." });
            return txn;
        }
    }

    internal byte[] Declare(TxnScope scope)
    {
        var id = Interlocked.Increment(ref _nextId);
        var txn = new Transaction(id);
        lock (_transactions) _transactions[id] = txn;
        scope.Track(id);
        _logger.LogTrace("Txn declared id={Id}", id);
        return BitConverter.GetBytes(id);
    }

    internal void Discharge(TxnScope scope, byte[] txnId, bool fail)
    {
        var id = BitConverter.ToInt32(txnId, 0);
        Transaction? txn;
        lock (_transactions)
        {
            if (!_transactions.TryGetValue(id, out txn))
                throw new AmqpException(new Error(ErrorCode.NotFound) { Description = $"Transaction {id} not found." });
            _transactions.Remove(id);
        }
        scope.Untrack(id);
        _logger.LogTrace("Txn discharge id={Id} fail={Fail} ops={Ops}", id, fail, txn.OperationCount);
        txn.Run(fail);
    }

    internal void RollbackScope(TxnScope scope)
    {
        var ids = scope.Drain();
        if (ids.Length == 0) return;
        var orphans = new List<Transaction>(ids.Length);
        lock (_transactions)
        {
            foreach (var id in ids)
            {
                if (_transactions.Remove(id, out var txn)) orphans.Add(txn);
            }
        }
        foreach (var txn in orphans)
        {
            _logger.LogInformation("Rolling back orphaned txn id={Id} ops={Ops}", txn.Id, txn.OperationCount);
            txn.Run(fail: true);
        }
    }
}

// Per-coordinator-link tracking. The `CoordinatorMessageSink` registers itself
// with a fresh scope and hooks the link's Closed event to RollbackScope.
sealed class TxnScope
{
    private readonly TxnManager _manager;
    private readonly HashSet<int> _ids = new();

    internal TxnScope(TxnManager manager) { _manager = manager; }

    public byte[] Declare() => _manager.Declare(this);
    public void Discharge(byte[] txnId, bool fail) => _manager.Discharge(this, txnId, fail);
    public void Rollback() => _manager.RollbackScope(this);

    internal void Track(int id) { lock (_ids) _ids.Add(id); }
    internal void Untrack(int id) { lock (_ids) _ids.Remove(id); }
    internal int[] Drain()
    {
        lock (_ids)
        {
            var arr = _ids.ToArray();
            _ids.Clear();
            return arr;
        }
    }
}

sealed class Transaction
{
    private readonly Queue<Action<bool>> _operations = new();

    public Transaction(int id) { Id = id; }
    public int Id { get; }
    public int OperationCount { get { lock (_operations) return _operations.Count; } }

    public void AddOperation(Action<bool> op)
    {
        lock (_operations) _operations.Enqueue(op);
    }

    public void Run(bool fail)
    {
        Action<bool>[] ops;
        lock (_operations)
        {
            ops = _operations.ToArray();
            _operations.Clear();
        }
        foreach (var op in ops) op(fail);
    }
}

// Coordinator link sink. Receives `Declare` and `Discharge` bodies and replies
// with the appropriate outcome via the listener link's DisposeMessage. We bypass
// MessageContext.Complete because Declare's response is a `Declared{TxnId}`,
// which isn't one of the outcomes Complete supports.
//
// The sink owns a `TxnScope` whose lifetime matches the link: on link close any
// undischarged txns declared via this scope auto-rollback.
sealed class CoordinatorMessageSink : IMessageProcessor
{
    private readonly TxnScope _scope;
    private readonly ILogger<CoordinatorMessageSink> _logger;

    public CoordinatorMessageSink(TxnScope scope, ILogger<CoordinatorMessageSink>? logger = null)
    {
        _scope = scope;
        _logger = logger ?? NullLogger<CoordinatorMessageSink>.Instance;
    }

    public int Credit => 100;

    public void Process(MessageContext messageContext)
    {
        var link = messageContext.Link;
        var message = messageContext.Message;
        try
        {
            switch (message.Body)
            {
                case Declare:
                    var txnId = _scope.Declare();
                    link.DisposeMessage(message, new Declared { TxnId = txnId }, true);
                    break;

                case Discharge discharge:
                    _scope.Discharge(discharge.TxnId, discharge.Fail);
                    link.DisposeMessage(message, new Accepted(), true);
                    break;

                default:
                    link.DisposeMessage(message,
                        new Rejected { Error = new Error(ErrorCode.NotImplemented) { Description = "Unsupported coordinator body." } },
                        true);
                    break;
            }
        }
        catch (AmqpException ex)
        {
            _logger.LogWarning("Coordinator op rejected: {Description}", ex.Error.Description);
            link.DisposeMessage(message, new Rejected { Error = ex.Error }, true);
        }
    }
}
