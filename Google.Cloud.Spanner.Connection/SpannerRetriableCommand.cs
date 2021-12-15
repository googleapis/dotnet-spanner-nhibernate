// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Api.Gax;
using Google.Cloud.Spanner.Data;
using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Spanner = Google.Cloud.Spanner.Data;

namespace Google.Cloud.Spanner.Connection
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// <see cref="DbCommand"/> implementation for Cloud Spanner that can be retried if the underlying
    /// Spanner transaction is aborted.
    /// </summary>
    public class SpannerRetriableCommand : DbCommand
    {
        private SpannerRetriableConnection _connection;
        private readonly SpannerCommand _spannerCommand;
        private SpannerTransactionBase _transaction;

        /// <summary>
        /// Creates a new SpannerRetriableCommand that will use the given SpannerCommand as its underlying command.
        /// </summary>
        /// <param name="spannerCommand"></param>
        public SpannerRetriableCommand(SpannerCommand spannerCommand) => _spannerCommand = spannerCommand;

        internal SpannerRetriableCommand(SpannerRetriableConnection connection, SpannerCommand spannerCommand)
        {
            _connection = connection;
            _spannerCommand = (SpannerCommand)GaxPreconditions.CheckNotNull(spannerCommand, nameof(spannerCommand)).Clone();
        }

        public virtual object Clone() => new SpannerRetriableCommand(_connection, _spannerCommand.Clone() as SpannerCommand)
        {
            Transaction = _transaction,
        };

        /// <inheritdoc />
        public override string CommandText { get => _spannerCommand.CommandText; set => _spannerCommand.CommandText = value; }
        
        /// <inheritdoc />
        public override int CommandTimeout { get => _spannerCommand.CommandTimeout; set => _spannerCommand.CommandTimeout = value; }
        
        /// <inheritdoc />
        public override CommandType CommandType { get => _spannerCommand.CommandType; set => _spannerCommand.CommandType = value; }
        
        /// <inheritdoc />
        public override bool DesignTimeVisible { get => _spannerCommand.DesignTimeVisible; set => _spannerCommand.DesignTimeVisible = value; }
        
        /// <inheritdoc />
        public override UpdateRowSource UpdatedRowSource { get => _spannerCommand.UpdatedRowSource; set => _spannerCommand.UpdatedRowSource = value; }
        protected override DbConnection DbConnection
        {
            get => _connection;
            set
            {
                if (!(value is SpannerRetriableConnection retriableConnection))
                {
                    throw new ArgumentException( "The connection must be a SpannerRetriableConnection", nameof(value));
                }
                _connection = retriableConnection;
                _spannerCommand.Connection = retriableConnection.SpannerConnection;
            } 
        }

        protected override DbTransaction DbTransaction
        {
            get => _transaction;
            set => _transaction = (SpannerTransactionBase)value;
        }
        
        /// <summary>
        /// The <see cref="Google.Cloud.Spanner.Data.TimestampBound"/> that will be used for queries that are executed
        /// using this command without a transaction.
        /// </summary>
        public TimestampBound TimestampBound { get; set; }

        /// <summary>
        /// Returns the underlying <see cref="Google.Cloud.Spanner.Data.SpannerCommand"/>
        /// </summary>
        public SpannerCommand SpannerCommand => _spannerCommand;

        protected override DbParameterCollection DbParameterCollection => _spannerCommand.Parameters;

        protected override DbParameter CreateDbParameter() => new SpannerParameter();

        /// <inheritdoc />
        public override void Cancel() => _spannerCommand.Cancel();

        /// <inheritdoc />
        public override int ExecuteNonQuery() =>
            _transaction?.ExecuteNonQueryWithRetry(_spannerCommand) ?? ExecuteNonQueryWithRetry(_spannerCommand);

        /// <summary>
        /// Wraps a DML command in a Spanner retriable transaction to retry Aborted errors.
        /// </summary>
        private int ExecuteNonQueryWithRetry(SpannerCommand spannerCommand)
        {
            var builder = SpannerCommandTextBuilder.FromCommandText(spannerCommand.CommandText);
            if (builder.SpannerCommandType == SpannerCommandType.Ddl)
            {
                return spannerCommand.ExecuteNonQuery();
            }
            return _connection.SpannerConnection.RunWithRetriableTransaction(transaction =>
            {
                spannerCommand.Transaction = transaction;
                return spannerCommand.ExecuteNonQuery();
            });
        }

        /// <inheritdoc />
        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) =>
            _transaction == null
            ? ExecuteNonQueryWithRetryAsync(_spannerCommand, cancellationToken)
            : _transaction.ExecuteNonQueryWithRetryAsync(_spannerCommand, cancellationToken);

        /// <summary>
        /// Wraps a DML command in a Spanner retriable transaction to retry Aborted errors.
        /// </summary>
        private async Task<int> ExecuteNonQueryWithRetryAsync(SpannerCommand spannerCommand, CancellationToken cancellationToken = default)
        {
            var builder = SpannerCommandTextBuilder.FromCommandText(spannerCommand.CommandText);
            if (builder.SpannerCommandType == SpannerCommandType.Ddl)
            {
                return await spannerCommand.ExecuteNonQueryAsync(cancellationToken);
            }
            return await _connection.SpannerConnection.RunWithRetriableTransactionAsync(async transaction =>
            {
                spannerCommand.Transaction = transaction;
                return await spannerCommand.ExecuteNonQueryAsync(cancellationToken);
            }, cancellationToken);
        }

        /// <inheritdoc />
        public override object ExecuteScalar() =>
            _transaction == null
            // These don't need retry protection as the ephemeral transaction used by the client library is a read-only transaction.
            ? _spannerCommand.ExecuteScalar()
            : _transaction.ExecuteScalarWithRetry(_spannerCommand);

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (_transaction != null)
            {
                return _transaction.ExecuteDbDataReaderWithRetry(_spannerCommand);
            }
            // These don't need retry protection as the ephemeral transaction used by the client library is a read-only transaction.
            if (TimestampBound != null || _connection.ReadOnlyStaleness != null && _connection.ReadOnlyStaleness.Mode != TimestampBoundMode.Strong)
            {
                return _spannerCommand.ExecuteReaderAsync(TimestampBound ?? _connection.ReadOnlyStaleness)
                    .ResultWithUnwrappedExceptions();
            }
            return _spannerCommand.ExecuteReader();
        }

        /// <inheritdoc />
        public override void Prepare() => _spannerCommand.Prepare();

        /// <summary>
        /// Executes this command as a partitioned update. The command must be a generalized DML command;
        /// <see cref="SpannerConnection.CreateDmlCommand(string, SpannerParameterCollection)"/> for details.
        /// </summary>
        /// <remarks>
        /// The command is executed in parallel across multiple partitions, and automatically committed as it executes.
        /// This operation is not atomic: if it is cancelled part way through, the data that has already been updated will
        /// remain updated. Additionally, it is performed "at least once" in each partition; if the statement is non-idempotent
        /// (for example, incrementing a column) then the update may be performed more than once on a given row. 
        /// This command must not be part of any other transaction.
        /// </remarks>
        /// <returns>A lower bound for the number of rows affected.</returns>
        public long ExecutePartitionedUpdate() => _spannerCommand.ExecutePartitionedUpdate();

        /// <summary>
        /// Executes this command as a partitioned update. The command must be a generalized DML command;
        /// <see cref="SpannerConnection.CreateDmlCommand(string, SpannerParameterCollection)"/> for details.
        /// </summary>
        /// <remarks>
        /// The command is executed in parallel across multiple partitions, and automatically committed as it executes.
        /// This operation is not atomic: if it is cancelled part way through, the data that has already been updated will
        /// remain updated. Additionally, it is performed "at least once" in each partition; if the statement is non-idempotent
        /// (for example, incrementing a column) then the update may be performed more than once on a given row. 
        /// This command must not be part of any other transaction.
        /// </remarks>
        /// <param name="cancellationToken">An optional token for canceling the call.</param>
        /// <returns>A task whose result is a lower bound for the number of rows affected.</returns>
        public Task<long> ExecutePartitionedUpdateAsync(CancellationToken cancellationToken = default) =>
            _spannerCommand.ExecutePartitionedUpdateAsync(cancellationToken);
    }
}
