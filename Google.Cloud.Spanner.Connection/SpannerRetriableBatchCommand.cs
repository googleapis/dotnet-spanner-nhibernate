﻿// Copyright 2021 Google LLC
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

using Google.Cloud.Spanner.Data;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.Connection
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// Batch DML command that will automatically retry the underlying transaction if it is aborted by Cloud Spanner.
    /// </summary>
    public class SpannerRetriableBatchCommand
    {
        private readonly IList<SpannerCommand> _commands = new List<SpannerCommand>();

        public SpannerRetriableBatchCommand()
        {
        }

        internal SpannerRetriableBatchCommand(SpannerRetriableConnection connection)
        {
            Connection = connection;
        }

        public SpannerRetriableConnection Connection { get; set; }

        public SpannerRetriableTransaction Transaction { get; set; }

        public void Add(string sql) => _commands.Add(Connection.SpannerConnection.CreateDmlCommand(sql));

        public void Add(SpannerCommand command) => _commands.Add(command);

        public void Add(SpannerRetriableCommand command) => _commands.Add(command.SpannerCommand);

        public int CommandCount => _commands.Count;

        internal SpannerBatchCommand CreateSpannerBatchCommand(SpannerTransaction transaction)
        {
            var batch = transaction.CreateBatchDmlCommand();
            foreach (var cmd in _commands)
            {
                batch.Add(cmd);
            }
            return batch;
        }

        public IEnumerable<long> ExecuteNonQuery() =>
            Transaction == null
            ? ExecuteNonQueryWithRetry() 
            : Transaction.ExecuteNonQueryWithRetry(this);

        private IEnumerable<long> ExecuteNonQueryWithRetry()
        {
            return Connection.SpannerConnection.RunWithRetriableTransaction(transaction =>
            {
                var spannerBatchCommand = CreateSpannerBatchCommand(transaction);
                return spannerBatchCommand.ExecuteNonQuery();
            });
        }

        public Task<IReadOnlyList<long>> ExecuteNonQueryAsync(CancellationToken cancellationToken = default) =>
            Transaction == null
            ? ExecuteNonQueryWithRetryAsync(cancellationToken)
            : Transaction.ExecuteNonQueryWithRetryAsync(this, cancellationToken);

        private Task<IReadOnlyList<long>> ExecuteNonQueryWithRetryAsync(CancellationToken cancellationToken)
        {
            return Connection.SpannerConnection.RunWithRetriableTransactionAsync(transaction =>
            {
                var spannerBatchCommand = transaction.CreateBatchDmlCommand();
                foreach (var cmd in _commands)
                {
                    spannerBatchCommand.Add(cmd);
                }
                return spannerBatchCommand.ExecuteNonQueryAsync(cancellationToken);
            }, cancellationToken);
        }
    }
}
