// Copyright 2021 Google Inc. All Rights Reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Data;
using NHibernate;
using NHibernate.AdoNet;
using NHibernate.Exceptions;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// NHibernate batcher implementation that will use BatchDml or Mutations to batch
    /// multiple insert/update/delete operations together for a Spanner database.
    /// </summary>
    public class SpannerBatcher : AbstractBatcher
    {
        private int _batchSize;
        private int _totalExpectedRowsAffected;
        private LinkedList<SpannerRetriableCommand> _currentBatch;
        private int _currentBatchStatementCount;
        private bool _containsMutations;
        private bool _containsDmlStatements;
        private bool _preparingBatchCommand;
        
        public SpannerBatcher(ConnectionManager connectionManager, IInterceptor interceptor) : base(connectionManager, interceptor)
        {
            _batchSize = Factory.Settings.AdoBatchSize;
            _currentBatch = new LinkedList<SpannerRetriableCommand>();
        }

        /// <summary>
        /// Mutation usage for batches that use an implicit transaction. That is; when no transaction has been started
        /// on the session. Defaults to Never.
        /// </summary>
        public MutationUsage MutationUsage { get; set; } = MutationUsage.Never;

        public override int BatchSize
        {
            get => _batchSize;
            set => _batchSize = value;
        }

        protected override int CountOfStatementsInCurrentBatch => _currentBatchStatementCount;

        public override void AddToBatch(IExpectation expectation)
        {
            if (CountOfStatementsInCurrentBatch == 0)
            {
                CheckReaders();
            }

            _currentBatchStatementCount++;
            _totalExpectedRowsAffected += expectation.ExpectedRowCount;
            var batchUpdate = CurrentCommand as SpannerRetriableCommand;
            Prepare(batchUpdate);
            AddToBatch(batchUpdate);

            if (_currentBatchStatementCount >= _batchSize)
            {
                DoExecuteBatch(batchUpdate);
            }
        }

        private void AddToBatch(SpannerRetriableCommand batchUpdate)
        {
            Driver.AdjustCommand(batchUpdate);
            var clone = (SpannerRetriableCommand) batchUpdate!.Clone();
            _currentBatch.AddLast(clone);
            _currentBatchStatementCount++;
        }

        private Tuple<List<SpannerRetriableCommand>, List<SpannerRetriableCommand>> GetDmlAndMutationCommands(DbCommand ps)
        {
            var dmlCommands = new List<SpannerRetriableCommand>(_currentBatch.Count);
            var mutationCommands = new List<SpannerRetriableCommand>(_currentBatch.Count);
            var transactionMutationUsage = (ps.Transaction as SpannerRetriableTransaction)?.GetMutationUsage()
                                           ?? MutationUsage.Unspecified;
            foreach (var cmd in _currentBatch)
            {
                cmd.Connection = ps.Connection;
                cmd.Transaction = ps.Transaction;
                if (cmd is SpannerDmlOrMutationCommand dmlOrMutationCommand)
                {
                    if (transactionMutationUsage == MutationUsage.Unspecified && MutationUsage == MutationUsage.Always
                        || ps.Transaction == null && MutationUsage == MutationUsage.ImplicitTransactions
                        || transactionMutationUsage == MutationUsage.Always)
                    {
                        var mutationCommand = dmlOrMutationCommand.MutationCommand;
                        // Copy the parameter values to the mutation command.
                        for (var i = 0; i < Math.Min(cmd.Parameters.Count, mutationCommand.Parameters.Count); i++)
                        {
                            // Do not overwrite any existing values on the parameter. It could be that the DML command
                            // has more parameters than the mutation command, if the DML command is an UPDATE statement
                            // and the command includes a WHERE clause with the current version of the row. The mutation
                            // command on the other hand might contain one or more fixed value parameters, for example
                            // for setting the commit timestamp.
                            mutationCommand.Parameters[i].Value ??= cmd.Parameters[i].Value;
                        }
                        mutationCommands.Add(dmlOrMutationCommand.MutationCommand);
                        _containsMutations = true;
                    }
                    else
                    {
                        // A SpannerDmlOrMutationCommand will default to DML.
                        dmlCommands.Add(dmlOrMutationCommand);
                        _containsDmlStatements = true;
                    }
                }
                else
                {
                    dmlCommands.Add(cmd);
                    _containsDmlStatements = true;
                }
            }
            CheckMutationUsage(ps);
            return new Tuple<List<SpannerRetriableCommand>, List<SpannerRetriableCommand>>(dmlCommands, mutationCommands);
        }

        protected override void DoExecuteBatch(DbCommand ps)
        {
            try
            {
                CheckReaders();
                var rowsAffected = 0;

                var dmlAndMutationCommands = GetDmlAndMutationCommands(ps);
                var dmlCommands = dmlAndMutationCommands.Item1;
                var mutationCommands = dmlAndMutationCommands.Item2;
                try
                {
                    if (dmlCommands.Count == 1)
                    {
                        rowsAffected = dmlCommands[0].ExecuteNonQuery();
                    }
                    else if (dmlCommands.Count > 1)
                    {
                        var batch = new SpannerRetriableBatchCommand();
                        dmlCommands.ForEach(cmd => batch.Add(cmd));
                        batch.Connection = (SpannerRetriableConnection) ps.Connection;
                        batch.Transaction = (SpannerRetriableTransaction) ps.Transaction;
                        // The maximum mutation count for a Spanner transaction is 20,000, so we don't
                        // have to worry that the total update count of a single batch will ever overflow
                        // an int.
                        rowsAffected = (int)batch.ExecuteNonQuery().Sum();
                    }
                    if (mutationCommands.Count > 0)
                    {
                        rowsAffected += DoExecuteMutations(mutationCommands, (SpannerRetriableConnection)ps.Connection,
                            ps.Transaction);
                    }
                }
                catch (DbException e)
                {
                    throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, e, "could not execute batch command.");
                }
                Expectations.VerifyOutcomeBatched(_totalExpectedRowsAffected, rowsAffected, ps);
            }
            finally
            {
                ClearCurrentBatch();
            }
        }

        private int DoExecuteMutations(List<SpannerRetriableCommand> mutations, SpannerRetriableConnection connection, DbTransaction transaction)
        {
            var rowsAffected = 0;
            var ownTransaction = transaction == null;
            if (ownTransaction)
            {
                transaction = connection.BeginTransaction();
            }
            foreach (var mutation in mutations)
            {
                mutation.Connection = connection;
                mutation.Transaction = transaction;
                mutation.ExecuteNonQuery();
                // Each mutation always affects one row.
                rowsAffected++;
            }
            if (ownTransaction)
            {
                transaction.Commit();
            }
            return rowsAffected;
        }

        private void ClearCurrentBatch()
        {
            _totalExpectedRowsAffected = 0;
            _currentBatchStatementCount = 0;
            _containsDmlStatements = false;
            _containsMutations = false;
            _currentBatch = new LinkedList<SpannerRetriableCommand>();
        }

        protected override async Task DoExecuteBatchAsync(DbCommand ps, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await CheckReadersAsync(cancellationToken);
                var rowsAffected = 0;

                var dmlAndMutationCommands = GetDmlAndMutationCommands(ps);
                var dmlCommands = dmlAndMutationCommands.Item1;
                var mutationCommands = dmlAndMutationCommands.Item2;
                try
                {
                    if (dmlCommands.Count == 1)
                    {
                        rowsAffected = await dmlCommands[0].ExecuteNonQueryAsync(cancellationToken);
                    }
                    else if (dmlCommands.Count > 1)
                    {
                        var batch = new SpannerRetriableBatchCommand();
                        dmlCommands.ForEach(cmd => batch.Add(cmd));
                        batch.Connection = (SpannerRetriableConnection) ps.Connection;
                        batch.Transaction = (SpannerRetriableTransaction) ps.Transaction;
                        // The maximum mutation count for a Spanner transaction is 20,000, so we don't
                        // have to worry that the total update count of a single batch will ever overflow
                        // an int.
                        rowsAffected = (int) (await batch.ExecuteNonQueryAsync(cancellationToken)).Sum();
                    }
                    if (mutationCommands.Count > 0)
                    {
                        rowsAffected += await DoExecuteMutationsAsync(mutationCommands, (SpannerRetriableConnection)ps.Connection,
                            ps.Transaction, cancellationToken);
                    }
                }
                catch (DbException e)
                {
                    throw ADOExceptionHelper.Convert(Factory.SQLExceptionConverter, e, "could not execute batch command.");
                }
                Expectations.VerifyOutcomeBatched(_totalExpectedRowsAffected, rowsAffected, ps);
            }
            finally
            {
                ClearCurrentBatch();
            }
        }

        private async Task<int> DoExecuteMutationsAsync(List<SpannerRetriableCommand> mutations, SpannerRetriableConnection connection, DbTransaction transaction, CancellationToken cancellationToken)
        {
            var rowsAffected = 0;
            var ownTransaction = transaction == null;
            if (ownTransaction)
            {
                transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            }
            foreach (var mutation in mutations)
            {
                mutation.Connection = connection;
                mutation.Transaction = transaction;
                await mutation.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                // Each mutation always affects one row.
                rowsAffected++;
            }
            if (ownTransaction)
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            return rowsAffected;
        }

        public override async Task AddToBatchAsync(IExpectation expectation, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (CountOfStatementsInCurrentBatch == 0)
            {
                await CheckReadersAsync(cancellationToken).ConfigureAwait(false);
            }

            _totalExpectedRowsAffected += expectation.ExpectedRowCount;
            var batchUpdate = (SpannerRetriableCommand) CurrentCommand;
            await PrepareAsync(batchUpdate, cancellationToken).ConfigureAwait(false);
            AddToBatch(batchUpdate);

            if (_currentBatchStatementCount >= _batchSize)
            {
                await DoExecuteBatchAsync(batchUpdate, cancellationToken).ConfigureAwait(false);
            }
        }

        protected override void OnPreparedCommand()
        {
            if (!_preparingBatchCommand)
            {
                _containsDmlStatements = true;
                CheckMutationUsage(CurrentCommand);
            }
            base.OnPreparedCommand();
        }

        public override DbCommand PrepareBatchCommand(CommandType type, SqlString sql, SqlType[] parameterTypes)
        {
            _preparingBatchCommand = true;
            try
            {
                return base.PrepareBatchCommand(type, sql, parameterTypes);
            }
            finally
            {
                _preparingBatchCommand = false;
            }
        }

        public override Task<DbCommand> PrepareBatchCommandAsync(CommandType type, SqlString sql,
            SqlType[] parameterTypes, CancellationToken cancellationToken)
        {
            _preparingBatchCommand = true;
            try
            {
                return base.PrepareBatchCommandAsync(type, sql, parameterTypes, cancellationToken);
            }
            finally
            {
                _preparingBatchCommand = false;
            }
        }
        
        protected override Task OnPreparedCommandAsync(CancellationToken cancellationToken)
        {
            if (!_preparingBatchCommand)
            {
                _containsDmlStatements = true;
                CheckMutationUsage(CurrentCommand);
            }
            return base.OnPreparedCommandAsync(cancellationToken);
        }

        private void CheckMutationUsage(DbCommand cmd)
        {
            var transactionMutationUsage = (cmd?.Transaction as SpannerRetriableTransaction)?.GetMutationUsage()
                                           ?? MutationUsage;
            if (_containsMutations && _containsDmlStatements)
            {
                throw new HibernateException(new SpannerException(ErrorCode.FailedPrecondition,
                    "The batch contains both Mutations and DML statements. Mixing both in one batch is not supported. " +
                    "This is probably caused by at least one entity that has a collection that is managed by NHibernate"));
            }
            if (_containsDmlStatements && transactionMutationUsage == MutationUsage.Always)
            {
                throw new HibernateException(new SpannerException(ErrorCode.FailedPrecondition,
                    "The batch contains DML statements while MutationUsage.Always has been specified. " +
                    "This is probably caused by at least one entity that has a collection that is managed by NHibernate"));
            }
        }
    }
}