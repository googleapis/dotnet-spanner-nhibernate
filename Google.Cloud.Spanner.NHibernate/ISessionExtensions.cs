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
using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Data;
using NHibernate;
using NHibernate.Impl;
using System.Data;

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// Extension methods for NHibernate sessions when working with Google Cloud Spanner.
    /// All methods in this class assume that the given NHibernate session is linked to a Google Cloud Spanner
    /// connection.
    /// </summary>
    public static class ISessionExtensions
    {
        /// <summary>
        /// Gets the underlying <see cref="SpannerRetriableConnection"/> for this session.
        /// </summary>
        /// <param name="session">The session to get the connection from</param>
        /// <returns>The connection that is used by this session</returns>
        public static SpannerRetriableConnection GetSpannerConnection(this ISession session) =>
            (SpannerRetriableConnection)session.Connection;
        
        /// <summary>
        /// Instruct the given session to use the given <see cref="TimestampBound"/> for the next query or read-only
        /// transaction. The given session will be modified and then returned for call chaining.
        /// </summary>
        /// <param name="session">The session to modify to use a different timestamp bound</param>
        /// <param name="timestampBound">The <see cref="TimestampBound"/> to use</param>
        /// <returns>The modified session</returns>
        public static ISession WithTimestampBound(this ISession session, TimestampBound timestampBound)
        {
            var connection = (SpannerRetriableConnection)session.Connection;
            connection.ReadOnlyStaleness = timestampBound;
            return session;
        }

        /// <summary>
        /// Begins a read-only transaction on the given session with a <see cref="TimestampBound.Strong"/> timestamp
        /// bound.
        /// </summary>
        /// <param name="session">The session to start the transaction on</param>
        /// <returns>
        /// A new read-only transaction. The transaction must be committed or rolled back in order to release the
        /// resources that the transaction is using. There is no semantic difference between a Commit or a Rollback call
        /// on a read-only transaction.
        /// </returns>
        public static ITransaction BeginReadOnlyTransaction(this ISession session) =>
            BeginReadOnlyTransaction(session, TimestampBound.Strong);

        /// <summary>
        /// Begins a read-only transaction on the given session with the given <see cref="TimestampBound"/>.
        /// </summary>
        /// <param name="session">The session to start the transaction on</param>
        /// <param name="timestampBound">The <see cref="TimestampBound"/> to use for the transaction</param>
        /// <returns>
        /// A new read-only transaction. The transaction must be committed or rolled back in order to release the
        /// resources that the transaction is using. There is no semantic difference between a Commit or a Rollback call
        /// on a read-only transaction.
        /// </returns>
        public static ITransaction BeginReadOnlyTransaction(this ISession session, TimestampBound timestampBound)
        {
            var connection = (SpannerRetriableConnection)session.Connection;
            connection.ReadOnlyStaleness = timestampBound;
            connection.CreateReadOnlyTransactionForSnapshot = true;
            return session.BeginTransaction(IsolationLevel.Snapshot);
        }

        /// <summary>
        /// Begins a read/write transaction on the given session that will use mutations as specified in the
        /// mutationUsage argument.
        /// </summary>
        /// <param name="session">The session to start the transaction on</param>
        /// <param name="mutationUsage">The mutation usage for the transaction. Must be either Always or Never.</param>
        /// <returns>A new read/write transaction</returns>
        public static ITransaction BeginTransaction(this ISession session, MutationUsage mutationUsage)
        {
            GaxPreconditions.CheckArgument(mutationUsage == MutationUsage.Always || mutationUsage == MutationUsage.Never, nameof(mutationUsage), $"value must be one of {MutationUsage.Always} and {MutationUsage.Never}");
            var transaction = session.BeginTransaction();
            var dbTransaction = transaction.GetDbTransaction();
            dbTransaction.SetMutationUsage(mutationUsage);
            var batcher = (SpannerBatcher) ((SessionImpl) session).Batcher;
            batcher.MutationUsage = mutationUsage;
            return transaction;
        }

        /// <summary>
        /// Gets the current mutation usage for batches on this session.
        /// </summary>
        /// <param name="session">The session to get the mutation usage for</param>
        /// <returns>The current mutation usage for batches</returns>
        public static MutationUsage GetBatchMutationUsage(this ISession session)
        {
            GaxPreconditions.CheckArgument(session is SessionImpl, nameof(session), "The session must be an instance of SessionImpl");
            GaxPreconditions.CheckArgument(((SessionImpl) session).Batcher is SpannerBatcher, nameof(session), "The session must use a SpannerBatcher");
            var batcher = (SpannerBatcher) ((SessionImpl) session).Batcher;
            return batcher.MutationUsage;
        }
        
        /// <summary>
        /// Sets the mutation usage for batches on the given session.
        /// </summary>
        /// <param name="session">The session to set the mutation usage for</param>
        /// <param name="mutationUsage">The mutation usage to set</param>
        public static ISession SetBatchMutationUsage(this ISession session, MutationUsage mutationUsage)
        {
            GaxPreconditions.CheckArgument(mutationUsage != MutationUsage.Unspecified, nameof(mutationUsage), $"Mutation usage may not be {MutationUsage.Unspecified}");
            GaxPreconditions.CheckArgument(session is SessionImpl, nameof(session), "The session must be an instance of SessionImpl");
            GaxPreconditions.CheckArgument(((SessionImpl) session).Batcher is SpannerBatcher, nameof(session), "The session must use a SpannerBatcher");
            var batcher = (SpannerBatcher) ((SessionImpl) session).Batcher;
            batcher.MutationUsage = mutationUsage;
            return session;
        }
    }
}