using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Data;
using NHibernate;
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
    }
}