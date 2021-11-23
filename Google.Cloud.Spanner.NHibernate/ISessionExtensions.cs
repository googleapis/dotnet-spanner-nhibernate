using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Data;
using NHibernate;
using System.Data;

namespace Google.Cloud.Spanner.NHibernate
{
    public static class ISessionExtensions
    {
        public static ISession WithTimestampBound(this ISession session, TimestampBound timestampBound)
        {
            var connection = (SpannerRetriableConnection)session.Connection;
            connection.ReadOnlyStaleness = timestampBound;
            return session;
        }

        public static ITransaction BeginReadOnlyTransaction(this ISession session) =>
            BeginReadOnlyTransaction(session, TimestampBound.Strong);

        public static ITransaction BeginReadOnlyTransaction(this ISession session, TimestampBound timestampBound)
        {
            var connection = (SpannerRetriableConnection)session.Connection;
            connection.ReadOnlyStaleness = timestampBound;
            connection.CreateReadOnlyTransactionForSnapshot = true;
            return session.BeginTransaction(IsolationLevel.Snapshot);
        }
    }
}