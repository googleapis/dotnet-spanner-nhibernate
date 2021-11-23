using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Data;
using NHibernate;

namespace Google.Cloud.Spanner.NHibernate
{
    public static class ITransactionExtensions
    {
        /// <summary>
        /// Returns the underlying DbTransaction of an NHibernate transaction. This method can only be called while the
        /// NHibernate transaction is still active (that is; before it is committed or rolled back).
        /// </summary>
        /// <param name="transaction">The NHibernate transaction</param>
        /// <returns>The underlying DbTransaction of the NHibernate transaction</returns>
        public static SpannerRetriableTransaction GetDbTransaction(this ITransaction transaction)
        {
            var cmd = new SpannerRetriableCommand(new SpannerCommand());
            transaction.Enlist(cmd);
            return (SpannerRetriableTransaction)cmd.Transaction;
        }
    }
}