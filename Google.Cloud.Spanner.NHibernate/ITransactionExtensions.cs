using Google.Api.Gax;
using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Data;
using NHibernate;

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// Extension methods for NHibernate transactions when working with Google Cloud Spanner.
    /// All methods in this class assume that the given NHibernate transaction is linked to a Google Cloud Spanner
    /// connection.
    /// </summary>
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
            GaxPreconditions.CheckArgument(transaction.IsActive, nameof(transaction), "The transaction must be active");
            var cmd = new SpannerRetriableCommand(new SpannerCommand());
            transaction.Enlist(cmd);
            return (SpannerRetriableTransaction)cmd.Transaction;
        }

        /// <summary>
        /// Disables internal retries of Aborted errors for the given transactions. Any Aborted error will then be
        /// propagated to the client application.
        /// </summary>
        /// <param name="transaction">The NHibernate read/write transaction to disable retries for.</param>
        public static void DisableInternalRetries(this ITransaction transaction)
        {
            var spannerRetriableTransaction = GetDbTransaction(transaction);
            spannerRetriableTransaction.EnableInternalRetries = false;
        }
    }
}