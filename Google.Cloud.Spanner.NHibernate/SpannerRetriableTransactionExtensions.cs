using Google.Api.Gax;
using Google.Cloud.Spanner.Connection;

namespace Google.Cloud.Spanner.NHibernate
{
    public static class SpannerRetriableTransactionExtensions
    {
        /// <summary>
        /// Gets the setting for mutation usage for the given Spanner transaction.
        /// </summary>
        /// <param name="transaction">The transaction to get the mutation usage setting for</param>
        /// <returns>The current mutation usage setting</returns>
        public static MutationUsage GetMutationUsage(this SpannerRetriableTransaction transaction)
        {
            if (transaction.Attributes.TryGetValue("MutationUsage", out var value) && value is MutationUsage mutationUsage)
            {
                return mutationUsage;
            }
            return MutationUsage.Unspecified;
        }

        /// <summary>
        /// Sets the mutation usage setting for the Spanner transaction.
        /// </summary>
        /// <param name="transaction">The transaction to set the mutation usage for</param>
        /// <param name="mutationUsage">The mutation usage to set for the transaction</param>
        /// <returns>The transaction for chaining</returns>
        public static SpannerRetriableTransaction SetMutationUsage(this SpannerRetriableTransaction transaction, MutationUsage mutationUsage)
        {
            GaxPreconditions.CheckArgument(mutationUsage != MutationUsage.ImplicitTransactions, nameof(mutationUsage), $"Mutation usage may not be {MutationUsage.ImplicitTransactions}");
            transaction.Attributes["MutationUsage"] = mutationUsage;
            return transaction;
        }
    }
}