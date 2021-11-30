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