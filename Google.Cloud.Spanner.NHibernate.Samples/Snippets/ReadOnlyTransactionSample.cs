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

using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.NHibernate.Samples.SampleModel;
using NHibernate.Linq;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// Sample for executing a read-only transaction on Spanner through NHibernate.
    /// Prefer read-only transactions over read/write transactions if you need to execute
    /// multiple consistent reads and no write operations.
    /// 
    /// Run from the command line with `dotnet run ReadOnlyTransaction`
    /// </summary>
    public static class ReadOnlyTransactionSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();

            // Start a read-only transaction with strong timestamp bound (i.e. read everything that has been committed
            // up until now). This transaction will be assigned a read-timestamp at the first read that it executes and
            // all following read operations will also use the same read timestamp. Any changes that are made after
            // this read timestamp will not be visible to the transaction.
            // NOTE: Although read-only transaction cannot be committed or rolled back, they still need to be disposed.
            using var readOnlyTransaction = session.BeginReadOnlyTransaction(TimestampBound.Strong);
            
            // Search for a singer with a specific last name.
            // This will establish a read timestamp for the read-only transaction.
            var lastName = "Goldberg";
            var count = await session
                .Query<Singer>()
                .Where(s => s.LastName.Equals(lastName))
                .CountAsync();
            Console.WriteLine($"Searching for singer with last name '{lastName}' yielded {count} result(s)");

            // Create a new session and insert a singer with the given last name. This singer will not be visible
            // to the read-only transaction.
            using (var writeSession = configuration.SessionFactory.OpenSession())
            {
                using var transaction = writeSession.BeginTransaction();
                await writeSession.SaveAsync(new Singer
                {
                    FirstName = "Alice",
                    LastName = lastName,
                });
                await transaction.CommitAsync();
            }

            // The count should not have changed, as the read-only transaction will continue to use
            // the read timestamp assigned during the first read, and that timestamp is before the transaction that
            // added the new singer.
            count = await session
                .Query<Singer>()
                .Where(s => s.LastName.Equals(lastName))
                .CountAsync();
            Console.WriteLine($"Searching for singer with last name '{lastName}' yielded {count} result(s)");

            // Now 'commit' the read-only transaction. This will close the transaction and allow us to start
            // a new transaction on the session.
            await readOnlyTransaction.CommitAsync();

            // Start a new read-only transaction. TimestampBound.Strong is default so we don't have to specify it.
            using var newTransaction = session.BeginReadOnlyTransaction();
            count = await session
                .Query<Singer>()
                .Where(s => s.LastName.Equals(lastName))
                .CountAsync();
            Console.WriteLine($"Searching for singer with last name '{lastName}' yielded {count} result(s)");
        }
    }
}