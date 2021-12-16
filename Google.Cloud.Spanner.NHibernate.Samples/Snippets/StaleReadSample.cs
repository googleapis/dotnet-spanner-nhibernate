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
    /// Sample for executing a stale read on Spanner through NHibernate without
    /// an explicit read-only transaction.
    /// 
    /// Run from the command line with `dotnet run StaleRead`
    /// </summary>
    public static class StaleReadSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();

            // Get the current timestamp on the backend.
            using var cmd = session.Connection.CreateCommand();
            cmd.CommandText = "SELECT CURRENT_TIMESTAMP";
            var timestamp = (DateTime) await cmd.ExecuteScalarAsync();

            // Search for a singer with a specific last name. This singer will not be found.
            var lastName = "Goldberg";
            var count = await session
                .Query<Singer>()
                .Where(s => s.LastName.Equals(lastName))
                .CountAsync();
            Console.WriteLine($"Searching for singer with last name '{lastName}' yielded {count} result(s)");

            // Create a new session and insert a singer with the given last name.
            using (var writeSession = configuration.SessionFactory.OpenSession())
            {
                await session.SaveAsync(new Singer
                {
                    FirstName = "Alice",
                    LastName = "Goldberg",
                });
                await session.FlushAsync();
            }
            
            // Now execute a stale read on the Singers table at a timestamp that is before the singer was inserted.
            // The count should be 0.
            count = await session
                .WithTimestampBound(TimestampBound.OfReadTimestamp(timestamp))
                .Query<Singer>()
                .Where(s => s.LastName.Equals(lastName))
                .CountAsync();
            Console.WriteLine($"Searching with read timestamp #{timestamp:yyyy-MM-ddTHH:mm:ss.fffffffZ} " +
                              $"for singer with last name '{lastName}' yielded {count} result(s)");

            // Try to read the row with a strong timestamp bound.
            count = await session
                .WithTimestampBound(TimestampBound.Strong)
                .Query<Singer>()
                .Where(s => s.LastName.Equals(lastName))
                .CountAsync();
            Console.WriteLine($"Searching with a strong timestamp bound for singer with last name '{lastName}' yielded {count} result(s)");
            
            // Try to read the singer with a max staleness. The result of this is non-deterministic, as the backend
            // may choose the read timestamp, as long as it is no older than the specified max staleness.
            count = await session
                .WithTimestampBound(TimestampBound.OfMaxStaleness(TimeSpan.FromSeconds(1)))
                .Query<Singer>()
                .Where(s => s.LastName.Equals(lastName))
                .CountAsync();
            Console.WriteLine($"Searching with a max staleness for singer with last name '{lastName}' yielded {count} result(s)");
        }
    }
}