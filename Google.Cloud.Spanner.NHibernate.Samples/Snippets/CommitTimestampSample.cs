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

using Google.Cloud.Spanner.NHibernate.Samples.SampleModel;
using System;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// Cloud Spanner supports writing the commit timestamp of a row: https://cloud.google.com/spanner/docs/commit-timestamp
    /// 
    /// See the <see cref="AbstractVersionedEntity"/> definition for the mapping that is needed to automatically fill a
    /// commit timestamp value when a record is inserted and/or updated.
    /// 
    /// This feature has a couple of limitations that should be taken into consideration before using it:
    /// 1. The commit timestamp is only assigned after the transaction is committed. Flushing the session will not
    ///    cause a commit timestamp to be assigned.
    /// 2. The entity is not automatically reloaded into the session after a transaction has committed. This means an
    ///    application needs to manually refresh the entity in order to inspect the commit timestamp that was assigned,
    ///    if it wants to do so in the same session that saved/updated the entity.
    /// 3. When writing a PENDING_COMMIT_TIMESTAMP to a table, that column will be unreadable for ALL rows in the table
    ///    for the remainder of the transaction. This means that you cannot execute any queries that reference this
    ///    column during the same transaction.
    /// 
    /// See also https://cloud.google.com/spanner/docs/commit-timestamp#dml
    /// 
    /// Run from the command line with `dotnet run CommitTimestampSample`
    /// </summary>
    public static class CommitTimestampSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();

            // Create a new Singer and save it.
            var transaction1 = session.BeginTransaction();
            var singer = new Singer
            {
                FirstName = "Kiara",
                LastName = "Meyers"
            };
            await session.SaveAsync(singer);
            // Committing the transaction will assign a value to the CreatedAt property.
            await transaction1.CommitAsync();
            
            // The value of CreatedAt is not automatically reloaded during the same session, so in order to be able to
            // read it, we must refresh the entity.
            await session.RefreshAsync(singer);
            Console.WriteLine($"Singer {singer.FullName} was created at: {singer.CreatedAt}");
            
            // The LastUpdatedAt property is not set when a record is inserted.
            Console.WriteLine($"Singer {singer.FullName} was updated at: {(singer.LastUpdatedAt == null ? "Never" : singer.LastUpdatedAt.ToString())}");
            
            // Updating one of the properties of the singer will generate a value for LastUpdatedAt.
            var transaction2 = session.BeginTransaction();
            singer.BirthDate = new SpannerDate(1990, 8, 12);
            await transaction2.CommitAsync();
            
            // Refresh the entity to read the latest values for CreatedAt and LastUpdatedAt.
            await session.RefreshAsync(singer);
            Console.WriteLine($"Singer {singer.FullName} was created at: {singer.CreatedAt}");
            Console.WriteLine($"Singer {singer.FullName} was updated at: {(singer.LastUpdatedAt == null ? "Never" : singer.LastUpdatedAt.ToString())}");
        }
    }
}