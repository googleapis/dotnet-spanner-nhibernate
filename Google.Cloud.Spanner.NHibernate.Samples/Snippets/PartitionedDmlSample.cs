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
    /// Cloud Spanner can execute DML statements in transactional mode (the default) or
    /// using Partitioned DML. Partitioned DML is not directly supported in NHibernate,
    /// but the underlying SpannerConnection can be extracted from a session and can be
    /// used to execute Partitioned DML.
    /// 
    /// See https://cloud.google.com/spanner/docs/dml-partitioned for more information
    /// on Partitioned DML.
    /// 
    /// Run from the command line with `dotnet run PartitionedDml`
    /// </summary>
    public static class PartitionedDmlSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            // Insert a couple of example Singer rows.
            await Setup(configuration);

            using var session = configuration.SessionFactory.OpenSession();
        
            // Now delete all Singer rows using Partitioned DML.
            // For this we first need to get the underlying Spanner connection.
            var connection = session.GetSpannerConnection();
            // Execute a Partitioned DML statement using the connection.
            using var cmd = connection.CreateDmlCommand("DELETE FROM Singers WHERE TRUE");
            var deleted = await cmd.ExecutePartitionedUpdateAsync();

            Console.WriteLine($"Deleted {deleted} Singer records using Partitioned DML");            
        }
        
        private static async Task Setup(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();
            await session.SaveAsync(new Singer
            {
                FirstName = "Alice",
                LastName = "Henderson",
                BirthDate = new SpannerDate(1983, 10, 19),
            });
            await session.FlushAsync();
            await session.SaveAsync(new Singer
            {
                FirstName = "Peter",
                LastName = "Allison",
                BirthDate = new SpannerDate(2000, 5, 2),
            });
            await session.FlushAsync();
            await session.SaveAsync(new Singer
            {
                FirstName = "Mike",
                LastName = "Nicholson",
                BirthDate = new SpannerDate(1976, 8, 31),
            });
            await session.FlushAsync();
        }
    }
}