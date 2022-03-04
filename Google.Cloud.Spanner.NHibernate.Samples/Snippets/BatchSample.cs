// Copyright 2022 Google Inc. All Rights Reserved.
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Cloud.Spanner.NHibernate.Samples.SampleModel;
using NHibernate;
using NHibernate.Multi;
using NHibernate.Type;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// This sample shows how to use a batch query in NHibernate.
    /// 
    /// Run from the command line with `dotnet run BatchSample`
    /// </summary>
    public class BatchSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            //Preparing samples.
            await Setup(configuration);

            using var session = configuration.SessionFactory.OpenSession();

            var firstNames = new List<string> { "Alice", "Peter", "Bobby", "Tom" };

            //First query - get singers from FirstName list.
            var singersQuery = session
                .CreateSQLQuery("SELECT * FROM Singers WHERE FirstName IN UNNEST(:id)")
                .AddEntity(typeof(Singer))
                .SetParameter("id", new SpannerStringArray(firstNames));

            //Second query - get total singers count.
            var totalSingersQuery = session
                .CreateSQLQuery("SELECT COUNT(*) as cnt FROM Singers")
                .AddScalar("cnt", new Int64Type());

            //Setting a batch query.
            var queries = session.CreateQueryBatch()
                .Add<Singer>(singersQuery)
                .Add<long>(totalSingersQuery);

            Console.WriteLine("Executing batch query");
            await queries.ExecuteAsync(default);

            //Get singers.
            var singers = await queries.GetResultAsync<Singer>(0, default);
            Console.WriteLine("Here are the selected singers:");
            foreach (var singer in singers)
            {
                Console.WriteLine($"\tId: {singer.Id}");
            }
            //Get singers count.
            var totalSingers = (await queries.GetResultAsync<long>(1, default)).FirstOrDefault();
            Console.WriteLine($"Total rows count in table Singers - {totalSingers}");
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
            await session.SaveAsync(new Singer
            {
                FirstName = "Tom",
                LastName = "Hamilton",
                BirthDate = new SpannerDate(1966, 3, 11),
            });
            await session.FlushAsync();
            await session.SaveAsync(new Singer
            {
                FirstName = "Bobby",
                LastName = "Ferry",
                BirthDate = new SpannerDate(1986, 9, 24),
            });
            await session.FlushAsync();
        }
    }
}