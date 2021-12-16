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
using NHibernate.Linq;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// Linq can be used to query entities from Cloud Spanner.
    /// 
    /// Run from the command line with `dotnet run QueryWithLinq`
    /// </summary>
    public static class QueryWithLinqSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            await Setup(configuration);

            using var session = configuration.SessionFactory.OpenSession();

            var singersBornBefore2000 = await session
                .Query<Singer>()
                .Where(s => s.BirthDate < new SpannerDate(2000, 1, 1))
                .OrderBy(s => s.BirthDate)
                .ToListAsync();
            Console.WriteLine("Singers born before 2000:");
            foreach (var singer in singersBornBefore2000)
            {
                Console.WriteLine($"\t{singer.FullName}, born at {singer.BirthDate}");
            }
            Console.WriteLine();

            var singersStartingWithAl = await session
                .Query<Singer>()
                .Where(s => s.FullName.StartsWith("Al"))
                .OrderBy(s => s.LastName)
                .ToListAsync();
            Console.WriteLine("Singers with a name starting with 'Al':");
            foreach (var singer in singersStartingWithAl)
            {
                Console.WriteLine($"\t{singer.FullName}");
            }
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