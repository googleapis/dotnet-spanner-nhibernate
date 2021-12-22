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
using NHibernate.Criterion;
using NHibernate.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Environment = NHibernate.Cfg.Environment;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// Cloud Spanner supports multiple query hints. The Cloud Spanner NHibernate driver is able
    /// to pick up hints that are specified as comments and translate these into actual query hints
    /// for Cloud Spanner. This feature requires the following:
    /// 1. Comments must be enabled in your NHibernate configuration.
    /// 2. The <see cref="SpannerQueryHintInterceptor"/> must be included in either the NHibernate
    ///    configuration or session.
    ///
    /// The driver includes a number of extension methods that will automatically generate the
    /// required comments. It is strongly recommended to use these extension methods when adding
    /// hints to a query.
    ///
    /// See https://cloud.google.com/spanner/docs/query-syntax#table-hints for more information
    /// on possible table, join and statement hints for Cloud Spanner.
    /// 
    /// Run from the command line with `dotnet run QueryHint`
    /// </summary>
    public static class QueryHintSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            // Make sure that all prerequisites have been met.
            if (!configuration.Configuration.Properties.TryGetValue(Environment.UseSqlComments, out var useComments)
                || !bool.Parse(useComments))
            {
                throw new ArgumentException("Comments must be enabled to use query hints");
            }
            // Check whether the SpannerQueryHintInterceptor is already the default interceptor for this configuration.
            // If it is, you don't need to explicitly add it to sessions that use query hints. 
            var queryHintInterceptorIsDefault = configuration.Configuration.Interceptor is SpannerQueryHintInterceptor;
            Console.WriteLine($"Configuration is using SpannerQueryHintInterceptor by default: {queryHintInterceptorIsDefault}");
            Console.WriteLine();

            // Insert a couple of example Singer rows.
            await Setup(configuration);

            // Create a session and make sure the SpannerQueryHintInterceptor is used for the session.
            using var session = queryHintInterceptorIsDefault
                ? configuration.SessionFactory.OpenSession()
                : configuration.SessionFactory.WithOptions().Interceptor(new SpannerQueryHintInterceptor()).OpenSession();
        
            // The Spanner driver contains extension methods that can be used to set both statement, table and join
            // hints for Linq, Criteria and HQL queries.
            
            // Example for adding a table hint to a Linq query.
            var singersOrderedByFullName = await session
                .Query<Singer>()
                // Table hints must be specified using the name of the table (and not the name of the entity).
                // So in this case 'Singers' and not 'Singer'.
                .SetHints(Hints.TableHint("Singers", "@{FORCE_INDEX=Idx_Singers_FullName}"))
                .OrderBy(s => s.FullName)
                .ToListAsync();
            Console.WriteLine("Singers ordered by FullName using FORCE_INDEX table hint:");
            foreach (var singer in singersOrderedByFullName)
            {
                Console.WriteLine($"\t{singer.FullName}");
            }
            Console.WriteLine();
            
            // Example for adding a join hint to a Linq query.
            var singersFromAlbums = await session
                .Query<Album>()
                .Select(a => a.Singer)
                // Join hints must be specified using the right-hand table name of the join operation,
                // and not the entity name or the alias in the query.
                .SetHints(Hints.JoinHint("Singers", "@{JOIN_METHOD=APPLY_JOIN}"))
                .OrderBy(s => s.LastName)
                .ToListAsync();
            Console.WriteLine("Singers ordered by LastName with at least one Album using APPLY_JOIN join hint:");
            foreach (var singer in singersFromAlbums)
            {
                Console.WriteLine($"\t{singer.FullName}");
            }
            Console.WriteLine();

            // Example for adding a statement hint to a Criteria query.
            var singersUsingAdditionalParallelism = await session
                .CreateCriteria(typeof(Singer))
                .SetHints(Hints.StatementHint("@{USE_ADDITIONAL_PARALLELISM=TRUE}"))
                .AddOrder(Order.Desc("FullName"))
                .ListAsync<Singer>();
            Console.WriteLine("Singers ordered by FullName DESC using a statement hint:");
            foreach (var singer in singersUsingAdditionalParallelism)
            {
                Console.WriteLine($"\t{singer.FullName}");
            }
            Console.WriteLine();
            
            // Example for adding a statement, table and a join hint to a HQL query.
            var singersUsingAdditionalParallelismAndJoinAndTableHint = await session
                .CreateQuery("select s from Singer s left outer join s.Albums as a order by s.FullName, s.Id, a.Title")
                .SetHints(Hints
                    .NewBuilder()
                    .SetStatementHint("@{USE_ADDITIONAL_PARALLELISM=TRUE}")
                    // Table hints must be specified using the table name, and not the entity name or the alias
                    // in the query.
                    .SetTableHint("Singers", "@{FORCE_INDEX=Idx_Singers_FullName}")
                    .SetTableHint("Albums", "@{FORCE_INDEX=Idx_Albums_Title}")
                    // Join hints must be specified using the right-hand table name of the join operation,
                    // and not the entity name or the alias in the query.
                    .SetJoinHint("Albums", "@{JOIN_METHOD=HASH_JOIN}")
                    .Build())
                .ListAsync<Singer>();
            Console.WriteLine("Singers order by FullName using both a statement hint and table hints:");
            foreach (var singer in singersUsingAdditionalParallelismAndJoinAndTableHint)
            {
                Console.WriteLine($"\t{singer.FullName}");
            }
            Console.WriteLine();
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
            var singer = new Singer
            {
                FirstName = "Mike",
                LastName = "Nicholson",
                BirthDate = new SpannerDate(1976, 8, 31),
            };
            await session.SaveAsync(singer);
            await session.SaveAsync(new Album
            {
                Singer = singer,
                Title = "Hot Jam",
            });
            await session.FlushAsync();
        }
    }
}