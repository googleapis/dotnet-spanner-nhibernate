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
using System.Threading.Tasks;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.NHibernate.Samples.SampleModel;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{

    /// <summary>
    /// This sample shows how to use STRUCT in Cloud Spanner.
    /// 
    /// Run from the command line with `dotnet run StructParametersSample`
    /// </summary>
    public class StructParametersSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            Console.WriteLine("Querying data with a STRUCT object");
            //Preparing samples
            await Setup(configuration);
            // Creating SpannerStruct instance
            var nameStruct = new SpannerStruct
            {
                { "FirstName", SpannerDbType.String, "Alice" },
                { "LastName", SpannerDbType.String, "Henderson" },
            };

            using (var session = configuration.SessionFactory.OpenSession())
            {
                using (var connection = session.GetSpannerConnection())
                {
                    //Executing SQL command with Struct parameter
                    using (var cmd = connection.CreateSelectCommand(
                               "SELECT Id FROM Singers WHERE STRUCT<FirstName STRING, LastName STRING>(FirstName, LastName) = @name"
                               , new SpannerParameterCollection(new List<SpannerParameter> { new SpannerParameter("name", nameStruct.GetSpannerDbType(), nameStruct) })))
                    {
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                Console.WriteLine($"Here as an Id: {reader.GetFieldValue<string>(0)}");
                            }
                        }
                    }
                }
            }

            Console.WriteLine("Querying data with an ARRAY of STRUCT objects");
            //Creating SpannerStruct collection
            var bandMembers = new List<SpannerStruct>
            {
                new SpannerStruct { { "FirstName", SpannerDbType.String, "Peter" },
                    { "LastName", SpannerDbType.String, "Allison" } },
                new SpannerStruct { { "FirstName", SpannerDbType.String, "Mike" },
                    { "LastName", SpannerDbType.String, "Nicholson" } }
            };

            using (var session = configuration.SessionFactory.OpenSession())
            {
                using (var connection = session.GetSpannerConnection())
                {
                    //Executing SQL command with Struct collection parameter
                    using (var cmd = connection.CreateSelectCommand("SELECT Id FROM Singers WHERE STRUCT<FirstName STRING, LastName STRING>(FirstName, LastName) IN UNNEST(@names)"
                               , new SpannerParameterCollection(new List<SpannerParameter> { new SpannerParameter("names", SpannerDbType.ArrayOf(nameStruct.GetSpannerDbType()), bandMembers) })))
                    {
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                Console.WriteLine($"Id: {reader.GetFieldValue<string>(0)}");
                            }
                        }
                    }
                }
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