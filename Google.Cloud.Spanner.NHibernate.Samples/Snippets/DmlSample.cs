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
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// This sample shows how to use DML command to manipulate data in Cloud Spanner.
    /// 
    /// Run from the command line with `dotnet run DmlSample`
    /// </summary>
    public class DmlSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();
            var cmd = session.Connection.CreateCommand();

            cmd.CommandText =
                @"INSERT INTO Singers (Id, LastName, FirstName, BirthDate, Version, CreatedAt) VALUES
                ('121', 'Garcia', 'Melissa', '1980-11-11', 1, '2020-11-11'),
                ('131', 'Morales', 'Russell', '1980-11-11', 1, '2020-11-11' ),
                ('141', 'Long', 'Jacqueline', '1980-11-11', 1, '2020-11-11' ),
                ('151', 'Shaw', 'Dylan', '1980-11-11', 1, '2020-11-11')";
            
            var insertedRowsCount = (long)await cmd.ExecuteNonQueryAsync();

            Console.WriteLine($"{insertedRowsCount} rows inserted");
        }
    }
}