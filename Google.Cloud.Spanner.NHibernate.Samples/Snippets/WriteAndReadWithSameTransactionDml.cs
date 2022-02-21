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
    /// This sample shows how to use the same transaction for write and read data from database in Cloud Spanner.
    /// 
    /// Run from the command line with `dotnet run WriteAndReadWithSameTransactionDmlSample`
    /// </summary>
    public class WriteAndReadWithSameTransactionDmlSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();
            //Create one transaction for insert and select commands
            using var transaction = session.BeginTransaction();
            //Create insert command
            var insertCommand = session.Connection.CreateCommand();
            insertCommand.CommandText =
                @"INSERT INTO Singers (Id, LastName, FirstName, BirthDate, Version, CreatedAt) VALUES
                ('121', 'Garcia', 'Melissa', '1980-11-11', 1, '2020-11-11'),
                ('131', 'Morales', 'Russell', '1980-11-11', 1, '2020-11-11' ),
                ('141', 'Long', 'Jacqueline', '1980-11-11', 1, '2020-11-11' ),
                ('151', 'Shaw', 'Dylan', '1980-11-11', 1, '2020-11-11')";
            //Create select command
            var selectCommand = session.Connection.CreateCommand();
            selectCommand.CommandText = @"SELECT Id FROM Singers";
            //Use the same transaction for insert and select commands
            transaction.Enlist(insertCommand);
            transaction.Enlist(selectCommand);
            
            Console.WriteLine("Inserting samples data...");
            var rowsInserted = await insertCommand.ExecuteNonQueryAsync();
            Console.WriteLine($"{rowsInserted} rows inserted");
            
            Console.WriteLine("Reading inserted data in the same transaction...");
            var reader = await selectCommand.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                Console.WriteLine($"Here as an selected Id: {reader.GetFieldValue<string>(0)}");
            }

            //Rollback transaction to ensure that insert command was rolled back
            Console.WriteLine("Rolling back transaction...");
            await transaction.RollbackAsync();
            
            Console.WriteLine("Checking that rows were not inserted");
            var selectCommandAfterRollback = session.Connection.CreateCommand();
            selectCommandAfterRollback.CommandText = @"SELECT Id FROM Singers";
            var readerAfterRollback = await selectCommandAfterRollback.ExecuteReaderAsync();
            Console.WriteLine($"After rollback rows were not inserted - {!readerAfterRollback.HasRows}");
        }
    }
}