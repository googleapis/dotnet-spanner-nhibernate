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
using Google.Api.Gax;
using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.Common.V1;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// This sample shows how to add a new column on table.
    /// 
    /// Run from the command line with `dotnet run AddNewColumnSample`
    /// </summary>
    public class AddNewColumnSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            var adminClientBuilder = new DatabaseAdminClientBuilder
            {
                EmulatorDetection = EmulatorDetection.EmulatorOnly
            };
            var databaseAdminClient = await adminClientBuilder.BuildAsync();

            var databaseName = new DatabaseName(configuration.ProjectId, configuration.InstanceId, configuration.DatabaseId);
            //Executing DDL command to add a column to table
            var updateDbData = await databaseAdminClient.UpdateDatabaseDdlAsync(databaseName, new[] { "ALTER TABLE Albums ADD COLUMN Sales NUMERIC" });

            if (updateDbData.IsFaulted)
            {
                Console.WriteLine("Failed to update a database");
                throw updateDbData.Exception;
            }

            Console.WriteLine($"Database `{databaseName.DatabaseId}` updated");
        }
    }
}