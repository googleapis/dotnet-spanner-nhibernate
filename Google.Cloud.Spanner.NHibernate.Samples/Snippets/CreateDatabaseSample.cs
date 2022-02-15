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
    /// This sample shows how to create and delete a database in Cloud Spanner.
    /// 
    /// Run from the command line with `dotnet run CreateDatabaseSample`
    /// </summary>
    public class CreateDatabaseSample
    {
        private static readonly string databaseId = "create-database-sample";

        public static async Task Run(SampleConfiguration configuration)
        {
            // Try to create a database on the emulator.
            var adminClientBuilder = new DatabaseAdminClientBuilder
            {
                EmulatorDetection = EmulatorDetection.EmulatorOnly
            };
            var databaseAdminClient = await adminClientBuilder.BuildAsync();

            var databaseName = new DatabaseName(configuration.ProjectId, configuration.InstanceId, databaseId);
            var instanceName = InstanceName.FromProjectInstance(configuration.ProjectId, configuration.InstanceId);

            var createDatabaseData = await databaseAdminClient.CreateDatabaseAsync(new CreateDatabaseRequest
            {
                ParentAsInstanceName = instanceName,
                CreateStatement = $"CREATE DATABASE `{databaseName.DatabaseId}`",
            });

            if (createDatabaseData.IsFaulted)
            {
                Console.WriteLine("Failed to create a database");
                throw createDatabaseData.Exception;
            }

            Console.WriteLine($"Database `{databaseName.DatabaseId}` created");
            //Try to delete created database.
            Console.WriteLine("Deleting it");
            await databaseAdminClient.DropDatabaseAsync(databaseName);
            Console.WriteLine("Done");
        }
    }
}