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
using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.Common.V1;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// This sample shows how to create a database with encryption key in Cloud Spanner.
    /// This example cannot be run on the Google Spanner emulator.
    /// To run this sample please fill fields instanceId, projectId, databaseId, kmsKeyName.
    /// Run from the command line with `dotnet run CreateDatabaseWithEncryptionKeySample`
    /// </summary>
    public class CreateDatabaseWithEncryptionKeySample
    {
        // TODO(developer): Replace these variables before running the sample.
        private static readonly string instanceId = "create-database-instance";
        private static readonly string projectId = "create-database-project";
        private static readonly string databaseId = "create-database-sample";
        private static readonly string kmsKeyName = $"projects/{projectId}/locations/<location>/keyRings/<keyRing>/cryptoKeys/<keyId>";

        public static async Task Run(SampleConfiguration configuration)
        {
            // Try to create a database.
            var adminClientBuilder = new DatabaseAdminClientBuilder();
            var databaseAdminClient = await adminClientBuilder.BuildAsync();

            var databaseName = new DatabaseName(projectId, instanceId, databaseId);
            var instanceName = InstanceName.FromProjectInstance(projectId, instanceId);

            var createDatabaseData = await databaseAdminClient.CreateDatabaseAsync(new CreateDatabaseRequest
            {
                ParentAsInstanceName = instanceName,
                CreateStatement = $"CREATE DATABASE `{databaseName.DatabaseId}`",
                EncryptionConfig = new EncryptionConfig
                {
                    KmsKeyName = kmsKeyName
                }
            });

            if (createDatabaseData.IsFaulted)
            {
                Console.WriteLine("Failed to create a database");
                throw createDatabaseData.Exception;
            }

            Console.WriteLine($"Database `{databaseName.DatabaseId}` created with encryption key");
            Console.WriteLine("Deleting it");
            await databaseAdminClient.DropDatabaseAsync(databaseName);
            Console.WriteLine("Done");
        }
    }
}