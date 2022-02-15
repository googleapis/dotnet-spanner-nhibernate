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
using Google.Api.Gax.ResourceNames;
using Google.Cloud.Spanner.Admin.Instance.V1;
using Google.Cloud.Spanner.Common.V1;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// This sample shows how to create and delete an instance in Cloud Spanner.
    /// 
    /// Run from the command line with `dotnet run CreateInstanceSample`
    /// </summary>
    public class CreateInstanceSample
    {
        private static readonly string instanceId = "create-instance-sample";

        public static async Task Run(SampleConfiguration configuration)
        {
            // Try to create an instance on the emulator.
            var adminClientBuilder = new InstanceAdminClientBuilder
            {
                EmulatorDetection = EmulatorDetection.EmulatorOnly
            };
            var instanceAdminClient = await adminClientBuilder.BuildAsync();

            var instanceName = InstanceName.FromProjectInstance(configuration.ProjectId, instanceId);

            var createInstanceData=await instanceAdminClient.CreateInstanceAsync(new CreateInstanceRequest
            {
                InstanceId = instanceName.InstanceId,
                ParentAsProjectName = ProjectName.FromProject(configuration.ProjectId),
                Instance = new Instance
                {
                    InstanceName = instanceName,
                    ConfigAsInstanceConfigName = new InstanceConfigName(configuration.ProjectId, $"{instanceId}-config"),
                    DisplayName = "Create instance sample",
                    NodeCount = 1,
                },
            });

            if (createInstanceData.IsFaulted)
            {
                Console.WriteLine("Failed to create an instance");
                throw createInstanceData.Exception;
            }

            Console.WriteLine($"Instance `{createInstanceData.Name}` created");
            //Try to delete created instance.
            Console.WriteLine("Deleting it");
            await instanceAdminClient.DeleteInstanceAsync(instanceName);
            Console.WriteLine("Done");
        }
    }
}