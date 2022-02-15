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

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// This sample shows how to list all instance configs in Cloud Spanner.
    /// 
    /// Run from the command line with `dotnet run ListInstanceConfigsSample`
    /// </summary>
    public class ListInstanceConfigsSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            var instanceAdminClientBuilder = new InstanceAdminClientBuilder
            {
                EmulatorDetection = EmulatorDetection.EmulatorOnly
            };
            var instanceAdminClient = await instanceAdminClientBuilder.BuildAsync();

            var instanceConfigs = instanceAdminClient.ListInstanceConfigsAsync(ProjectName.FromProject(configuration.ProjectId)).GetAsyncEnumerator();

            Console.WriteLine($"Configs for project `{configuration.ProjectId}` instance `{configuration.InstanceId}`:");

            while (await instanceConfigs.MoveNextAsync())
            {
                Console.WriteLine($"\t config: `{instanceConfigs.Current.Name}`");
            }
        }
    }
}