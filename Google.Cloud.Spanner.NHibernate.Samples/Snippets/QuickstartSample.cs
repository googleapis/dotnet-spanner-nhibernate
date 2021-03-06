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
using System;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// Simple sample showing how to create an NHibernate session, start a transaction and insert a new record.
    /// 
    /// Run from the command line with `dotnet run Quickstart`
    /// </summary>
    public static class QuickstartSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();

            // Create a new Singer and save it. Note that we don't assign the record a value for the Id column. The id
            // is automatically generated by NHibernate when the entity is saved.
            var transaction = session.BeginTransaction();
            var singer = new Singer
            {
                FirstName = "Jamie",
                LastName = "Yngvason"
            };
            await session.SaveAsync(singer);
            await transaction.CommitAsync();

            Console.WriteLine($"Added singer {singer.FullName} with id {singer.Id}.");
        }
    }
}