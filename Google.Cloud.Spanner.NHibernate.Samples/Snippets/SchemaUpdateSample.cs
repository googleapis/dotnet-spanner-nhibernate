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
using NHibernate.Mapping.ByCode;
using NHibernate.Tool.hbm2ddl;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate.Samples.Snippets
{
    /// <summary>
    /// Sample showing how to use <see cref="SchemaUpdate"/> with Google Cloud Spanner.
    ///
    /// The standard NHibernate <see cref="SchemaUpdate"/> is not compatible with the DDL dialect of Cloud Spanner.
    /// The Cloud Spanner driver therefore comes with its own implementation in <see cref="SpannerSchemaUpdate"/>. The
    /// SpannerSchemaUpdate generates DDL that is compatible with Cloud Spanner, and automatically executes multiple DDL
    /// statements as one batch, which significantly improves execution speed for large database schemas.
    /// 
    /// Run from the command line with `dotnet run SchemaUpdate`
    /// </summary>
    public static class SchemaUpdateSample
    {
        public static async Task Run(SampleConfiguration configuration)
        {
            using var session = configuration.SessionFactory.OpenSession();
            
            // The sample runner automatically creates the data model that is required for the entities that are used
            // by the samples in this project. The following query gets the number of tables currently in the database.
            // The SQL script that is used to create the data model can be found in `SampleModel/SampleDataModel.sql`.
            var cmd = session.Connection.CreateCommand();
            cmd.CommandText =
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='' AND TABLE_SCHEMA=''";
            var numberOfTablesBeforeUpdate = (long) await cmd.ExecuteScalarAsync();

            // Add two new entities to the mapping and then run the schema updater.
            var mapper = new ModelMapper();
            mapper.AddMapping<ManagerMapping>();
            mapper.AddMapping<ContractMapping>();
            configuration.Configuration.AddMapping(mapper.CompileMappingForAllExplicitlyAddedEntities());
            
            // Create a SpannerSchemaUpdate using the updated configuration.
            var updater = new SpannerSchemaUpdate(configuration.Configuration);
            // This will automatically execute an update script and print the statements that are executed to
            // the console (StdOut).
            await updater.ExecuteAsync(true /*useStdOut*/, true /*doUpdate*/);
            
            // Get the new table count. The updater should create two new tables plus two foreign key constraints
            // between the Contracts and Singers/Managers tables.
            cmd.CommandText =
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='' AND TABLE_SCHEMA=''";
            var numberOfTablesAfterDropAndRecreate = (long) await cmd.ExecuteScalarAsync();
            
            Console.WriteLine();
            Console.WriteLine("Updated the schema using the updated NHibernate mapping");
            Console.WriteLine($"Tables BEFORE update: {numberOfTablesBeforeUpdate}");
            Console.WriteLine($"Tables AFTER update: {numberOfTablesAfterDropAndRecreate}");
        }

        /// <summary>
        /// A Manager has management relationships with one or more Singers.
        /// </summary>
        public class Manager : AbstractVersionedEntity
        {
            public virtual string Name { get; set; }
            
            public virtual SpannerNumeric Rate { get; set; }
            
            public virtual IList<Contract> Contracts { get; set; }
        }

        public class ManagerMapping : VersionedEntityMapping<Manager>
        {
            public ManagerMapping()
            {
                Table("Managers");
                Property(x => x.Name, m =>
                {
                    m.NotNullable(true);
                    m.Length(200);
                });
                Property(x => x.Rate, m => m.NotNullable(true));
                Bag(x => x.Contracts, m =>
                {
                    // Set Inverse(true) to make sure that the Contract side of the relationship is
                    // the owner of the relationship.
                    m.Inverse(true);
                    m.Key(key => key.Column("ManagerId"));
                }, r => r.OneToMany());
            }
        }

        /// <summary>
        /// A Contract represents a management contract between a Manager and a Singer for a specific period.
        /// </summary>
        public class Contract : AbstractVersionedEntity
        {
            public virtual Manager Manager { get; set; }
            
            public virtual Singer Singer { get; set; }
            
            public virtual SpannerDate StartDate { get; set; }
            
            public virtual SpannerDate EndDate { get; set; }
        }

        public class ContractMapping : VersionedEntityMapping<Contract>
        {
            public ContractMapping()
            {
                Table("Contracts");
                ManyToOne(x => x.Manager, m =>
                {
                    m.Column(c =>
                    {
                        c.Name("ManagerId");
                        c.NotNullable(true);
                        c.Length(36);
                    });
                    m.ForeignKey("FK_Contracts_Manager");
                });
                ManyToOne(x => x.Singer, m =>
                {
                    m.Column(c =>
                    {
                        c.Name("SingerId");
                        c.NotNullable(true);
                        c.Length(36);
                    });
                    m.ForeignKey("FK_Contracts_Singer");
                });
                Property(x => x.StartDate, m => m.NotNullable(true));
                Property(x => x.EndDate);
            }
        }
    }
}