// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Api.Gax;
using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.NHibernate.Internal;
using NHibernate.Cfg;
using NHibernate.Dialect;
using NHibernate.Id;
using NHibernate.Mapping;
using NHibernate.Tool.hbm2ddl;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Environment = NHibernate.Cfg.Environment;

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// Schema export implementation specifically for Cloud Spanner. Use this instead of the normal
    /// <see cref="SchemaExport"/> when working with Cloud Spanner.
    ///
    /// This schema exporter will ensure that:
    /// 1. The generated DDL is compatible with Cloud Spanner.
    /// 2. DDL scripts are executed as one batch instead of as individual statements. This significantly improves the
    ///    execution speed of large DDL batches.
    /// </summary>
    public class SpannerSchemaExport : SchemaExport
    {
        private static readonly SimpleValue DisablePrimaryKeyGenerator = new SimpleValue { IdentifierGeneratorStrategy = typeof(IdentityGenerator).AssemblyQualifiedName };
        private static readonly Dialect ExportDialect = new SpannerSchemaExportDialect();
        
        private readonly Dictionary<Table, IKeyValue> _primaryKeysGenerators = new Dictionary<Table, IKeyValue>();
        private readonly Dictionary<Table, string> _tableComments = new Dictionary<Table, string>();

        private readonly Configuration _configuration;


        public SpannerSchemaExport(Configuration cfg) : this(cfg, cfg.Properties)
        {
        }

        public SpannerSchemaExport(Configuration cfg, IDictionary<string, string> configProperties)
            : base(cfg, ReplaceDialectAndConnectionProvider(configProperties))
        {
            _configuration = cfg;
        }

        /// <inheritdoc cref="SchemaExport.Create(bool,bool)"/>
        public new void Create(bool useStdOut, bool execute) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Create(useStdOut, execute));

        /// <inheritdoc cref="SchemaExport.CreateAsync(bool,bool,CancellationToken)"/>
        public new async Task CreateAsync(bool useStdOut, bool execute, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.CreateAsync(useStdOut, execute, cancellationToken));

        /// <inheritdoc cref="SchemaExport.Create(bool,bool,DbConnection)"/>
        public new void Create(bool useStdOut, bool execute, DbConnection connection) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Create(useStdOut, execute, WrapInBatchConnection(connection)));

        /// <inheritdoc cref="SchemaExport.CreateAsync(bool,bool,DbConnection,CancellationToken)"/>
        public new async Task CreateAsync(bool useStdOut, bool execute, DbConnection connection, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.CreateAsync(useStdOut, execute, WrapInBatchConnection(connection), cancellationToken));

        /// <inheritdoc cref="SchemaExport.Create(Action&lt;string&gt;,bool)"/>
        public new void Create(Action<string> scriptAction, bool execute) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Create(scriptAction, execute));

        /// <inheritdoc cref="SchemaExport.CreateAsync(Action&lt;string&gt;,bool,CancellationToken)"/>
        public new async Task CreateAsync(Action<string> scriptAction, bool execute, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.CreateAsync(scriptAction, execute, cancellationToken));

        /// <inheritdoc cref="SchemaExport.Create(Action&lt;string&gt;,bool,DbConnection)"/>
        public new void Create(Action<string> scriptAction, bool execute, DbConnection connection) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Create(scriptAction, execute, WrapInBatchConnection(connection)));

        /// <inheritdoc cref="SchemaExport.CreateAsync(Action&lt;string&gt;,bool,DbConnection,CancellationToken)"/>
        public new async Task CreateAsync(Action<string> scriptAction, bool execute, DbConnection connection, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.CreateAsync(scriptAction, execute, WrapInBatchConnection(connection), cancellationToken));
        
        /// <inheritdoc cref="SchemaExport.Create(TextWriter,bool)"/>
        public new void Create(TextWriter exportOutput, bool execute) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Create(exportOutput, execute));
        
        /// <inheritdoc cref="SchemaExport.CreateAsync(TextWriter,bool,CancellationToken)"/>
        public new async Task CreateAsync(TextWriter exportOutput, bool execute, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.CreateAsync(exportOutput, execute, cancellationToken));

        /// <inheritdoc cref="SchemaExport.Create(TextWriter,bool,DbConnection)"/>
        public new void Create(TextWriter exportOutput, bool execute, DbConnection connection) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Create(exportOutput, execute, WrapInBatchConnection(connection)));

        /// <inheritdoc cref="SchemaExport.CreateAsync(TextWriter,bool,DbConnection,CancellationToken)"/>
        public new async Task CreateAsync(TextWriter exportOutput, bool execute, DbConnection connection, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.CreateAsync(exportOutput, execute, WrapInBatchConnection(connection), cancellationToken));

        /// <inheritdoc cref="SchemaExport.Drop(bool,bool)"/>
        public new void Drop(bool useStdOut, bool execute) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Drop(useStdOut, execute));

        /// <inheritdoc cref="SchemaExport.DropAsync(bool,bool,CancellationToken)"/>
        public new async Task DropAsync(bool useStdOut, bool execute, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.DropAsync(useStdOut, execute, cancellationToken));

        /// <inheritdoc cref="SchemaExport.Drop(bool,bool,DbConnection)"/>
        public new void Drop(bool useStdOut, bool execute, DbConnection connection) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Drop(useStdOut, execute, WrapInBatchConnection(connection)));

        /// <inheritdoc cref="SchemaExport.DropAsync(bool,bool,DbConnection,CancellationToken)"/>
        public new async Task DropAsync(bool useStdOut, bool execute, DbConnection connection, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.DropAsync(useStdOut, execute, WrapInBatchConnection(connection), cancellationToken));

        /// <inheritdoc cref="SchemaExport.Drop(TextWriter,bool)"/>
        public new void Drop(TextWriter exportOutput, bool execute) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Drop(exportOutput, execute));

        /// <inheritdoc cref="SchemaExport.DropAsync(TextWriter,bool,CancellationToken)"/>
        public new async Task DropAsync(TextWriter exportOutput, bool execute, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.DropAsync(exportOutput, execute, cancellationToken));

        /// <inheritdoc cref="SchemaExport.Drop(TextWriter,bool,DbConnection)"/>
        public new void Drop(TextWriter exportOutput, bool execute, DbConnection connection) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Drop(exportOutput, execute, WrapInBatchConnection(connection)));

        /// <inheritdoc cref="SchemaExport.DropAsync(TextWriter,bool,DbConnection,CancellationToken)"/>
        public new async Task DropAsync(TextWriter exportOutput, bool execute, DbConnection connection, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.DropAsync(exportOutput, execute, WrapInBatchConnection(connection), cancellationToken));

        /// <inheritdoc cref="SchemaExport.Execute(bool,bool,bool,DbConnection,TextWriter)"/>
        public new void Execute(bool useStdOut, bool execute, bool justDrop, DbConnection connection,
            TextWriter exportOutput) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Execute(useStdOut, execute, justDrop, WrapInBatchConnection(connection), exportOutput));

        /// <inheritdoc cref="SchemaExport.ExecuteAsync(bool,bool,bool,DbConnection,TextWriter,CancellationToken)"/>
        public new async Task ExecuteAsync(bool useStdOut, bool execute, bool justDrop, DbConnection connection,
            TextWriter exportOutput, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.ExecuteAsync(useStdOut, execute, justDrop, WrapInBatchConnection(connection), exportOutput, cancellationToken));
        
        /// <inheritdoc cref="SchemaExport.Execute(Action&lt;string&gt;,bool,bool,DbConnection,TextWriter)"/>
        public new void Execute(Action<string> scriptAction, bool execute, bool justDrop, DbConnection connection,
            TextWriter exportOutput) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Execute(scriptAction, execute, justDrop, WrapInBatchConnection(connection), exportOutput));
        
        /// <inheritdoc cref="SchemaExport.ExecuteAsync(Action&lt;string&gt;,bool,bool,DbConnection,TextWriter,CancellationToken)"/>
        public new async Task ExecuteAsync(Action<string> scriptAction, bool execute, bool justDrop, DbConnection connection,
            TextWriter exportOutput, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.ExecuteAsync(scriptAction, execute, justDrop, WrapInBatchConnection(connection), exportOutput, cancellationToken));

        /// <inheritdoc cref="SchemaExport.Execute(bool,bool,bool)"/>
        public new void Execute(bool useStdOut, bool execute, bool justDrop) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Execute(useStdOut, execute, justDrop));

        /// <inheritdoc cref="SchemaExport.ExecuteAsync(bool,bool,bool,CancellationToken)"/>
        public new async Task ExecuteAsync(bool useStdOut, bool execute, bool justDrop, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.ExecuteAsync(useStdOut, execute, justDrop, cancellationToken));

        /// <inheritdoc cref="SchemaExport.Execute(Action&lt;string&gt;,bool,bool)"/>
        public new void Execute(Action<string> scriptAction, bool execute, bool justDrop) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Execute(scriptAction, execute, justDrop));

        /// <inheritdoc cref="SchemaExport.ExecuteAsync(Action&lt;string&gt;,bool,bool,CancellationToken)"/>
        public new async Task ExecuteAsync(Action<string> scriptAction, bool execute, bool justDrop, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.ExecuteAsync(scriptAction, execute, justDrop, cancellationToken));

        /// <inheritdoc cref="SchemaExport.Execute(Action&lt;string&gt;,bool,bool,TextWriter)"/>
        public new void Execute(Action<string> scriptAction, bool execute, bool justDrop, TextWriter exportOutput) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Execute(scriptAction, execute, justDrop, exportOutput));

        /// <inheritdoc cref="SchemaExport.ExecuteAsync(Action&lt;string&gt;,bool,bool,TextWriter,CancellationToken)"/>
        public new async Task ExecuteAsync(Action<string> scriptAction, bool execute, bool justDrop, TextWriter exportOutput, CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.ExecuteAsync(scriptAction, execute, justDrop, exportOutput, cancellationToken));
        
        private DbConnection WrapInBatchConnection(DbConnection connection) =>
            connection is SpannerRetriableConnection spannerRetriableConnection
                ? new DdlBatchConnection(spannerRetriableConnection)
                : connection;

        private void ExecuteWithPrimaryKeysAsComment(Action action)
        {
            try
            {
                MovePrimaryKeysToComment(_configuration, _tableComments, _primaryKeysGenerators);
                action.Invoke();
            }
            finally
            {
                ResetPrimaryKeys(_configuration, _tableComments, _primaryKeysGenerators);
            }
        }
        
        private async Task ExecuteWithPrimaryKeysAsCommentAsync(Func<Task> action)
        {
            try
            {
                MovePrimaryKeysToComment(_configuration, _tableComments, _primaryKeysGenerators);
                await action.Invoke();
            }
            finally
            {
                ResetPrimaryKeys(_configuration, _tableComments, _primaryKeysGenerators);
            }
        }

        internal static IDictionary<string, string> ReplaceDialectAndConnectionProvider(IDictionary<string, string> properties)
        {
            GaxPreconditions.CheckNotNull(properties, nameof(properties));
            properties[Environment.Dialect] = typeof(SpannerSchemaExportDialect).AssemblyQualifiedName;
            if (properties.TryGetValue(Environment.ConnectionProvider, out var providerClass))
            {
                properties[$"wrapped.{Environment.ConnectionProvider}"] = providerClass;
            }
            properties[Environment.ConnectionProvider] = typeof(DdlBatchConnectionProvider).AssemblyQualifiedName;
            return properties;
        }

        internal static void MovePrimaryKeysToComment(Configuration configuration, Dictionary<Table, string> tableComments, Dictionary<Table, IKeyValue> primaryKeysGenerators)
        {
            foreach (var mapping in configuration.ClassMappings)
            {
                if (mapping.Table.IdentifierValue != DisablePrimaryKeyGenerator)
                {
                    tableComments[mapping.Table] = mapping.Table.Comment;
                    primaryKeysGenerators[mapping.Table] = mapping.Table.IdentifierValue;
                    mapping.Table.Comment = mapping.Table.PrimaryKey?.SqlConstraintString(ExportDialect, "");
                    mapping.Table.IdentifierValue = DisablePrimaryKeyGenerator;
                }
            }
        }

        internal static void ResetPrimaryKeys(Configuration configuration, Dictionary<Table, string> tableComments, Dictionary<Table, IKeyValue> primaryKeysGenerators)
        {
            foreach (var mapping in configuration.ClassMappings)
            {
                if (mapping.Table.IdentifierValue == DisablePrimaryKeyGenerator)
                {
                    mapping.Table.Comment = tableComments[mapping.Table];
                    mapping.Table.IdentifierValue = primaryKeysGenerators[mapping.Table];
                }
            }
        }
    }
}