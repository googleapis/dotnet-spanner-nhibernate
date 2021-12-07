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
using Google.Cloud.Spanner.Data;
using NHibernate.Cfg;
using NHibernate.Connection;
using NHibernate.Dialect;
using NHibernate.Driver;
using NHibernate.Id;
using NHibernate.Mapping;
using NHibernate.Tool.hbm2ddl;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
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
        private readonly SimpleValue _disablePrimaryKeyGenerator = new SimpleValue { IdentifierGeneratorStrategy = typeof(IdentityGenerator).AssemblyQualifiedName };
        
        private readonly Dictionary<Table, IKeyValue> _primaryKeysGenerators = new Dictionary<Table, IKeyValue>();
        private readonly Dictionary<Table, string> _tableComments = new Dictionary<Table, string>();

        private readonly Configuration _configuration;

        private readonly Dialect _dialect = new SpannerSchemaExportDialect();

        public SpannerSchemaExport(Configuration cfg)
            : this(cfg, new Dictionary<string, string> {{Environment.Dialect, typeof(SpannerSchemaExportDialect).AssemblyQualifiedName}})
        {
        }

        public SpannerSchemaExport(Configuration cfg, IDictionary<string, string> configProperties)
            : base(cfg, GaxPreconditions.CheckNotNull(configProperties, nameof(configProperties)))
        {
            _configuration = cfg;
            configProperties[Environment.Dialect] = typeof(SpannerSchemaExportDialect).AssemblyQualifiedName;
            if (configProperties.TryGetValue(Environment.ConnectionProvider, out var providerClass))
            {
                configProperties[$"wrapped.{Environment.ConnectionProvider}"] = providerClass;
            }
            configProperties[Environment.ConnectionProvider] = typeof(DdlBatchConnectionProvider).AssemblyQualifiedName;
        }

        /// <inheritdoc cref="SchemaExport.Create(bool,bool)"/>
        public new void Create(bool useStdOut, bool execute) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Create(useStdOut, execute));

        /// <inheritdoc cref="SchemaExport.Create(bool,bool,DbConnection)"/>
        public new void Create(bool useStdOut, bool execute, DbConnection connection) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Create(useStdOut, execute, WrapInBatchConnection(connection)));

        /// <inheritdoc cref="SchemaExport.Create(Action&lt;string&gt;,bool)"/>
        public new void Create(Action<string> scriptAction, bool execute) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Create(scriptAction, execute));

        /// <inheritdoc cref="SchemaExport.Create(Action&lt;string&gt;,bool,DbConnection)"/>
        public new void Create(Action<string> scriptAction, bool execute, DbConnection connection) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Create(scriptAction, execute, WrapInBatchConnection(connection)));
        
        /// <inheritdoc cref="SchemaExport.Create(TextWriter,bool)"/>
        public new void Create(TextWriter exportOutput, bool execute) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Create(exportOutput, execute));

        /// <inheritdoc cref="SchemaExport.Create(TextWriter,bool,DbConnection)"/>
        public new void Create(TextWriter exportOutput, bool execute, DbConnection connection) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Create(exportOutput, execute, WrapInBatchConnection(connection)));

        /// <inheritdoc cref="SchemaExport.Drop(bool,bool)"/>
        public new void Drop(bool useStdOut, bool execute) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Drop(useStdOut, execute));

        /// <inheritdoc cref="SchemaExport.Drop(bool,bool,DbConnection)"/>
        public new void Drop(bool useStdOut, bool execute, DbConnection connection) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Drop(useStdOut, execute, WrapInBatchConnection(connection)));

        /// <inheritdoc cref="SchemaExport.Drop(TextWriter,bool)"/>
        public new void Drop(TextWriter exportOutput, bool execute) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Drop(exportOutput, execute));

        /// <inheritdoc cref="SchemaExport.Drop(TextWriter,bool,DbConnection)"/>
        public new void Drop(TextWriter exportOutput, bool execute, DbConnection connection) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Drop(exportOutput, execute, WrapInBatchConnection(connection)));

        /// <inheritdoc cref="SchemaExport.Execute(bool,bool,bool,DbConnection,TextWriter)"/>
        public new void Execute(bool useStdOut, bool execute, bool justDrop, DbConnection connection,
            TextWriter exportOutput) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Execute(useStdOut, execute, justDrop, WrapInBatchConnection(connection), exportOutput));
        
        /// <inheritdoc cref="SchemaExport.Execute(Action&lt;string&gt;,bool,bool,DbConnection,TextWriter)"/>
        public new void Execute(Action<string> scriptAction, bool execute, bool justDrop, DbConnection connection,
            TextWriter exportOutput) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Execute(scriptAction, execute, justDrop, WrapInBatchConnection(connection), exportOutput));

        /// <inheritdoc cref="SchemaExport.Execute(bool,bool,bool)"/>
        public new void Execute(bool useStdOut, bool execute, bool justDrop) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Execute(useStdOut, execute, justDrop));

        /// <inheritdoc cref="SchemaExport.Execute(Action&lt;string&gt;,bool,bool)"/>
        public new void Execute(Action<string> scriptAction, bool execute, bool justDrop) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Execute(scriptAction, execute, justDrop));

        /// <inheritdoc cref="SchemaExport.Execute(Action&lt;string&gt;,bool,bool,TextWriter)"/>
        public new void Execute(Action<string> scriptAction, bool execute, bool justDrop, TextWriter exportOutput) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Execute(scriptAction, execute, justDrop, exportOutput));
        
        private DbConnection WrapInBatchConnection(DbConnection connection) =>
            connection is SpannerRetriableConnection spannerRetriableConnection
                ? new DdlBatchConnection(spannerRetriableConnection)
                : connection;

        private void ExecuteWithPrimaryKeysAsComment(Action action)
        {
            try
            {
                MovePrimaryKeysToComment();
                action.Invoke();
            }
            finally
            {
                ResetPrimaryKeys();
            }
        }

        private void MovePrimaryKeysToComment()
        {
            foreach (var mapping in _configuration.ClassMappings)
            {
                if (mapping.Table.IdentifierValue != _disablePrimaryKeyGenerator)
                {
                    _tableComments[mapping.Table] = mapping.Table.Comment;
                    _primaryKeysGenerators[mapping.Table] = mapping.Table.IdentifierValue;
                    mapping.Table.Comment = mapping.Table.PrimaryKey?.SqlConstraintString(_dialect, "");
                    mapping.Table.IdentifierValue = _disablePrimaryKeyGenerator;
                }
            }
        }

        private void ResetPrimaryKeys()
        {
            foreach (var mapping in _configuration.ClassMappings)
            {
                if (mapping.Table.IdentifierValue == _disablePrimaryKeyGenerator)
                {
                    mapping.Table.Comment = _tableComments[mapping.Table];
                    mapping.Table.IdentifierValue = _primaryKeysGenerators[mapping.Table];
                }
            }
        }
    }
    internal class SpannerSchemaExportDialect : SpannerDialect
    {
        // Cloud Spanner does not support any identity columns, but this allows us to skip the primary key generation
        // during schema export.
        public override bool GenerateTablePrimaryKeyConstraintForIdentityColumn => false;
        
        public override string IdentityColumnString => "NOT NULL";

        public override string GetTableComment(string comment) => $" {comment}";
    }

    internal sealed class DdlBatchConnection : SpannerRetriableConnection
    {
        public DdlBatchConnection(SpannerRetriableConnection connection) : base(connection)
        {
        }

        protected override DbCommand CreateDbCommand() => new DdlBatchCommand(this, new SpannerCommand());
    }

    internal sealed class DdlBatchCommand : SpannerRetriableCommand
    {
        private readonly LinkedList<string> _statements = new LinkedList<string>();

        public DdlBatchCommand(SpannerRetriableConnection connection, SpannerCommand spannerCommand) : base(spannerCommand)
        {
            DbConnection = GaxPreconditions.CheckNotNull(connection, nameof(connection));
        }

        public override int ExecuteNonQuery()
        {
            _statements.AddLast(CommandText);
            return 0;
        }

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            _statements.AddLast(CommandText);
            return Task.FromResult(0);
        }

        protected override void Dispose(bool disposing)
        {
            if (_statements.Count > 0)
            {
                var spannerConnection = (SpannerRetriableConnection)DbConnection;
                var cmd = spannerConnection.CreateDdlCommand(_statements.First!.Value, _statements.Skip(1).ToArray());
                cmd.ExecuteNonQuery();
            }
            base.Dispose(disposing);
        }
    }
    
    /// <summary>
    /// !!! ONLY INTENDED FOR INTERNAL USAGE !!!
    /// 
    /// Connection provider that will group all DDL statements together and execute these as one batch when the command
    /// is disposed.  
    /// </summary>
    public sealed class DdlBatchConnectionProvider : IConnectionProvider
    {
        private IConnectionProvider _provider;

        public void Dispose() => _provider?.Dispose();

        public Task<DbConnection> GetConnectionAsync(CancellationToken cancellationToken) =>
            Task.FromResult(GetConnection());

        public void Configure(IDictionary<string, string> settings)
        {
            var newSettings = new Dictionary<string, string>(settings);
            if (settings.TryGetValue($"wrapped.{Environment.ConnectionProvider}", out var providerClass))
            {
                newSettings[Environment.ConnectionProvider] = providerClass;
            }
            else
            {
                newSettings.Remove(Environment.ConnectionProvider);
            }
            _provider = ConnectionProviderFactory.NewConnectionProvider(newSettings);
            _provider.Configure(newSettings);
        }

        public void CloseConnection(DbConnection conn) => _provider?.CloseConnection(conn);

        public DbConnection GetConnection()
        {
            var connection = _provider?.GetConnection();
            if (connection is DdlBatchConnection ddlBatchConnection)
            {
                return ddlBatchConnection;
            }
            if (connection is SpannerRetriableConnection spannerRetriableConnection)
            {
                return new DdlBatchConnection(spannerRetriableConnection);
            }
            return connection;
        }

        public IDriver Driver => _provider?.Driver;
    }
}