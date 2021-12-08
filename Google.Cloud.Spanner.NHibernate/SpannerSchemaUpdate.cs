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

using Google.Cloud.Spanner.NHibernate.Internal;
using NHibernate.Cfg;
using NHibernate.Mapping;
using NHibernate.Tool.hbm2ddl;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Environment = NHibernate.Cfg.Environment;

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// Schema udpate implementation specifically for Cloud Spanner. Use this instead of the normal
    /// <see cref="SchemaUpdate"/> when working with Cloud Spanner.
    ///
    /// This schema update will ensure that:
    /// 1. The generated DDL is compatible with Cloud Spanner.
    /// 2. DDL scripts are executed as one batch instead of as individual statements. This significantly improves the
    ///    execution speed of large DDL batches.
    /// </summary>
    public class SpannerSchemaUpdate : SchemaUpdate
    {
        private readonly Dictionary<Table, IKeyValue> _primaryKeysGenerators = new Dictionary<Table, IKeyValue>();
        private readonly Dictionary<Table, string> _tableComments = new Dictionary<Table, string>();

        private readonly Configuration _configuration;
        
        /// <summary>
        /// Creates a <see cref="SchemaUpdate"/> that works with a Cloud Spanner database.
        /// </summary>
        /// <param name="cfg">The NHibernate configuration to use for the schema update</param>
        public SpannerSchemaUpdate(Configuration cfg) : this(cfg, cfg.Properties)
        {
        }

        /// <summary>
        /// Creates a <see cref="SchemaUpdate"/> that works with a Cloud Spanner database.
        /// </summary>
        /// <param name="cfg">The NHibernate configuration to use for the schema update</param>
        /// <param name="configProperties">Any additional configuration properties to use</param>
        public SpannerSchemaUpdate(Configuration cfg, IDictionary<string, string> configProperties)
            : base(cfg, SpannerSchemaExport.ReplaceDialectAndConnectionProvider(configProperties))
        {
            _configuration = cfg;
        }

        /// <inheritdoc cref="SchemaUpdate.Execute(bool,bool)"/>
        public new void Execute(bool useStdOut, bool doUpdate) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Execute(useStdOut, doUpdate));

        /// <inheritdoc cref="SchemaUpdate.Execute(Action&lt;string&gt;,bool)"/>
        public new void Execute(Action<string> scriptAction, bool doUpdate) =>
            ExecuteWithPrimaryKeysAsComment(() => base.Execute(scriptAction, doUpdate));

        /// <inheritdoc cref="SchemaUpdate.ExecuteAsync(bool,bool,CancellationToken)"/>
        public new async Task ExecuteAsync(bool useStdOut, bool doUpdate,
            CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.ExecuteAsync(useStdOut, doUpdate, cancellationToken));
        
        /// <inheritdoc cref="SchemaUpdate.ExecuteAsync(Action&lt;string&gt;,bool,CancellationToken)"/>
        public new async Task ExecuteAsync(Action<string> scriptAction, bool doUpdate,
            CancellationToken cancellationToken = default) =>
            await ExecuteWithPrimaryKeysAsCommentAsync(() => base.ExecuteAsync(scriptAction, doUpdate, cancellationToken));
        
        private void ExecuteWithPrimaryKeysAsComment(Action action)
        {
            try
            {
                SpannerSchemaExport.MovePrimaryKeysToComment(_configuration, _tableComments, _primaryKeysGenerators);
                action.Invoke();
            }
            finally
            {
                SpannerSchemaExport.ResetPrimaryKeys(_configuration, _tableComments, _primaryKeysGenerators);
            }
        }
        
        private async Task ExecuteWithPrimaryKeysAsCommentAsync(Func<Task> action)
        {
            try
            {
                SpannerSchemaExport.MovePrimaryKeysToComment(_configuration, _tableComments, _primaryKeysGenerators);
                await action.Invoke();
            }
            finally
            {
                SpannerSchemaExport.ResetPrimaryKeys(_configuration, _tableComments, _primaryKeysGenerators);
            }
        }
    }
}