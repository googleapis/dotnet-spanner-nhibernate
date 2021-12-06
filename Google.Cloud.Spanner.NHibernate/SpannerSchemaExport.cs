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

using NHibernate.Cfg;
using NHibernate.Dialect;
using NHibernate.Id;
using NHibernate.Mapping;
using NHibernate.Tool.hbm2ddl;
using System.Collections.Generic;
using System.IO;

namespace Google.Cloud.Spanner.NHibernate
{
    internal class SpannerSchemaExportDialect : SpannerDialect
    {
        // Cloud Spanner does not support any identity columns, but this allows us to skip the primary key generation
        // during schema export.
        public override bool GenerateTablePrimaryKeyConstraintForIdentityColumn => false;
        
        public override string IdentityColumnString => "";

        public override string GetTableComment(string comment) => $" {comment}";
    }
    
    public class SpannerSchemaExport : SchemaExport
    {
        private readonly SimpleValue _disablePrimaryKeyGenerator = new SimpleValue { IdentifierGeneratorStrategy = typeof(IdentityGenerator).AssemblyQualifiedName };
        
        private readonly Dictionary<Table, IKeyValue> _primaryKeysGenerators = new Dictionary<Table, IKeyValue>();
        private readonly Dictionary<Table, string> _tableComments = new Dictionary<Table, string>();

        private readonly Configuration _configuration;

        private readonly Dialect _dialect = new SpannerSchemaExportDialect();

        public SpannerSchemaExport(Configuration cfg)
            : base(cfg, new Dictionary<string, string> {{Environment.Dialect, typeof(SpannerSchemaExportDialect).AssemblyQualifiedName}})
        {
            _configuration = cfg;
        }

        public SpannerSchemaExport(Configuration cfg, IDictionary<string, string> configProperties)
            : base(cfg, new Dictionary<string, string>(configProperties) {{Environment.Dialect, typeof(SpannerSchemaExportDialect).AssemblyQualifiedName}})
        {
            _configuration = cfg;
        }
        
        /// <inheritdoc cref="SchemaExport.Create(bool,bool)"/>>
        public new void Create(TextWriter exportOutput, bool execute)
        {
            MovePrimaryKeysToComment();
            Create(exportOutput, execute, null);
            ResetPrimaryKeys();
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
}