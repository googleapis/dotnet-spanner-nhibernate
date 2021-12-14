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

using NHibernate.Dialect;
using NHibernate.Engine;
using NHibernate.Mapping;
using System.Linq;

namespace Google.Cloud.Spanner.NHibernate.Internal
{
    internal class UniqueKeyAsAuxiliaryObject : AbstractAuxiliaryDatabaseObject
    {
        private static readonly Column PreventGenerationColumn = new Column("DoNotGenerate") { IsNullable = true };
        private readonly UniqueKey _uniqueKey;
        
        internal UniqueKeyAsAuxiliaryObject(UniqueKey uniqueKey)
        {
            _uniqueKey = uniqueKey;
            _uniqueKey.AddColumn(PreventGenerationColumn);
        }

        public override string SqlCreateString(Dialect dialect, IMapping p, string defaultCatalog, string defaultSchema)
        {
            var columns = _uniqueKey.Columns.Where(c => !Equals(c, PreventGenerationColumn)).ToList();
            return Index.BuildSqlCreateIndexString(dialect, _uniqueKey.Name, _uniqueKey.Table, columns, true,
                defaultCatalog, defaultSchema);
        }

        public override string SqlDropString(Dialect dialect, string defaultCatalog, string defaultSchema) =>
            $"DROP INDEX {_uniqueKey.Name}";
    }
}
