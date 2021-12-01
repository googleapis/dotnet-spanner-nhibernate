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

using NHibernate.Cache;
using NHibernate.Engine;
using NHibernate.Mapping;
using NHibernate.Persister.Entity;
using NHibernate.SqlCommand;
using NHibernate.Util;
using System.Linq;

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// Entity persister that can be used with Cloud Spanner tables that contain one or more columns that should be
    /// set to a default value when inserted/updated. The default value must be given as a string literal, and may be
    /// a server side function, such as PENDING_COMMIT_TIMESTAMP().
    ///
    /// The persister will automatically set the value of a column to the default value of that column for any column
    /// that fulfills the following criteria:
    /// 1. INSERT: Default value of the column is not null and the column is not insertable in the Hibernate config.
    /// 2. UPDATE: Default value of the column is not null and the column is not insertable in the Hibernate config.
    ///
    /// The following example mapping will for example cause the ColCommitTs column to be set to
    /// PENDING_COMMIT_TIMESTAMP() for both insert and update statements:
    ///
    /// <code>
    /// Persister<SpannerDefaultValueSingleTableEntityPersister>();
    /// Property(x => x.ColCommitTs, mapper =>
    ///     {
    ///         mapper.Insert(false);
    ///         mapper.Update(false);
    ///         mapper.Column(c =>
    ///         {
    ///             c.Default("PENDING_COMMIT_TIMESTAMP()");
    ///         });
    ///     }
    /// );
    /// </code>
    /// </summary>
    public class SpannerDefaultValueSingleTableEntityPersister : SingleTableEntityPersister
    {
        private readonly LinkedHashMap<string, string> _defaultInsertValues = new LinkedHashMap<string, string>();
        private readonly LinkedHashMap<string, string> _defaultUpdateValues = new LinkedHashMap<string, string>();
        
        public SpannerDefaultValueSingleTableEntityPersister(PersistentClass persistentClass, ICacheConcurrencyStrategy cache, ISessionFactoryImplementor factory, IMapping mapping) : base(persistentClass, cache, factory, mapping)
        {
            foreach (var prop in persistentClass.PropertyIterator)
            {
                foreach (var col in prop.ColumnIterator.OfType<Column>())
                {
                    if (col.DefaultValue != null)
                    {
                        if (!prop.IsInsertable)
                        {
                            _defaultInsertValues[col.Name] = col.DefaultValue;
                        }
                        if (!prop.IsUpdateable)
                        {
                            _defaultUpdateValues[col.Name] = col.DefaultValue;
                        }
                    }
                }
            }
        }
        
        protected override void AddDiscriminatorToInsert(SqlInsertBuilder insert)
        {
            base.AddDiscriminatorToInsert(insert);
            foreach (var col in _defaultInsertValues.Keys)
            {
                insert.AddColumn(col, _defaultInsertValues[col]);
            }
        }
        
        protected override SqlCommandInfo GenerateUpdateString(bool[] includeProperty, int j, bool useRowId) =>
            GenerateUpdateString(includeProperty, j, null, useRowId);
        
		/// <summary> Generate the SQL that updates a row by id (and version)</summary>
		private new SqlCommandInfo GenerateUpdateString(bool[] includeProperty, int j, object[] oldFields, bool useRowId) =>
            AppendUpdateString(base.GenerateUpdateString(includeProperty, j, oldFields, useRowId));

        private SqlCommandInfo AppendUpdateString(SqlCommandInfo info)
        {
            // This is safe because we know that the SQL string is a simple statement in the form:
            // UPDATE Foo SET Col1 = ?, Col2 = ?, ... WHERE ColId = ?
            var whereIndex = info.Text.LastIndexOfCaseInsensitive(" WHERE ");
            if (whereIndex > -1)
            {
                foreach (var col in _defaultUpdateValues.Keys)
                {
                    info.Text.Insert(whereIndex, $", {col} = {_defaultUpdateValues[col]}");
                }
            }
            return info;
        }
    }
}