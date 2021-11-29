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

using NHibernate.Cache;
using NHibernate.Engine;
using NHibernate.Mapping;
using NHibernate.Persister.Entity;
using NHibernate.SqlCommand;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate
{
	/// <summary>
	/// EntityPersister implementation for Cloud Spanner for entities that use a single
	/// table. This persister can generate commands that use mutations instead of DML
	/// when that is appropriate or requested by the client application.
	/// </summary>
    public class SpannerMutationsEntityPersister : SingleTableEntityPersister
    {
	    private readonly bool[][] _propertyColumnInsertable;
	    private readonly bool _discriminatorInsertable;
	    
        public SpannerMutationsEntityPersister(PersistentClass persistentClass, ICacheConcurrencyStrategy cache, ISessionFactoryImplementor factory, IMapping mapping) : base(persistentClass, cache, factory, mapping)
        {
	        var hydrateSpan = EntityMetamodel.PropertySpan;
	        _propertyColumnInsertable = new bool[hydrateSpan][];
	        var i = 0;
	        foreach (var prop in persistentClass.PropertyClosureIterator)
	        {
		        _propertyColumnInsertable[i] = prop.Value.ColumnInsertability;
		        i++;
	        }

	        if (persistentClass.IsPolymorphic)
	        {
		        if (persistentClass.IsDiscriminatorValueNull)
		        {
			        _discriminatorInsertable = false;
		        }
		        else if (persistentClass.IsDiscriminatorValueNotNull)
		        {
			        _discriminatorInsertable = false;
		        }
		        else
		        {
			        var discriminatorValue = persistentClass.Discriminator;
			        _discriminatorInsertable = persistentClass.IsDiscriminatorInsertable && !discriminatorValue.HasFormula;
		        }
	        }
	        else
	        {
		        _discriminatorInsertable = false;
	        }
        }

        protected override SqlCommandInfo GenerateInsertString(bool identityInsert, bool[] includeProperty, int j)
        {
	        var sql = base.GenerateInsertString(identityInsert, includeProperty, j);
	        return AddInsertColumnInformation(sql, includeProperty, j);
        }

        protected override void UpdateOrInsert(object id, object[] fields, object[] oldFields, object rowId,
	        bool[] includeProperty, int j, object oldVersion, object obj, SqlCommandInfo sql,
	        ISessionImplementor session)
        {
	        sql = AddUpdateColumnInformation(sql, GetTableName(j));
	        base.UpdateOrInsert(id, fields, oldFields, rowId, includeProperty, j, oldVersion, obj, sql, session);
        }

        protected override Task UpdateOrInsertAsync(object id, object[] fields, object[] oldFields, object rowId,
	        bool[] includeProperty, int j, object oldVersion, object obj, SqlCommandInfo sql,
	        ISessionImplementor session, CancellationToken cancellationToken)
        {
	        sql = AddUpdateColumnInformation(sql, GetTableName(j));
	        return base.UpdateOrInsertAsync(id, fields, oldFields, rowId, includeProperty, j, oldVersion, obj, sql,
		        session, cancellationToken);
        }

        private void AddInsertColumns(List<string> columns, string[] columnNames, bool[] insertable)
        {
	        for (int i = 0; i < columnNames.Length; i++)
	        {
		        if (insertable == null || insertable[i])
		        {
			        columns.Add(columnNames[i]);
		        }
	        }
        }
        
        private SqlCommandInfo AddInsertColumnInformation(SqlCommandInfo sql, bool[] includeProperty, int j)
        {
	        var discriminatorColumndIndex = -1;
	        var columns = new List<string>();
	        // add normal properties
	        for (var i = 0; i < EntityMetamodel.PropertySpan; i++)
	        {
		        if (includeProperty[i] && IsPropertyOfTable(i, j))
		        {
			        AddInsertColumns(columns, GetPropertyColumnNames(i), _propertyColumnInsertable[i]);
		        }
	        }
	        // add the discriminator
	        if (j == 0 && _discriminatorInsertable)
	        {
		        discriminatorColumndIndex = columns.Count;
		        columns.Add(DiscriminatorColumnName);
	        }
	        // add the primary key
	        foreach (var col in GetKeyColumns(j))
	        {
		        columns.Add(col);
	        }
	        
	        var sqlString = new SpannerMutationSqlString(sql.Text, "INSERT", GetTableName(j), columns.ToArray(), discriminatorColumndIndex);
	        return new SqlCommandInfo(sqlString, sql.ParameterTypes);
        }

        private SqlCommandInfo AddUpdateColumnInformation(SqlCommandInfo sql, string table)
        {
	        var columns = new List<string>();
	        var whereClause = new SqlStringBuilder();
	        var whereColumns = new List<string>();
	        var inWhereClause = false;
	        foreach (var part in sql.Text)
	        {
		        if (part == null)
		        {
			        continue;
		        }
		        var content = part.ToString()!.Trim();
		        if (content.StartsWith("WHERE", StringComparison.InvariantCultureIgnoreCase))
		        {
			        inWhereClause = true;
		        }
		        if (inWhereClause)
		        {
			        if (content.EndsWith('='))
			        {
				        var column = ExtractColumnName(content, table);
				        columns.Add(column);
				        whereColumns.Add(column);
			        }
			        whereClause.Add(part.ToString());
		        }
		        else
		        {
			        if (content.EndsWith('='))
			        {
				        columns.Add(ExtractColumnName(content, table));
			        }
		        }
	        }
	        var sqlString = new SpannerMutationSqlString(sql.Text, "UPDATE", table, columns.ToArray(), whereColumns.ToArray(), 0);
	        return new SqlCommandInfo(sqlString, sql.ParameterTypes);
        }

        /// <summary>
        /// Removes ', ' from the start and ' = ' from the end of the string.
        /// </summary>
        /// <param name="part">The sql part to trim</param>
        /// <param name="table">The table that is being updated</param>
        /// <returns>The trimmed sql part</returns>
        private static string ExtractColumnName(string part, string table)
        {
	        var column = part;
	        int index;
	        var updatePrefix = $"UPDATE {table} SET ";
	        var wherePrefix = "WHERE ";
	        if (part.StartsWith(',') && part.EndsWith('='))
	        {
		        column = part.Substring(1, part.Length-2);
	        }
	        else if (part.StartsWith(wherePrefix) && part.EndsWith('='))
	        {
		        column = part.Substring(wherePrefix.Length, part.Length - wherePrefix.Length - 1);
	        }
	        else if (part.EndsWith('=') && (index = part.LastIndexOf(updatePrefix, StringComparison.InvariantCultureIgnoreCase)) > -1)
	        {
		        column = part.Substring(index + updatePrefix.Length, part.Length - updatePrefix.Length - index - 1);
	        }
	        return column.Trim();
        }
    }
}