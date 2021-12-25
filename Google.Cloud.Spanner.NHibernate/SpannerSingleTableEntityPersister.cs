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

using Google.Cloud.Spanner.Data;
using NHibernate.Cache;
using NHibernate.Engine;
using NHibernate.Mapping;
using NHibernate.Persister.Entity;
using NHibernate.SqlCommand;
using NHibernate.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.NHibernate
{
	/// <summary>
	/// EntityPersister implementation for Cloud Spanner for entities that use a single
	/// table. This persister can generate commands that use mutations instead of DML
	/// when that is appropriate or requested by the client application.
	///
	/// In addition, this persister can be used with tables that contain one or more columns that should be
	/// set to a fixed value when inserted/updated. The fixed value must be given as a string literal, and may be
	/// a server side function, such as PENDING_COMMIT_TIMESTAMP().
	///
	/// The persister will automatically set the value of a column to the fixed value of that column for any column
	/// that fulfills the following criteria:
	/// 1. INSERT: Default value of the column is not null and the column is not insertable in the Hibernate config.
	/// 2. UPDATE: Default value of the column is not null and the column is not insertable in the Hibernate config.
	///
	/// The following example mapping will for example cause the ColCommitTs column to be set to
	/// PENDING_COMMIT_TIMESTAMP() for both insert and update statements:
	///
	/// <code>
	/// Persister&lt;SpannerSingleTableWithFixedValuesEntityPersister&gt;();
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
    public class SpannerSingleTableEntityPersister : SingleTableEntityPersister
    {
	    // These maps contain the default values that should be set when a record is inserted
	    // or updated, and no value has been set by NHibernate.
	    private readonly LinkedHashMap<string, string> _defaultInsertValues = new LinkedHashMap<string, string>();
	    private readonly LinkedHashMap<string, string> _defaultUpdateValues = new LinkedHashMap<string, string>();
	    
	    private readonly bool[][] _propertyColumnInsertable;
	    private readonly bool _discriminatorInsertable;
	    
        public SpannerSingleTableEntityPersister(PersistentClass persistentClass, ICacheConcurrencyStrategy cache, ISessionFactoryImplementor factory, IMapping mapping) : base(persistentClass, cache, factory, mapping)
        {
	        var hydrateSpan = EntityMetamodel.PropertySpan;
	        _propertyColumnInsertable = new bool[hydrateSpan][];
	        var i = 0;
	        foreach (var prop in persistentClass.PropertyClosureIterator)
	        {
		        _propertyColumnInsertable[i] = prop.Value.ColumnInsertability;
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
        
        protected override void AddDiscriminatorToInsert(SqlInsertBuilder insert)
        {
	        base.AddDiscriminatorToInsert(insert);
	        foreach (var col in _defaultInsertValues.Keys)
	        {
		        insert.AddColumn(col, _defaultInsertValues[col]);
	        }
        }
        
        private SqlCommandInfo AppendUpdateString(SqlCommandInfo info)
        {
            if (_defaultUpdateValues.Count == 0)
            {
                return info;
            }
            if (!(info.Text is SpannerMutationSqlString spannerMutationSqlString))
            {
                return info;
            }
            // This is safe because we know that the SQL string is a simple statement in the form:
            // UPDATE Foo SET Col1 = ?, Col2 = ?, ... WHERE ColId = ?
            var whereIndex = spannerMutationSqlString.LastIndexOfCaseInsensitive(" WHERE ");
            if (whereIndex > -1)
            {
                SqlString dmlSqlString = spannerMutationSqlString;
                foreach (var col in _defaultUpdateValues.Keys)
                {
                    dmlSqlString = dmlSqlString.Insert(whereIndex, $", {col} = {_defaultUpdateValues[col]}");
                }
                spannerMutationSqlString = new SpannerMutationSqlString(dmlSqlString,
                    spannerMutationSqlString.Operation, spannerMutationSqlString.Table,
                    spannerMutationSqlString.Columns, spannerMutationSqlString.CheckVersionText,
                    spannerMutationSqlString.WhereColumns, spannerMutationSqlString.WhereParamsStartIndex,
                    spannerMutationSqlString.IsVersioned);
                foreach (var col in _defaultUpdateValues.Keys)
                {
                    var spannerParameter = CreateSpannerParameter(col, _defaultUpdateValues[col]);
                    spannerMutationSqlString.AdditionalParameters.Add(spannerParameter);
                }
            }
            return new SqlCommandInfo(spannerMutationSqlString, info.ParameterTypes);
        }

        private static SpannerParameter CreateSpannerParameter(string col, object value)
        {
            var spannerParameter = new SpannerParameter
            {
                ParameterName = col
            };
            if ("PENDING_COMMIT_TIMESTAMP()".Equals(value?.ToString() ?? "", StringComparison.OrdinalIgnoreCase))
            {
                spannerParameter.Value = SpannerParameter.CommitTimestamp;
                spannerParameter.SpannerDbType = SpannerDbType.Timestamp;
            }
            else
            {
                spannerParameter.Value = value;
            }
            return spannerParameter;
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
	        sql = AddUpdateColumnInformation(sql, j);
	        base.UpdateOrInsert(id, fields, oldFields, rowId, includeProperty, j, oldVersion, obj, sql, session);
        }

        protected override Task UpdateOrInsertAsync(object id, object[] fields, object[] oldFields, object rowId,
	        bool[] includeProperty, int j, object oldVersion, object obj, SqlCommandInfo sql,
	        ISessionImplementor session, CancellationToken cancellationToken)
        {
	        sql = AddUpdateColumnInformation(sql, j);
	        return base.UpdateOrInsertAsync(id, fields, oldFields, rowId, includeProperty, j, oldVersion, obj, sql,
		        session, cancellationToken);
        }

        protected override SqlCommandInfo GenerateDeleteString(int j)
        {
	        var sql = base.GenerateDeleteString(j);
	        return AddDeleteColumnInformation(sql, j);
        }
        
        protected virtual SqlCommandInfo AddInsertColumnInformation(SqlCommandInfo sql, bool[] includeProperty, int j)
        {
	        var columns = new List<string>();
	        // add normal properties
	        for (var i = 0; i < EntityMetamodel.PropertySpan; i++)
	        {
		        if (includeProperty[i] && IsPropertyOfTable(i, j))
		        {
			        AddInsertColumns(columns, GetPropertyColumnNames(i), _propertyColumnInsertable[i]);
		        }
	        }
	        // add the primary key
	        foreach (var col in GetKeyColumns(j))
	        {
		        columns.Add(col);
	        }
	        
	        var spannerMutationSqlString = new SpannerMutationSqlString(sql.Text, "INSERT", GetTableName(j), columns.ToArray());
	        // Add the discriminator.
	        if (j == 0 && _discriminatorInsertable)
	        {
		        var spannerParameter = CreateSpannerParameter(DiscriminatorColumnName, DiscriminatorValue);
		        spannerMutationSqlString.AdditionalParameters.Add(spannerParameter);
	        }
	        // Add default values.
	        foreach (var col in _defaultInsertValues.Keys)
	        {
		        var spannerParameter = CreateSpannerParameter(col, _defaultInsertValues[col]);
		        spannerMutationSqlString.AdditionalParameters.Add(spannerParameter);
	        }
	        return new SqlCommandInfo(spannerMutationSqlString, sql.ParameterTypes);
        }

        protected virtual SqlCommandInfo AddUpdateColumnInformation(SqlCommandInfo sql, int j)
        {
	        var table = GetTableName(j);
	        var columns = new List<string>();
	        var isVersioned = j == 0 && IsVersioned &&
	                          EntityMetamodel.OptimisticLockMode == Versioning.OptimisticLock.Version;
	        var checkVersionBuilder = new SqlStringBuilder().Add($"SELECT 1 AS C FROM {table}");
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
				        var column = ExtractColumnNameFromUpdateStatementPart(content, table);
				        if (!column.Equals(VersionColumnName, StringComparison.InvariantCultureIgnoreCase))
				        {
					        columns.Add(column);
				        }
				        whereColumns.Add(column);
			        }
			        if (content.Equals("?"))
			        {
				        checkVersionBuilder.AddParameter();
			        }
			        else
			        {
				        checkVersionBuilder.Add(part.ToString());
			        }
		        }
		        else
		        {
			        if (content.EndsWith('='))
			        {
				        columns.Add(ExtractColumnNameFromUpdateStatementPart(content, table));
			        }
		        }
	        }
	        // The columns that are added to the mutation command also include the key columns.
	        // The WHERE parameters therefore start at the number of columns minus the number of key columns.
	        var whereParamsStartIndex = columns.Count - GetKeyColumns(j).Length;
	        var sqlString = new SpannerMutationSqlString(sql.Text, "UPDATE", table, columns.ToArray(), checkVersionBuilder.ToSqlString(), whereColumns.ToArray(), whereParamsStartIndex, isVersioned);
	        return AppendUpdateString(new SqlCommandInfo(sqlString, sql.ParameterTypes));
        }
        
        private SqlCommandInfo AddDeleteColumnInformation(SqlCommandInfo sql, int j)
        {
	        var keyColumns = GetKeyColumns(j);
	        var columns = new List<string>();
	        var checkVersionBuilder = new SqlStringBuilder().Add($"SELECT 1 AS C FROM {GetTableName(j)}");
	        var whereColumns = new List<string>();
	        // add the primary key
	        var first = true;
	        foreach (var col in keyColumns)
	        {
		        columns.Add(col);
		        whereColumns.Add(col);
		        if (first)
		        {
			        checkVersionBuilder = checkVersionBuilder.Add(" WHERE ");
			        first = false;
		        }
		        else
		        {
			        checkVersionBuilder = checkVersionBuilder.Add(" AND ");
		        }
		        checkVersionBuilder.Add(col + " = ").AddParameter();
	        }
	        var isVersioned = j == 0 && IsVersioned &&
	                          EntityMetamodel.OptimisticLockMode == Versioning.OptimisticLock.Version;
	        if (isVersioned)
	        {
		        whereColumns.Add(VersionColumnName);
		        checkVersionBuilder.Add(" AND " + VersionColumnName + " = ").AddParameter();
	        }
	        var sqlString = new SpannerMutationSqlString(sql.Text, "DELETE", GetTableName(j), columns.ToArray(), checkVersionBuilder.ToSqlString(), whereColumns.ToArray(), 0, isVersioned);
	        return new SqlCommandInfo(sqlString, sql.ParameterTypes);
        }

        /// <summary>
        /// Adds the array of column names to the list of columns for each column that is insertable.
        /// </summary>
        /// <param name="columns">The list of columns to add the column names</param>
        /// <param name="columnNames">The column names to add</param>
        /// <param name="insertable">An array indicating which columns are insertable</param>
        private static void AddInsertColumns(List<string> columns, string[] columnNames, bool[] insertable)
        {
	        for (int i = 0; i < columnNames.Length; i++)
	        {
		        if (insertable == null || insertable[i])
		        {
			        columns.Add(columnNames[i]);
		        }
	        }
        }

        /// <summary>
        /// Removes ', ' from the start and ' = ' from the end of the string.
        /// </summary>
        /// <param name="part">The sql part to trim</param>
        /// <param name="table">The table that is being updated</param>
        /// <returns>The trimmed sql part</returns>
        private static string ExtractColumnNameFromUpdateStatementPart(string part, string table)
        {
	        var column = part;
	        int index;
	        var updatePrefix = $"UPDATE {table} SET ";
	        var wherePrefix = "WHERE ";
	        var andPrefix = "AND ";
	        if (part.StartsWith(',') && part.EndsWith('='))
	        {
		        column = part.Substring(1, part.Length-2);
	        }
	        else if (part.StartsWith(wherePrefix) && part.EndsWith('='))
	        {
		        column = part.Substring(wherePrefix.Length, part.Length - wherePrefix.Length - 1);
	        }
	        else if (part.StartsWith(andPrefix) && part.EndsWith('='))
	        {
		        column = part.Substring(andPrefix.Length, part.Length - andPrefix.Length - 1);
	        }
	        else if (part.EndsWith('=') && (index = part.LastIndexOf(updatePrefix, StringComparison.InvariantCultureIgnoreCase)) > -1)
	        {
		        column = part.Substring(index + updatePrefix.Length, part.Length - updatePrefix.Length - index - 1);
	        }
	        return column.Trim();
        }
    }
}