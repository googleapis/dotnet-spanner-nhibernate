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
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.Connection
{
	internal sealed class SchemaProvider
	{
		private static readonly List<string> DataTypes =
			Enum.GetValues(typeof(TypeCode)).Cast<TypeCode>().Select(t => t.ToString()).ToList();
		private readonly SpannerRetriableConnection _connection;
		private readonly Dictionary<string, Tuple<Action<DataTable, string[]>, int>> _schemaCollections;
		private static readonly string[] TableRestrictions = { "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE" };
		private static readonly string[] ColumnRestrictions = { "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME" };
		private static readonly string[] ViewRestrictions = { "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME" };
		private static readonly string[] IndexRestrictions = { "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME" };
		private static readonly string[] IndexColumnRestrictions = { "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME", "COLUMN_NAME" };
		
		public SchemaProvider(SpannerRetriableConnection connection)
		{
			_connection = connection;
			_schemaCollections = new Dictionary<string, Tuple<Action<DataTable, string[]>, int>>(StringComparer.OrdinalIgnoreCase)
			{
				{ DbMetaDataCollectionNames.MetaDataCollections, new Tuple<Action<DataTable, string[]>, int>(FillMetadataCollections, 0) },
				{ DbMetaDataCollectionNames.ReservedWords, new Tuple<Action<DataTable, string[]>, int>(FillReservedWords, 0) },
				{ DbMetaDataCollectionNames.DataTypes, new Tuple<Action<DataTable, string[]>, int>(FillDataTypes, 0) },
				{ DbMetaDataCollectionNames.Restrictions, new Tuple<Action<DataTable, string[]>, int>(FillRestrictions, 0) },
				{ "Columns", new Tuple<Action<DataTable, string[]>, int>(FillColumns, 4) },
				{ "ColumnOptions", new Tuple<Action<DataTable, string[]>, int>(FillColumnOptions, 0) },
				{ "Indexes", new Tuple<Action<DataTable, string[]>, int>(FillIndexes, 4) },
				{ "IndexColumns", new Tuple<Action<DataTable, string[]>, int>(FillIndexColumns, 5) },
				{ "KeyColumnUsage", new Tuple<Action<DataTable, string[]>, int>(FillKeyColumnUsage, 0) },
				{ "Tables", new Tuple<Action<DataTable, string[]>, int>(FillTables, 4) },
				{ "ReferentialConstraints", new Tuple<Action<DataTable, string[]>, int>(FillReferentialConstraints, 0) },
				{ "TableConstraints", new Tuple<Action<DataTable, string[]>, int>(FillTableConstraints, 0) },
				{ "Views", new Tuple<Action<DataTable, string[]>, int>(FillViews, 3) },
			};
		}

		public DataTable GetSchema() => GetSchema(DbMetaDataCollectionNames.MetaDataCollections);

		public DataTable GetSchema(string collectionName, string[] restrictionValues = null)
		{
			GaxPreconditions.CheckNotNull(collectionName, nameof(collectionName));
			GaxPreconditions.CheckArgument(_schemaCollections.ContainsKey(collectionName), nameof(collectionName), $"Unknown collection name: {collectionName}");

			var dataTable = new DataTable(collectionName);
			_schemaCollections[collectionName].Item1(dataTable, restrictionValues);
			return dataTable;
		}

		private void FillMetadataCollections(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new [] {
				new DataColumn(DbMetaDataColumnNames.CollectionName, typeof(string)),
				new DataColumn(DbMetaDataColumnNames.NumberOfRestrictions, typeof(int)),
				new DataColumn(DbMetaDataColumnNames.NumberOfIdentifierParts, typeof(int)),
			});
			_ = _schemaCollections.Select(entry => dataTable.Rows.Add(entry.Key, 0, 0));
		}

		private void FillReservedWords(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new [] {
				new DataColumn(DbMetaDataColumnNames.ReservedWord, typeof(string)),
			});
			_ = Keywords.ReservedKeywords.Select(w => dataTable.Rows.Add(w));
		}

		private void FillDataTypes(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new [] {
				new DataColumn(DbMetaDataColumnNames.DataType, typeof(string)),
			});
			_ = DataTypes.Select(w => dataTable.Rows.Add(w));
		}

		private void FillRestrictions(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new []
			{
				new DataColumn("CollectionName", typeof(string)),
				new DataColumn("RestrictionName", typeof(string)),
				new DataColumn("RestrictionDefault", typeof(string)),
				new DataColumn("RestrictionNumber", typeof(int)),
			});
			dataTable.Rows.Add("Tables", "Catalog", "TABLE_CATALOG", 1);
			dataTable.Rows.Add("Tables", "Schema", "TABLE_SCHEMA", 2);
			dataTable.Rows.Add("Tables", "Table", "TABLE_NAME", 3);
			dataTable.Rows.Add("Tables", "TableType", "TABLE_TYPE", 4);
			dataTable.Rows.Add("Columns", "Catalog", "TABLE_CATALOG", 1);
			dataTable.Rows.Add("Columns", "Schema", "TABLE_SCHEMA", 2);
			dataTable.Rows.Add("Columns", "TableName", "TABLE_NAME", 3);
			dataTable.Rows.Add("Columns", "Column", "COLUMN_NAME", 4);
			dataTable.Rows.Add("Views", "Catalog", "TABLE_CATALOG", 1);
			dataTable.Rows.Add("Views", "Schema", "TABLE_SCHEMA", 2);
			dataTable.Rows.Add("Views", "Table", "TABLE_NAME", 3);
			dataTable.Rows.Add("Indexes", "Catalog", "TABLE_CATALOG", 1);
			dataTable.Rows.Add("Indexes", "Schema", "TABLE_SCHEMA", 2);
			dataTable.Rows.Add("Indexes", "Table", "TABLE_NAME", 3);
			dataTable.Rows.Add("Indexes", "Index", "INDEX_NAME", 4);
			dataTable.Rows.Add("IndexColumns", "Catalog", "TABLE_CATALOG", 1);
			dataTable.Rows.Add("IndexColumns", "Schema", "TABLE_SCHEMA", 2);
			dataTable.Rows.Add("IndexColumns", "Table", "TABLE_NAME", 3);
			dataTable.Rows.Add("IndexColumns", "Index", "INDEX_NAME", 4);
			dataTable.Rows.Add("IndexColumns", "Column", "COLUMN_NAME", 5);
		}

		private void FillColumns(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new []
			{
				new DataColumn("TABLE_CATALOG", typeof(string)),
				new DataColumn("TABLE_SCHEMA", typeof(string)),
				new DataColumn("TABLE_NAME", typeof(string)),
				new DataColumn("ORDINAL_POSITION", typeof(long)),
				new DataColumn("COLUMN_NAME", typeof(string)),
				new DataColumn("COLUMN_DEFAULT", typeof(string)),
				new DataColumn("DATA_TYPE", typeof(string)),
				new DataColumn("IS_NULLABLE", typeof(string)),
				new DataColumn("SPANNER_TYPE", typeof(string)),
				new DataColumn("IS_GENERATED", typeof(string)),
				new DataColumn("GENERATION_EXPRESSION", typeof(string)),
				new DataColumn("IS_STORED", typeof(string)),
				new DataColumn("SPANNER_STATE", typeof(string)),
			});

			FillDataTable(dataTable, "COLUMNS", ColumnRestrictions, restrictionValues);
		}

		private void FillColumnOptions(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new []
			{
				new DataColumn("TABLE_CATALOG", typeof(string)),
				new DataColumn("TABLE_SCHEMA", typeof(string)),
				new DataColumn("TABLE_NAME", typeof(string)),
				new DataColumn("COLUMN_NAME", typeof(string)),
				new DataColumn("OPTION_NAME", typeof(string)),
				new DataColumn("OPTION_TYPE", typeof(string)),
				new DataColumn("OPTION_VALUE", typeof(string)),
			});

			FillDataTable(dataTable, "COLUMN_OPTIONS", null, restrictionValues);
		}

		private void FillKeyColumnUsage(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new []
			{
				new DataColumn("CONSTRAINT_CATALOG", typeof(string)),
				new DataColumn("CONSTRAINT_SCHEMA", typeof(string)),
				new DataColumn("CONSTRAINT_NAME", typeof(string)),
				new DataColumn("TABLE_CATALOG", typeof(string)),
				new DataColumn("TABLE_SCHEMA", typeof(string)),
				new DataColumn("TABLE_NAME", typeof(string)),
				new DataColumn("COLUMN_NAME", typeof(string)),
				new DataColumn("ORDINAL_POSITION", typeof(long)),
				new DataColumn("POSITION_IN_UNIQUE_CONSTRAINT", typeof(string)),
			});

			FillDataTable(dataTable, "KEY_COLUMN_USAGE", null, restrictionValues);
		}


		private void FillReferentialConstraints(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new []
			{
				new DataColumn("CONSTRAINT_CATALOG", typeof(string)),
				new DataColumn("CONSTRAINT_SCHEMA", typeof(string)),
				new DataColumn("CONSTRAINT_NAME", typeof(string)),
				new DataColumn("UNIQUE_CONSTRAINT_CATALOG", typeof(string)),
				new DataColumn("UNIQUE_CONSTRAINT_SCHEMA", typeof(string)),
				new DataColumn("UNIQUE_CONSTRAINT_NAME", typeof(string)),
				new DataColumn("MATCH_OPTION", typeof(string)),
				new DataColumn("UPDATE_RULE", typeof(string)),
				new DataColumn("DELETE_RULE", typeof(string)),
				new DataColumn("SPANNER_STATE", typeof(string)),
			});

			FillDataTable(dataTable, "REFERENTIAL_CONSTRAINTS", null, restrictionValues);
		}

		private void FillIndexes(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new []
			{
				new DataColumn("TABLE_CATALOG", typeof(string)),
				new DataColumn("TABLE_SCHEMA", typeof(string)),
				new DataColumn("TABLE_NAME", typeof(string)),
				new DataColumn("INDEX_NAME", typeof(string)),
				new DataColumn("INDEX_TYPE", typeof(string)),
				new DataColumn("PARENT_TABLE_NAME", typeof(string)),
				new DataColumn("IS_UNIQUE", typeof(bool)),
				new DataColumn("IS_NULL_FILTERED", typeof(bool)),
				new DataColumn("INDEX_STATE", typeof(string)),
				new DataColumn("SPANNER_IS_MANAGED", typeof(bool)),
			});

			FillDataTable(dataTable, "INDEXES", IndexRestrictions, restrictionValues);
		}

		private void FillIndexColumns(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new []
			{
				new DataColumn("TABLE_CATALOG", typeof(string)),
				new DataColumn("TABLE_SCHEMA", typeof(string)),
				new DataColumn("TABLE_NAME", typeof(string)),
				new DataColumn("INDEX_NAME", typeof(string)),
				new DataColumn("INDEX_TYPE", typeof(string)),
				new DataColumn("COLUMN_NAME", typeof(string)),
				new DataColumn("ORDINAL_POSITION", typeof(long)),
				new DataColumn("COLUMN_ORDERING", typeof(string)),
				new DataColumn("IS_NULLABLE", typeof(string)),
				new DataColumn("SPANNER_TYPE", typeof(string)),
			});

			FillDataTable(dataTable, "INDEX_COLUMNS", IndexColumnRestrictions, restrictionValues);
		}

		private void FillTables(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new []
			{
				new DataColumn("TABLE_CATALOG", typeof(string)),
				new DataColumn("TABLE_SCHEMA", typeof(string)),
				new DataColumn("TABLE_NAME", typeof(string)),
				new DataColumn("PARENT_TABLE_NAME", typeof(string)),
				new DataColumn("ON_DELETE_ACTION", typeof(string)),
				new DataColumn("SPANNER_STATE", typeof(string)),
			});

			FillDataTable(dataTable, "TABLES", TableRestrictions, restrictionValues);
		}

		private void FillTableConstraints(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new []
			{
				new DataColumn("CONSTRAINT_CATALOG", typeof(string)),
				new DataColumn("CONSTRAINT_SCHEMA", typeof(string)),
				new DataColumn("CONSTRAINT_NAME", typeof(string)),
				new DataColumn("TABLE_SCHEMA", typeof(string)),
				new DataColumn("TABLE_NAME", typeof(string)),
				new DataColumn("CONSTRAINT_TYPE", typeof(string)),
				new DataColumn("IS_DEFERRABLE", typeof(string)),
				new DataColumn("INITIALLY_DEFERRED", typeof(string)),
				new DataColumn("ENFORCED", typeof(string)),
			});

			FillDataTable(dataTable, "TABLE_CONSTRAINTS", null, restrictionValues);
		}

		private void FillViews(DataTable dataTable, string[] restrictionValues = null)
		{
			dataTable.Columns.AddRange(new []
			{
				new DataColumn("TABLE_CATALOG", typeof(string)),
				new DataColumn("TABLE_SCHEMA", typeof(string)),
				new DataColumn("TABLE_NAME", typeof(string)),
				new DataColumn("VIEW_DEFINITION", typeof(string)),
			});

			FillDataTable(dataTable, "VIEWS", ViewRestrictions, restrictionValues);
		}

		private void FillDataTable(DataTable dataTable, string tableName, string[] restrictionColumns, string[] restrictionValues)
		{
			Action close = null;
			if (_connection.State != ConnectionState.Open)
			{
				_connection.Open();
				close = _connection.Close;
			}

			using (var command = _connection.CreateCommand())
			{
				command.CommandText = $"SELECT {string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(x => x!.ColumnName.Replace("COLUMN_DEFAULT", "CAST(COLUMN_DEFAULT AS STRING) AS COLUMN_DEFAULT")))}\n"
				                      + $"FROM INFORMATION_SCHEMA.{tableName}\n"
				                      + BuildWhereClause(command, restrictionColumns, restrictionValues)
				                      + $"ORDER BY {string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(x => x!.ColumnName))}";
				using var reader = command.ExecuteReader();
				while (reader.Read())
				{
					var rowValues = new object[dataTable.Columns.Count];
					reader.GetValues(rowValues);
					dataTable.Rows.Add(rowValues);
				}
			}

			close?.Invoke();
		}

		private static string BuildWhereClause(DbCommand command, string[] restrictionColumns, string[] restrictionValues)
		{
			if (restrictionValues == null || restrictionColumns == null || restrictionValues.Length == 0)
			{
				return "";
			}
			GaxPreconditions.CheckArgument(restrictionColumns.Length >= restrictionValues.Length, nameof(restrictionValues), $"Unsupported number of restriction values supplied: {restrictionValues.Length}. Expected at most {restrictionColumns.Length} values.");
			var builder = new StringBuilder();
			var first = true;
			for (var i = 0; i < restrictionValues.Length; i++)
			{
				if (restrictionValues[i] != null)
				{
					builder.Append(first ? "WHERE " : "AND ");
					builder.Append(restrictionColumns[i]).Append("=@p").Append(i).Append("\n");
					first = false;
					
					var param = command.CreateParameter();
					param.ParameterName = $"@p{i}";
					param.Value = restrictionValues[i];
					command.Parameters.Add(param);
				}
			}
			return builder.ToString();
		}
	}
}