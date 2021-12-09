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
using Google.Cloud.Spanner.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.Connection
{

	internal sealed class SchemaProvider
	{
		private static readonly List<string> ReservedWords = new List<string>
		{
			"SELECT",
		};
		private static readonly List<string> DataTypes =
			Enum.GetValues(typeof(TypeCode)).Cast<TypeCode>().Select(t => t.ToString()).ToList();
		private readonly SpannerRetriableConnection _connection;
		private readonly Dictionary<string, Action<DataTable>> _schemaCollections;
		
		public SchemaProvider(SpannerRetriableConnection connection)
		{
			_connection = connection;
			_schemaCollections = new Dictionary<string, Action<DataTable>>(StringComparer.OrdinalIgnoreCase)
			{
				{ DbMetaDataCollectionNames.MetaDataCollections, FillMetadataCollections },
				{ DbMetaDataCollectionNames.ReservedWords, FillReservedWords },
				{ DbMetaDataCollectionNames.DataTypes, FillDataTypes },
				{ "Columns", FillColumns },
				{ "ColumnOptions", FillColumnOptions },
				{ "Indexes", FillIndexes },
				{ "IndexColumns", FillIndexColumns },
				{ "KeyColumnUsage", FillKeyColumnUsage },
				{ "Tables", FillTables },
				{ "ReferentialConstraints", FillReferentialConstraints },
				{ "TableConstraints", FillTableConstraints },
				{ "Views", FillViews },
			};
		}

		public DataTable GetSchema() => GetSchema(DbMetaDataCollectionNames.MetaDataCollections);

		public DataTable GetSchema(string collectionName)
		{
			GaxPreconditions.CheckNotNull(collectionName, nameof(collectionName));
			GaxPreconditions.CheckArgument(_schemaCollections.ContainsKey(collectionName), nameof(collectionName), $"Unknown collection name: {collectionName}");

			var dataTable = new DataTable(collectionName);
			_schemaCollections[collectionName](dataTable);
			return dataTable;
		}

		private void FillMetadataCollections(DataTable dataTable)
		{
			dataTable.Columns.AddRange(new [] {
				new DataColumn(DbMetaDataColumnNames.CollectionName, typeof(string)),
				new DataColumn(DbMetaDataColumnNames.NumberOfRestrictions, typeof(int)),
				new DataColumn(DbMetaDataColumnNames.NumberOfIdentifierParts, typeof(int)),
			});
			_ = _schemaCollections.Select(entry => dataTable.Rows.Add(entry.Key, 0, 0));
		}

		private void FillReservedWords(DataTable dataTable)
		{
			dataTable.Columns.AddRange(new [] {
				new DataColumn(DbMetaDataColumnNames.ReservedWord, typeof(string)),
			});
			_ = ReservedWords.Select(w => dataTable.Rows.Add(w));
		}

		private void FillDataTypes(DataTable dataTable)
		{
			dataTable.Columns.AddRange(new [] {
				new DataColumn(DbMetaDataColumnNames.DataType, typeof(string)),
			});
			_ = DataTypes.Select(w => dataTable.Rows.Add(w));
		}

		private void FillColumns(DataTable dataTable)
		{
			dataTable.Columns.AddRange(new []
			{
				new DataColumn("TABLE_CATALOG", typeof(string)),
				new DataColumn("TABLE_SCHEMA", typeof(string)),
				new DataColumn("TABLE_NAME", typeof(string)),
				new DataColumn("COLUMN_NAME", typeof(string)),
				new DataColumn("ORDINAL_POSITION", typeof(long)),
				new DataColumn("COLUMN_DEFAULT", typeof(string)),
				new DataColumn("DATA_TYPE", typeof(string)),
				new DataColumn("IS_NULLABLE", typeof(string)),
				new DataColumn("SPANNER_TYPE", typeof(string)),
				new DataColumn("IS_GENERATED", typeof(string)),
				new DataColumn("GENERATION_EXPRESSION", typeof(string)),
				new DataColumn("IS_STORED", typeof(string)),
				new DataColumn("SPANNER_STATE", typeof(string)),
			});

			FillDataTable(dataTable, "COLUMNS");
		}

		private void FillColumnOptions(DataTable dataTable)
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

			FillDataTable(dataTable, "COLUMN_OPTIONS");
		}

		private void FillKeyColumnUsage(DataTable dataTable)
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

			FillDataTable(dataTable, "KEY_COLUMN_USAGE");
		}


		private void FillReferentialConstraints(DataTable dataTable)
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

			FillDataTable(dataTable, "REFERENTIAL_CONSTRAINTS");
		}

		private void FillIndexes(DataTable dataTable)
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

			FillDataTable(dataTable, "INDEXES");
		}

		private void FillIndexColumns(DataTable dataTable)
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

			FillDataTable(dataTable, "INDEX_COLUMNS");
		}

		private void FillTables(DataTable dataTable)
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

			FillDataTable(dataTable, "TABLES");
		}

		private void FillTableConstraints(DataTable dataTable)
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

			FillDataTable(dataTable, "TABLE_CONSTRAINTS");
		}

		private void FillViews(DataTable dataTable)
		{
			dataTable.Columns.AddRange(new []
			{
				new DataColumn("TABLE_CATALOG", typeof(string)),
				new DataColumn("TABLE_SCHEMA", typeof(string)),
				new DataColumn("TABLE_NAME", typeof(string)),
				new DataColumn("VIEW_DEFINITION", typeof(string)),
			});

			FillDataTable(dataTable, "VIEWS");
		}

		private void FillDataTable(DataTable dataTable, string tableName)
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
	}
}