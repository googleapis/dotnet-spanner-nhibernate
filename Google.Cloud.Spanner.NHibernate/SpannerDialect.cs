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

using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.NHibernate.Functions;
using NHibernate;
using NHibernate.Dialect;
using NHibernate.Dialect.Function;
using NHibernate.Dialect.Schema;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;
using System.Data;
using System.Data.Common;
using Environment = NHibernate.Cfg.Environment;

namespace Google.Cloud.Spanner.NHibernate
{
    public class SpannerDialect : Dialect
    {
        public SpannerDialect()
        {
			DefaultProperties[Environment.ConnectionDriver] = typeof(SpannerDriver).AssemblyQualifiedName;

			RegisterDateTimeTypeMappings();
			RegisterColumnType(DbType.AnsiStringFixedLength, "STRING(MAX)");
			RegisterColumnType(DbType.AnsiStringFixedLength, 2621440, "STRING($l)");
			RegisterColumnType(DbType.AnsiString, "STRING(MAX)");
			RegisterColumnType(DbType.AnsiString, 2621440, "STRING($l)");
			RegisterColumnType(DbType.Binary, "BYTES(MAX)");
			RegisterColumnType(DbType.Binary, 10485760, "BYTES($l)");
			RegisterColumnType(DbType.Boolean, "BOOL");
			RegisterColumnType(DbType.Byte, "INT64");
			RegisterColumnType(DbType.Currency, "NUMERIC");
			RegisterColumnType(DbType.Decimal, "NUMERIC");
			RegisterColumnType(DbType.VarNumeric, "NUMERIC");
			RegisterColumnType(DbType.Double, "FLOAT64");
			RegisterColumnType(DbType.Int16, "INT64");
			RegisterColumnType(DbType.Int32, "INT64");
			RegisterColumnType(DbType.Int64, "INT64");
			RegisterColumnType(DbType.Single, "FLOAT64");
			RegisterColumnType(DbType.StringFixedLength, "STRING(MAX)");
			RegisterColumnType(DbType.StringFixedLength, 2621440, "STRING($l)");
			RegisterColumnType(DbType.String, "STRING(MAX)");
			RegisterColumnType(DbType.String, 2621440, "STRING($l)");

			// Override standard HQL function
			RegisterFunction("current_timestamp", new NoArgSQLFunction("CURRENT_TIMESTAMP", NHibernateUtil.LocalDateTime, true));
			RegisterFunction("str", new SQLFunctionTemplate(NHibernateUtil.String, "cast(?1 as STRING)"));
			RegisterFunction("locate", new SQLFunctionTemplate(NHibernateUtil.String, "STRPOS(?2, ?1)"));
			RegisterFunction("substring", new StandardSQLFunction("SUBSTR", NHibernateUtil.String));
			RegisterFunction("trim", new AnsiTrimEmulationFunction());
			RegisterFunction("extract", new SpannerExtractFunction());
			RegisterFunction("second", new SQLFunctionTemplate(NHibernateUtil.Int32, "extract(second from ?1 AT TIME ZONE 'UTC')"));
			RegisterFunction("minute", new SQLFunctionTemplate(NHibernateUtil.Int32, "extract(minute from ?1 AT TIME ZONE 'UTC')"));
			RegisterFunction("hour", new SQLFunctionTemplate(NHibernateUtil.Int32, "extract(hour from ?1 AT TIME ZONE 'UTC')"));
			RegisterFunction("day", new SQLFunctionTemplate(NHibernateUtil.Int32, "extract(day from ?1 AT TIME ZONE 'UTC')"));
			RegisterFunction("dayofyear", new SQLFunctionTemplate(NHibernateUtil.Int32, "extract(dayofyear from ?1 AT TIME ZONE 'UTC')"));
			RegisterFunction("dayofweek", new SQLFunctionTemplate(NHibernateUtil.Int32, "extract(dayofweek from ?1 AT TIME ZONE 'UTC')"));
			RegisterFunction("month", new SQLFunctionTemplate(NHibernateUtil.Int32, "extract(month from ?1 AT TIME ZONE 'UTC')"));
			RegisterFunction("year", new SQLFunctionTemplate(NHibernateUtil.Int32, "extract(year from ?1 AT TIME ZONE 'UTC')"));
			
			RegisterKeywords();
		}

		#region private static readonly string[] DialectKeywords = { ... }

		private static readonly string[] DialectKeywords =
		{
			"ASC"
		};

		#endregion

		protected virtual void RegisterDateTimeTypeMappings()
		{
			RegisterColumnType(DbType.Date, "DATE");
			RegisterColumnType(DbType.DateTime, "TIMESTAMP");
			RegisterColumnType(DbType.Time, "TIMESTAMP");
		}

		protected virtual void RegisterKeywords()
		{
			base.RegisterKeywords();
			RegisterKeywords(DialectKeywords);
		}

		private static string WithSize(SpannerDbType type)
		{
			if (type.Size.HasValue)
			{
				return type.ToString();
			}
			if (type.Equals(SpannerDbType.Bytes) || type.Equals(SpannerDbType.String))
			{
				return $"{type}(MAX)";
			}
			return type.ToString();
		}

		public override string GetTypeName(SqlType sqlType) =>
			GetTypeName(sqlType, 0, 0, 0);

		public override string GetTypeName(SqlType sqlType, int length, int precision, int scale)
		{
			if (sqlType is SpannerCommitTimestampSqlType)
			{
				return "TIMESTAMP OPTIONS (allow_commit_timestamp=true)";
			}
			if (sqlType is SpannerSqlType spannerSqlType)
			{
				if (spannerSqlType.ArrayElementType != null)
				{
					var elementType = length > 0
						? spannerSqlType.ArrayElementType.WithSize(length)
						: spannerSqlType.ArrayElementType;
					return $"ARRAY<{WithSize(elementType)}>";
				}
				var type = length > 0
					? spannerSqlType.SpannerDbType.WithSize(length)
					: spannerSqlType.SpannerDbType;
				return WithSize(type);
			}
			if (length > 0)
			{
				return base.GetTypeName(sqlType, length, precision, scale);
			}
			return base.GetTypeName(sqlType);
		}
		
		public override char OpenQuote => '`';

		public override char CloseQuote => '`';

		public override string AddColumnString => "ADD COLUMN";

		public override bool SupportsUnique => false;

		public override bool SupportsUniqueConstraintInCreateAlterTable => false;

		public override bool SupportsColumnCheck => false;
		
		public override bool SupportsLimit => true;

		public override bool SupportsLimitOffset => true;

		public override int MaxAliasLength => 128;

		public override SqlString GetLimitString(SqlString queryString, SqlString offset, SqlString limit)
		{
			SqlStringBuilder pagingBuilder = new SqlStringBuilder();
			pagingBuilder.Add(queryString);
			if (offset != null && limit == null)
			{
				// Cloud Spanner requires limit if offset is specified.
				// This creates a LIMIT clause contains a very large number
				// rows. The sum of LIMIT and OFFSET may not exceed the max
				// value for INT64.
				pagingBuilder.Add($" LIMIT {long.MaxValue / 2}");
			}

			if (limit != null)
			{
				pagingBuilder.Add(" LIMIT ");
				pagingBuilder.Add(limit);
			}

			if (offset != null)
			{
				pagingBuilder.Add(" OFFSET ");
				pagingBuilder.Add(offset);
			}

			return pagingBuilder.ToSqlString();
		}

		public override string GetAddForeignKeyConstraintString(string constraintName, string[] foreignKey,
			string referencedTable, string[] primaryKey, bool referencesPrimaryKey) =>
			// Simulate that we are never referencing the primary key of the referenced table.
			// This ensures that the referenced column names are included in the constraint definition.
			base.GetAddForeignKeyConstraintString(constraintName, foreignKey, referencedTable, primaryKey,
				false);

		public override bool SupportsUnionAll => true;

		public override string ToBooleanValueString(bool value)
			=> value ? "TRUE" : "FALSE";
		
		public override IDataBaseSchema GetDataBaseSchema(DbConnection connection) => new SpannerDataBaseMetadata(connection);

		public override bool SupportsCurrentTimestampSelection => true;

		public override string CurrentTimestampSelectString => "SELECT CURRENT_TIMESTAMP";

		#region Overridden informational metadata

		public override bool SupportsEmptyInList => false;

		public override bool UseInputStreamToInsertBlob => false;

		public override bool SupportsLobValueChangePropogation => false;

		public override bool SupportsUnboundedLobLocatorMaterialization => false;

		public override bool SupportsDistributedTransactions => false;

		#endregion
    }
}