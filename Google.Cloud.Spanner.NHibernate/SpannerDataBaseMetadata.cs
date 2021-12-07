using NHibernate.Dialect;
using NHibernate.Dialect.Schema;
using System;
using System.Data;
using System.Data.Common;

namespace Google.Cloud.Spanner.NHibernate
{
    public class SpannerDataBaseMetadata : AbstractDataBaseSchema
    {
        public SpannerDataBaseMetadata(DbConnection connection) : base(connection)
        {
        }

        public override bool StoresLowerCaseIdentifiers => false;

        protected override string ForeignKeysSchemaName => "ReferentialConstraints";
        
        public override ITableMetadata GetTableMetadata(DataRow rs, bool extras) =>
	        new SpannerTableMetadata(rs, this, extras);
    }

	public class SpannerTableMetadata : AbstractTableMetadata
	{
		public SpannerTableMetadata(DataRow rs, IDataBaseSchema meta, bool extras) : base(rs, meta, extras) { }

		protected override IColumnMetadata GetColumnMetadata(DataRow rs)
		{
			return new SpannerColumnMetadata(rs);
		}

		protected override string GetColumnName(DataRow rs)
		{
			return Convert.ToString(rs["COLUMN_NAME"]);
		}

		protected override string GetConstraintName(DataRow rs)
		{
			return Convert.ToString(rs["CONSTRAINT_NAME"]);
		}

		protected override IForeignKeyMetadata GetForeignKeyMetadata(DataRow rs)
		{
			return new SpannerForeignKeyMetadata(rs);
		}

		protected override IIndexMetadata GetIndexMetadata(DataRow rs)
		{
			return new SpannerIndexMetadata(rs);
		}

		protected override string GetIndexName(DataRow rs)
		{
			return Convert.ToString(rs["INDEX_NAME"]);
		}

		protected override void ParseTableInfo(DataRow rs)
		{
			Catalog = Convert.ToString(rs["TABLE_CATALOG"]);
			Schema = Convert.ToString(rs["TABLE_SCHEMA"]);
			if (string.IsNullOrEmpty(Catalog))
			{
				Catalog = null;
			}
			if (string.IsNullOrEmpty(Schema))
			{
				Schema = null;
			}
			Name = Convert.ToString(rs["TABLE_NAME"]);
		}
	}

	public class SpannerColumnMetadata : AbstractColumnMetaData
	{
		public SpannerColumnMetadata(DataRow rs)
			: base(rs)
		{
			Name = Convert.ToString(rs["COLUMN_NAME"]);
			Nullable = Convert.ToString(rs["IS_NULLABLE"]);
			TypeName = Convert.ToString(rs["SPANNER_TYPE"]);
		}
	}

	public class SpannerIndexMetadata : AbstractIndexMetadata
	{
		public SpannerIndexMetadata(DataRow rs)
			: base(rs)
		{
			Name = Convert.ToString(rs["INDEX_NAME"]);
		}
	}

	public class SpannerForeignKeyMetadata : AbstractForeignKeyMetadata
	{
		public SpannerForeignKeyMetadata(DataRow rs)
			: base(rs)
		{
			Name = Convert.ToString(rs["CONSTRAINT_NAME"]);
		}
	}
}