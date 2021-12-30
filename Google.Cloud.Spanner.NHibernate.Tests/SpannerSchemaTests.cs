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
using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.Connection.MockServer;
using Google.Cloud.Spanner.NHibernate.Tests.Entities;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Mapping.ByCode;
using NHibernate.Tool.hbm2ddl;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using Xunit;
using Environment = NHibernate.Cfg.Environment;

namespace Google.Cloud.Spanner.NHibernate.Tests
{
    public class SpannerSchemaTests : IClassFixture<NHibernateMockServerFixture>
    {
        private readonly NHibernateMockServerFixture _fixture;
        
        private Configuration Configuration { get; }
        
        private Configuration InterleavedTableConfiguration { get; }

        public SpannerSchemaTests(NHibernateMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
            fixture.DatabaseAdminMock.Reset();
            
            Configuration = new Configuration().DataBaseIntegration(db =>
            {
                db.Dialect<SpannerDialect>();
                db.ConnectionString = _fixture.ConnectionString;
                db.ConnectionProvider<TestConnectionProvider>();
            });
            var mapper = new ModelMapper();
            mapper.AddMapping<SingerMapping>();
            mapper.AddMapping<AlbumMapping>();
            mapper.AddMapping<TableWithAllColumnTypesMapping>();
            mapper.AddMapping<TrackMapping>();
            var mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
            Configuration.AddMapping(mapping);
            
            InterleavedTableConfiguration = new Configuration().DataBaseIntegration(db =>
            {
                db.Dialect<SpannerDialect>();
                db.ConnectionString = _fixture.ConnectionString;
                db.ConnectionProvider<TestConnectionProvider>();
            });
            var interleavedTableMapper = new ModelMapper();
            interleavedTableMapper.AddMapping<InvoiceMapping>();
            interleavedTableMapper.AddMapping<InvoiceLineMapping>();
            interleavedTableMapper.AddMapping<InvoiceLineNoteMapping>();
            var interleavedTableMapping = interleavedTableMapper.CompileMappingForAllExplicitlyAddedEntities();
            InterleavedTableConfiguration.AddMapping(interleavedTableMapping);
        }

        private static void AssertEqual(string expected, string actual) =>
            Assert.Equal(expected.Replace(System.Environment.NewLine, "\n"), actual.Replace(System.Environment.NewLine, "\n"));
        
        [Fact]
        public void SpannerExporterCanGenerateCreateModel()
        {
            // The SpannerSchemaExport can create a valid Spanner DDL script. The default NHibernate SchemaExport will
            // generate a DDL script that contains primary key constraints that are not compatible with Spanner.
            var exporter = new SpannerSchemaExport(Configuration);
            exporter.SetDelimiter(";");
            var writer = new StringWriter();
            exporter.Create(writer, false);
            var ddl = writer.ToString();
            var expected = GetExpectedSpannerCreateDdl();
            AssertEqual(expected, ddl);
        }

        [Fact]
        public void DefaultExporterCanGenerateCreateModel()
        {
            // The default NHibernate SchemaExport generates a DDL script that is not 100% compatible with Spanner. It
            // should still be able to generate a script that is at least as close at it can get to a valid Spanner
            // script, and not cause any errors.
            var exporter = new SchemaExport(Configuration);
            exporter.SetDelimiter(";");
            var writer = new StringWriter();
            exporter.Create(writer, false);
            var ddl = writer.ToString();
            var expected = GetExpectedDefaultCreateDdl();
            AssertEqual(expected, ddl);
        }
        
        [Fact]
        public void SpannerExporterCanGenerateInterleavedTableModel()
        {
            var exporter = new SpannerSchemaExport(InterleavedTableConfiguration);
            exporter.SetDelimiter(";");
            var writer = new StringWriter();
            exporter.Create(writer, false);
            var ddl = writer.ToString();
            var expected = GetExpectedInterleavedTableDdl();
            AssertEqual(expected, ddl);
        }

        [Fact]
        public void SpannerExporterCanGenerateDropModel()
        {
            var exporter = new SpannerSchemaExport(Configuration);
            exporter.SetDelimiter(";");
            var writer = new StringWriter();
            exporter.Drop(writer, false);
            var ddl = writer.ToString();
            var expected = GetExpectedSpannerDropDdl();
            AssertEqual(expected, ddl);
        }

        [Fact]
        public void SpannerExporterCreateWithStdOutExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.Create(false, true));

        [Fact]
        public void SpannerExporterCreateAsyncWithStdOutExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.CreateAsync(false, true).WaitWithUnwrappedExceptions());

        [Fact]
        public void SpannerExporterCreateWithStdOutAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Create(false, true, conn));

        [Fact]
        public void SpannerExporterCreateAsyncWithStdOutAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.CreateAsync(false, true, conn).WaitWithUnwrappedExceptions());

        [Fact]
        public void SpannerExporterCreateWithActionExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.Create(s => { }, true));

        [Fact]
        public void SpannerExporterCreateAsyncWithActionExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.CreateAsync(s => { }, true).WaitWithUnwrappedExceptions());

        [Fact]
        public void SpannerExporterCreateWithActionAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Create(s => { }, true, conn));

        [Fact]
        public void SpannerExporterCreateAsyncWithActionAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.CreateAsync(s => { }, true, conn).WaitWithUnwrappedExceptions());

        [Fact]
        public void SpannerExporterCreateWithWriterExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.Create((TextWriter)null, true));

        [Fact]
        public void SpannerExporterCreateAsyncWithWriterExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.CreateAsync((TextWriter)null, true).WaitWithUnwrappedExceptions());

        [Fact]
        public void SpannerExporterCreateWithWriterAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Create((TextWriter)null, true, conn));

        [Fact]
        public void SpannerExporterCreateAsyncWithWriterAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.CreateAsync((TextWriter)null, true, conn).WaitWithUnwrappedExceptions());

        [Fact]
        public void SpannerExporterDropWithStdOutExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.Drop(false, true), true);

        [Fact]
        public void SpannerExporterDropAsyncWithStdOutExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.DropAsync(false, true).WaitWithUnwrappedExceptions(), true);

        [Fact]
        public void SpannerExporterDropWithStdOutAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Drop(false, true, conn), true);

        [Fact]
        public void SpannerExporterDropAsyncWithStdOutAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.DropAsync(false, true, conn).WaitWithUnwrappedExceptions(), true);

        [Fact]
        public void SpannerExporterDropWithWriterExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.Drop(null, true), true);

        [Fact]
        public void SpannerExporterDropAsyncWithWriterExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.DropAsync(null, true).WaitWithUnwrappedExceptions(), true);

        [Fact]
        public void SpannerExporterDropWithWriterAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Drop(null, true, conn), true);

        [Fact]
        public void SpannerExporterDropAsyncWithWriterAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.DropAsync(null, true, conn).WaitWithUnwrappedExceptions(), true);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteWithStdOutExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch(exporter => exporter.Execute(false, true, onlyDrop), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteAsyncWithStdOutExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch(exporter => exporter.ExecuteAsync(false, true, onlyDrop).WaitWithUnwrappedExceptions(), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteWithActionExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch(exporter => exporter.Execute(s => { }, true, onlyDrop), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteAsyncWithActionExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch(exporter => exporter.ExecuteAsync(s => { }, true, onlyDrop).WaitWithUnwrappedExceptions(), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteWithActionAndConnExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Execute(s => { }, true, onlyDrop, conn, null), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteAsyncWithActionAndConnExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.ExecuteAsync(s => { }, true, onlyDrop, conn, null).WaitWithUnwrappedExceptions(), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteWithSdtOutAndConnAndTextWriterExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Execute(false, true, onlyDrop, conn, null), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteAsyncWithSdtOutAndConnAndTextWriterExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.ExecuteAsync(false, true, onlyDrop, conn, null).WaitWithUnwrappedExceptions(), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteWithActionAndTextWriterExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch(exporter => exporter.Execute(s => { }, true, onlyDrop, null), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteAsyncWithActionAndTextWriterExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch(exporter => exporter.ExecuteAsync(s => { }, true, onlyDrop, null).WaitWithUnwrappedExceptions(), onlyDrop);

        [Fact]
        public void SpannerUpdaterSkipsExistingTable()
        {
            AddTablesResult(new []
            {
                new Table {Name = "Singer"},
            });
            AddColumnsResult(new []
            {
                new Column {TableName = "Singer", Name = "SingerId", OrdinalPosition = 1, IsNullable = "NO", SpannerType = "INT64", IsGenerated = "NEVER"},
                new Column {TableName = "Singer", Name = "FirstName", OrdinalPosition = 2, IsNullable = "YES", SpannerType = "STRING(MAX)", IsGenerated = "NEVER"},
                new Column {TableName = "Singer", Name = "LastName", OrdinalPosition = 3, IsNullable = "YES", SpannerType = "STRING(MAX)", IsGenerated = "NEVER"},
                new Column {TableName = "Singer", Name = "FullName", OrdinalPosition = 4, IsNullable = "YES", SpannerType = "STRING(MAX)", IsGenerated = "ALWAYS", GenerationExpression = "FirstName + LastName", IsStored = "YES"},
                new Column {TableName = "Singer", Name = "BirthDate", OrdinalPosition = 5, IsNullable = "YES", SpannerType = "DATE", IsGenerated = "NEVER"},
                new Column {TableName = "Singer", Name = "Picture", OrdinalPosition = 6, IsNullable = "YES", SpannerType = "BYTES(MAX)", IsGenerated = "NEVER"},
            });
            AddReferentialConstraintsResult(new ReferentialConstraint[]{});
            AddIndexesResult(new Index[]{});
            var updater = new SpannerSchemaUpdate(Configuration, new Dictionary<string, string>(Configuration.Properties){{Environment.FormatSql, "false"}});
            var statements = new List<string>();
            updater.Execute(s => statements.Add(s), false);
            Assert.Empty(updater.Exceptions);
            Assert.Collection(statements,
                s => AssertEqual("create table Album (AlbumId INT64 NOT NULL, SingerId INT64, Title STRING(MAX), ReleaseDate DATE) primary key (AlbumId)", s),
                s => AssertEqual("create table TableWithAllColumnTypes (ColInt64 INT64 NOT NULL, ColFloat64 FLOAT64, ColNumeric NUMERIC, ColBool BOOL, ColString STRING(100), ColStringMax STRING(MAX), ColBytes BYTES(100), ColBytesMax BYTES(MAX), ColDate DATE, ColTimestamp TIMESTAMP, ColJson JSON, ColCommitTs TIMESTAMP OPTIONS (allow_commit_timestamp=true), ColInt64Array ARRAY<INT64>, ColFloat64Array ARRAY<FLOAT64>, ColNumericArray ARRAY<NUMERIC>, ColBoolArray ARRAY<BOOL>, ColStringArray ARRAY<STRING(100)>, ColStringMaxArray ARRAY<STRING(MAX)>, ColBytesArray ARRAY<BYTES(100)>, ColBytesMaxArray ARRAY<BYTES(MAX)>, ColDateArray ARRAY<DATE>, ColTimestampArray ARRAY<TIMESTAMP>, ColJsonArray ARRAY<JSON>, ColComputed STRING(MAX) AS (ARRAY_TO_STRING(ColStringArray, ',')) STORED, ASC STRING(MAX)) primary key (ColInt64)", s),
                s => AssertEqual("create table Track (TrackId INT64 NOT NULL, Title STRING(MAX), AlbumId INT64 not null) primary key (TrackId)", s),
                s => AssertEqual("create index Idx_Singers_FullName on Singer (FullName)", s),
                s => AssertEqual("alter table Album add constraint FK_8373D1C5 foreign key (SingerId) references Singer (SingerId)", s),
                s => AssertEqual("alter table Track add constraint FK_1F357587 foreign key (AlbumId) references Album (AlbumId)", s)
            );
        }

        [Fact]
        public void SpannerUpdaterCreatesNewColumns()
        {
            AddTablesResult(new []
            {
                new Table {Name = "Singer"},
            });
            AddColumnsResult(new []
            {
                new Column {TableName = "Singer", Name = "SingerId", OrdinalPosition = 1, IsNullable = "NO", SpannerType = "INT64", IsGenerated = "NEVER"},
                new Column {TableName = "Singer", Name = "FirstName", OrdinalPosition = 2, IsNullable = "YES", SpannerType = "STRING(MAX)", IsGenerated = "NEVER"},
                new Column {TableName = "Singer", Name = "LastName", OrdinalPosition = 3, IsNullable = "YES", SpannerType = "STRING(MAX)", IsGenerated = "NEVER"},
                new Column {TableName = "Singer", Name = "FullName", OrdinalPosition = 4, IsNullable = "YES", SpannerType = "STRING(MAX)", IsGenerated = "ALWAYS", GenerationExpression = "FirstName + LastName", IsStored = "YES"},
            });
            AddReferentialConstraintsResult(new ReferentialConstraint[]{});
            AddIndexesResult(new Index[]{});
            var updater = new SpannerSchemaUpdate(Configuration, new Dictionary<string, string>(Configuration.Properties){{Environment.FormatSql, "false"}});
            var statements = new List<string>();
            updater.Execute(s => statements.Add(s), false);
            Assert.Empty(updater.Exceptions);
            Assert.Collection(statements,
                s => AssertEqual("alter table Singer ADD COLUMN BirthDate DATE", s),
                s => AssertEqual("alter table Singer ADD COLUMN Picture BYTES(MAX)", s),
                s => AssertEqual("create table Album (AlbumId INT64 NOT NULL, SingerId INT64, Title STRING(MAX), ReleaseDate DATE) primary key (AlbumId)", s),
                s => AssertEqual("create table TableWithAllColumnTypes (ColInt64 INT64 NOT NULL, ColFloat64 FLOAT64, ColNumeric NUMERIC, ColBool BOOL, ColString STRING(100), ColStringMax STRING(MAX), ColBytes BYTES(100), ColBytesMax BYTES(MAX), ColDate DATE, ColTimestamp TIMESTAMP, ColJson JSON, ColCommitTs TIMESTAMP OPTIONS (allow_commit_timestamp=true), ColInt64Array ARRAY<INT64>, ColFloat64Array ARRAY<FLOAT64>, ColNumericArray ARRAY<NUMERIC>, ColBoolArray ARRAY<BOOL>, ColStringArray ARRAY<STRING(100)>, ColStringMaxArray ARRAY<STRING(MAX)>, ColBytesArray ARRAY<BYTES(100)>, ColBytesMaxArray ARRAY<BYTES(MAX)>, ColDateArray ARRAY<DATE>, ColTimestampArray ARRAY<TIMESTAMP>, ColJsonArray ARRAY<JSON>, ColComputed STRING(MAX) AS (ARRAY_TO_STRING(ColStringArray, ',')) STORED, ASC STRING(MAX)) primary key (ColInt64)", s),
                s => AssertEqual("create table Track (TrackId INT64 NOT NULL, Title STRING(MAX), AlbumId INT64 not null) primary key (TrackId)", s),
                s => AssertEqual("create index Idx_Singers_FullName on Singer (FullName)", s),
                s => AssertEqual("alter table Album add constraint FK_8373D1C5 foreign key (SingerId) references Singer (SingerId)", s),
                s => AssertEqual("alter table Track add constraint FK_1F357587 foreign key (AlbumId) references Album (AlbumId)", s)
            );
        }

        [Fact]
        public void SpannerUpdaterExecuteWithStdOut() =>
            SpannerUpdaterExecute(updater => updater.Execute(false, true));

        [Fact]
        public void SpannerUpdaterExecuteWithAction() =>
            SpannerUpdaterExecute(updater => updater.Execute(s => {}, true));

        [Fact]
        public void SpannerUpdaterExecuteAsyncWithStdOut() =>
            SpannerUpdaterExecute(updater => updater.ExecuteAsync(false, true).WaitWithUnwrappedExceptions());

        [Fact]
        public void SpannerUpdaterExecuteAsyncWithAction() =>
            SpannerUpdaterExecute(updater => updater.ExecuteAsync(s => {}, true).WaitWithUnwrappedExceptions());

        private void SpannerUpdaterExecute(Action<SpannerSchemaUpdate> action)
        {
            AddTablesResult(new Table[]{});
            AddColumnsResult(new Column[] {});
            AddReferentialConstraintsResult(new ReferentialConstraint[]{});
            AddIndexesResult(new Index[]{});
            var updater = new SpannerSchemaUpdate(Configuration, new Dictionary<string, string>(Configuration.Properties){{Environment.FormatSql, "false"}});
            action.Invoke(updater);
            Assert.Empty(updater.Exceptions);
            AssertCreateBatch();
        }

        [Fact]
        public void SchemaValidatorMissesAllTables()
        {
            AddTablesResult(new Table[]{});
            AddColumnsResult(new Column[] {});
            
            var validator = new SchemaValidator(Configuration);
            var validationException = Assert.Throws<SchemaValidationException>(() => validator.Validate());
            Assert.Collection(validationException.ValidationErrors,
                err => Assert.Equal("Missing table: Singer", err),
                err => Assert.Equal("Missing table: Album", err),
                err => Assert.Equal("Missing table: TableWithAllColumnTypes", err),
                err => Assert.Equal("Missing table: Track", err)
            );
        }

        [Fact]
        public void SchemaValidatorMissesOneTable()
        {
            AddTablesResult(new []
            {
                new Table{ Name = "Singer" },
                new Table{ Name = "Album" },
                new Table{ Name = "Track" },
            });
            AddColumnsResult(new []
            {
                new Column{ TableName = "Singer", Name = "SingerId", OrdinalPosition = 1L, SpannerType = "INT64" },
                new Column{ TableName = "Singer", Name = "FirstName", OrdinalPosition = 2L, SpannerType = "STRING" },
                new Column{ TableName = "Singer", Name = "LastName", OrdinalPosition = 3L, SpannerType = "STRING" },
                new Column{ TableName = "Singer", Name = "FullName", OrdinalPosition = 4L, SpannerType = "STRING" },
                new Column{ TableName = "Singer", Name = "BirthDate", OrdinalPosition = 5L, SpannerType = "DATE" },
                new Column{ TableName = "Singer", Name = "Picture", OrdinalPosition = 6L, SpannerType = "BYTES" },
                new Column{ TableName = "Album", Name = "AlbumId", OrdinalPosition = 1L, SpannerType = "INT64" },
                new Column{ TableName = "Album", Name = "Title", OrdinalPosition = 2L, SpannerType = "STRING" },
                new Column{ TableName = "Album", Name = "ReleaseDate", OrdinalPosition = 3L, SpannerType = "DATE" },
                new Column{ TableName = "Track", Name = "TrackId", OrdinalPosition = 1L, SpannerType = "INT64" },
                new Column{ TableName = "Track", Name = "Title", OrdinalPosition = 2L, SpannerType = "STRING" },
            });
            
            var validator = new SchemaValidator(Configuration);
            var validationException = Assert.Throws<SchemaValidationException>(() => validator.Validate());
            Assert.Collection(validationException.ValidationErrors,
                err => Assert.Equal("Missing table: TableWithAllColumnTypes", err)
            );
        }

        [Fact]
        public void SchemaValidatorMissesOneColumn()
        {
            AddTablesResult(new []
            {
                new Table{ Name = "Singer" },
                new Table{ Name = "Album" },
                new Table{ Name = "TableWithAllColumnTypes" },
                new Table{ Name = "Track" },
            });
            AddColumnsResult(new []
            {
                new Column{ TableName = "Singer", Name = "SingerId", OrdinalPosition = 1L, SpannerType = "INT64" },
                new Column{ TableName = "Singer", Name = "FirstName", OrdinalPosition = 2L, SpannerType = "STRING" },
                new Column{ TableName = "Singer", Name = "LastName", OrdinalPosition = 3L, SpannerType = "STRING" },
                new Column{ TableName = "Singer", Name = "FullName", OrdinalPosition = 4L, SpannerType = "STRING" },
                new Column{ TableName = "Singer", Name = "BirthDate", OrdinalPosition = 5L, SpannerType = "DATE" },
                new Column{ TableName = "Singer", Name = "Picture", OrdinalPosition = 6L, SpannerType = "BYTES" },
                new Column{ TableName = "Album", Name = "AlbumId", OrdinalPosition = 1L, SpannerType = "INT64" },
                new Column{ TableName = "Album", Name = "Title", OrdinalPosition = 2L, SpannerType = "STRING" },
                new Column{ TableName = "Album", Name = "ReleaseDate", OrdinalPosition = 3L, SpannerType = "DATE" },
                new Column{ TableName = "Track", Name = "TrackId", OrdinalPosition = 1L, SpannerType = "INT64" },
                new Column{ TableName = "Track", Name = "Title", OrdinalPosition = 2L, SpannerType = "STRING" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColInt64", OrdinalPosition = 1L, SpannerType = "INT64" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColFloat64", OrdinalPosition = 2L, SpannerType = "FLOAT64" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColNumeric", OrdinalPosition = 3L, SpannerType = "NUMERIC" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColBool", OrdinalPosition = 4L, SpannerType = "BOOL" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColString", OrdinalPosition = 5L, SpannerType = "STRING" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColStringMax", OrdinalPosition = 6L, SpannerType = "STRING" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColBytes", OrdinalPosition = 7L, SpannerType = "BYTES" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColBytesMax", OrdinalPosition = 8L, SpannerType = "BYTES" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColDate", OrdinalPosition = 9L, SpannerType = "DATE" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColTimestamp", OrdinalPosition = 10L, SpannerType = "TIMESTAMP" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColJson", OrdinalPosition = 11L, SpannerType = "JSON" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColCommitTs", OrdinalPosition = 12L, SpannerType = "TIMESTAMP" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColInt64Array", OrdinalPosition = 13L, SpannerType = "ARRAY<INT64>" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColFloat64Array", OrdinalPosition = 14L, SpannerType = "ARRAY<FLOAT64>" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColNumericArray", OrdinalPosition = 15L, SpannerType = "ARRAY<NUMERIC>" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColBoolArray", OrdinalPosition = 16L, SpannerType = "ARRAY<BOOL>" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColStringArray", OrdinalPosition = 17L, SpannerType = "ARRAY<STRING(100)>" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColStringMaxArray", OrdinalPosition = 18L, SpannerType = "ARRAY<STRING(MAX)>" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColBytesArray", OrdinalPosition = 19L, SpannerType = "ARRAY<BYTES(100)>" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColBytesMaxArray", OrdinalPosition = 20L, SpannerType = "ARRAY<BYTES(MAX)>" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColDateArray", OrdinalPosition = 21L, SpannerType = "ARRAY<DATE>" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColTimestampArray", OrdinalPosition = 22L, SpannerType = "ARRAY<TIMESTAMP>" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColJsonArray", OrdinalPosition = 23L, SpannerType = "ARRAY<JSON>" },
                new Column{ TableName = "TableWithAllColumnTypes", Name = "ColComputed", OrdinalPosition = 24L, SpannerType = "STRING" },
            });
            
            var validator = new SchemaValidator(Configuration);
            var validationException = Assert.Throws<SchemaValidationException>(() => validator.Validate());
            Assert.Collection(validationException.ValidationErrors,
                err => Assert.Equal("Missing column: ASC in TableWithAllColumnTypes", err)
            );
        }

        [Fact]
        public void SchemaValidatorDetectsWrongDataType()
        {
            AddTablesResult(new []
            {
                new Table{ Name = "Singer" },
            });
            AddColumnsResult(new []
            {
                new Column{ TableName = "Singer", Name = "SingerId", OrdinalPosition = 1L, SpannerType = "INT64" },
                new Column{ TableName = "Singer", Name = "FirstName", OrdinalPosition = 2L, SpannerType = "STRING" },
                new Column{ TableName = "Singer", Name = "LastName", OrdinalPosition = 3L, SpannerType = "STRING" },
                new Column{ TableName = "Singer", Name = "FullName", OrdinalPosition = 4L, SpannerType = "STRING" },
                new Column{ TableName = "Singer", Name = "BirthDate", OrdinalPosition = 5L, SpannerType = "STRING" },
                new Column{ TableName = "Singer", Name = "Picture", OrdinalPosition = 6L, SpannerType = "BYTES" },
            });
            
            var validator = new SchemaValidator(Configuration);
            var validationException = Assert.Throws<SchemaValidationException>(() => validator.Validate());
            Assert.Collection(validationException.ValidationErrors,
                err => Assert.Equal("Wrong column type in Singer for column BirthDate. Found: string, Expected DATE", err),
                err => Assert.Equal("Missing table: Album", err),
                err => Assert.Equal("Missing table: TableWithAllColumnTypes", err),
                err => Assert.Equal("Missing table: Track", err)
            );
        }

        struct Table
        {
            public string Name;
            public string ParentTable;
            public string OnDeleteAction;
        }

        private void AddTablesResult(IEnumerable<Table> rows)
        {
            var sql =
                "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PARENT_TABLE_NAME, ON_DELETE_ACTION, SPANNER_STATE\nFROM INFORMATION_SCHEMA.TABLES\nWHERE TABLE_NAME=@p2\nORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PARENT_TABLE_NAME, ON_DELETE_ACTION, SPANNER_STATE";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "TABLE_CATALOG"),
                    Tuple.Create(V1.TypeCode.String, "TABLE_SCHEMA"),
                    Tuple.Create(V1.TypeCode.String, "TABLE_NAME"),
                    Tuple.Create(V1.TypeCode.String, "PARENT_TABLE_NAME"),
                    Tuple.Create(V1.TypeCode.String, "ON_DELETE_ACTION"),
                    Tuple.Create(V1.TypeCode.String, "SPANNER_STATE"),
                }, rows.Select(row => new object[]{"", "", row.Name, row.ParentTable, row.OnDeleteAction, "COMMITTED"})));
        }

        struct Column
        {
            public string TableName;
            public string Name;
            public long OrdinalPosition;
            public string IsNullable;
            public string SpannerType;
            public string IsGenerated;
            public string GenerationExpression;
            public string IsStored;
        }

        private void AddColumnsResult(IEnumerable<Column> rows)
        {
            var sql =
                "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME, CAST(COLUMN_DEFAULT AS STRING) AS COLUMN_DEFAULT, DATA_TYPE, IS_NULLABLE, SPANNER_TYPE, IS_GENERATED, GENERATION_EXPRESSION, IS_STORED, SPANNER_STATE\nFROM INFORMATION_SCHEMA.COLUMNS\nWHERE TABLE_NAME=@p2\nORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME, COLUMN_DEFAULT, DATA_TYPE, IS_NULLABLE, SPANNER_TYPE, IS_GENERATED, GENERATION_EXPRESSION, IS_STORED, SPANNER_STATE";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "TABLE_CATALOG"),
                    Tuple.Create(V1.TypeCode.String, "TABLE_SCHEMA"),
                    Tuple.Create(V1.TypeCode.String, "TABLE_NAME"),
                    Tuple.Create(V1.TypeCode.Int64, "ORDINAL_POSITION"),
                    Tuple.Create(V1.TypeCode.String, "COLUMN_NAME"),
                    Tuple.Create(V1.TypeCode.String, "COLUMN_DEFAULT"),
                    Tuple.Create(V1.TypeCode.String, "DATA_TYPE"),
                    Tuple.Create(V1.TypeCode.String, "IS_NULLABLE"),
                    Tuple.Create(V1.TypeCode.String, "SPANNER_TYPE"),
                    Tuple.Create(V1.TypeCode.String, "IS_GENERATED"),
                    Tuple.Create(V1.TypeCode.String, "GENERATION_EXPRESSION"),
                    Tuple.Create(V1.TypeCode.String, "IS_STORED"),
                    Tuple.Create(V1.TypeCode.String, "SPANNER_STATE"),
                }, rows.Select(row => new object[]{"", "", row.TableName, row.OrdinalPosition, row.Name, null, null, row.IsNullable, row.SpannerType, row.IsGenerated, row.GenerationExpression, row.IsStored, "COMMITTED"})));
        }

        struct ReferentialConstraint
        {
            public string Name;
            public string PkName;
        }

        private void AddReferentialConstraintsResult(IEnumerable<ReferentialConstraint> rows)
        {
            var sql =
                "SELECT CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, UNIQUE_CONSTRAINT_CATALOG, UNIQUE_CONSTRAINT_SCHEMA, UNIQUE_CONSTRAINT_NAME, " +
                "MATCH_OPTION, UPDATE_RULE, DELETE_RULE, SPANNER_STATE\nFROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS\n" +
                "ORDER BY CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, UNIQUE_CONSTRAINT_CATALOG, UNIQUE_CONSTRAINT_SCHEMA, UNIQUE_CONSTRAINT_NAME, MATCH_OPTION, UPDATE_RULE, DELETE_RULE, SPANNER_STATE";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "CONSTRAINT_CATALOG"),
                    Tuple.Create(V1.TypeCode.String, "CONSTRAINT_SCHEMA"),
                    Tuple.Create(V1.TypeCode.String, "CONSTRAINT_NAME"),
                    Tuple.Create(V1.TypeCode.String, "UNIQUE_CONSTRAINT_CATALOG"),
                    Tuple.Create(V1.TypeCode.String, "UNIQUE_CONSTRAINT_SCHEMA"),
                    Tuple.Create(V1.TypeCode.String, "UNIQUE_CONSTRAINT_NAME"),
                    Tuple.Create(V1.TypeCode.String, "MATCH_OPTION"),
                    Tuple.Create(V1.TypeCode.String, "UPDATE_RULE"),
                    Tuple.Create(V1.TypeCode.String, "DELETE_RULE"),
                    Tuple.Create(V1.TypeCode.String, "SPANNER_STATE"),
                }, rows.Select(row => new object[]{"", "", row.Name, "", "", row.PkName, "SIMPLE", "NO ACTION", "NO ACTION", "COMMITTED"})));
        }

        struct Index
        {
            public string TableName;
            public string Name;
            public string IndexType;
            public string ParentTableName;
            public bool IsUnique;
            public bool IsNullFiltered;
            public string IndexState;
            public bool SpannerIsManaged;
        }

        private void AddIndexesResult(IEnumerable<Index> rows)
        {
            var sql =
                "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, INDEX_TYPE, PARENT_TABLE_NAME, IS_UNIQUE, IS_NULL_FILTERED, INDEX_STATE, SPANNER_IS_MANAGED\nFROM INFORMATION_SCHEMA.INDEXES\nWHERE TABLE_NAME=@p2\nORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, INDEX_TYPE, PARENT_TABLE_NAME, IS_UNIQUE, IS_NULL_FILTERED, INDEX_STATE, SPANNER_IS_MANAGED";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "TABLE_CATALOG"),
                    Tuple.Create(V1.TypeCode.String, "TABLE_SCHEMA"),
                    Tuple.Create(V1.TypeCode.String, "TABLE_NAME"),
                    Tuple.Create(V1.TypeCode.String, "INDEX_NAME"),
                    Tuple.Create(V1.TypeCode.String, "INDEX_TYPE"),
                    Tuple.Create(V1.TypeCode.String, "PARENT_TABLE_NAME"),
                    Tuple.Create(V1.TypeCode.Bool, "IS_UNIQUE"),
                    Tuple.Create(V1.TypeCode.Bool, "IS_NULL_FILTERED"),
                    Tuple.Create(V1.TypeCode.String, "INDEX_STATE"),
                    Tuple.Create(V1.TypeCode.Bool, "SPANNER_IS_MANAGED"),
                }, rows.Select(row => new object[]{"", "", row.TableName, row.Name, row.IndexType, row.ParentTableName, row.IsUnique, row.IsNullFiltered, row.IndexState, row.SpannerIsManaged})));
        }

        private void SpannerExporterExecutesBatch(Action<SpannerSchemaExport> action, bool onlyDrop = false)
        {
            var settings = new Dictionary<string, string>
            {
                {Environment.ConnectionProvider, typeof(TestConnectionProvider).AssemblyQualifiedName},
                {Environment.ConnectionString, _fixture.ConnectionString},
            };
            var exporter = new SpannerSchemaExport(Configuration, settings);
            action.Invoke(exporter);
            AssertDropAndCreateBatch(onlyDrop);
        }

        private void SpannerExporterExecutesBatch(Action<SpannerSchemaExport, DbConnection> action, bool onlyDrop = false)
        {
            var exporter = new SpannerSchemaExport(Configuration);
            using var session = _fixture.SessionFactory.OpenSession();
            action.Invoke(exporter, session.Connection);
            AssertDropAndCreateBatch(onlyDrop);
        }

        private void AssertDropAndCreateBatch(bool onlyDrop)
        {
            if (onlyDrop)
            {
                AssertDropBatch();
            }
            else
            {
                AssertDropAndCreateBatch();
            }
        }

        private void AssertDropAndCreateBatch()
        {
            var requests = _fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>();
            Assert.Collection(requests, request =>
            {
                Assert.Collection(request.Statements,
                    statement => AssertEqual("DROP INDEX Idx_Albums_Title", statement),
                    statement => AssertEqual("DROP INDEX Idx_Singers_FullName", statement),
                    statement => AssertEqual("alter table Album  drop constraint FK_8373D1C5", statement),
                    statement => AssertEqual("alter table Track  drop constraint FK_1F357587", statement),
                    statement => AssertEqual("drop table Track", statement),
                    statement => AssertEqual("drop table TableWithAllColumnTypes", statement),
                    statement => AssertEqual("drop table Album", statement),
                    statement => AssertEqual("drop table Singer", statement),
                    statement => AssertEqual("create table Singer (SingerId INT64 NOT NULL, FirstName STRING(MAX), LastName STRING(MAX), FullName STRING(MAX), BirthDate DATE, Picture BYTES(MAX)) primary key (SingerId)", statement),
                    statement => AssertEqual("create table Album (AlbumId INT64 NOT NULL, SingerId INT64, Title STRING(MAX), ReleaseDate DATE) primary key (AlbumId)", statement),
                    statement => AssertEqual("create table TableWithAllColumnTypes (ColInt64 INT64 NOT NULL, ColFloat64 FLOAT64, ColNumeric NUMERIC, ColBool BOOL, ColString STRING(100), ColStringMax STRING(MAX), ColBytes BYTES(100), ColBytesMax BYTES(MAX), ColDate DATE, ColTimestamp TIMESTAMP, ColJson JSON, ColCommitTs TIMESTAMP OPTIONS (allow_commit_timestamp=true), ColInt64Array ARRAY<INT64>, ColFloat64Array ARRAY<FLOAT64>, ColNumericArray ARRAY<NUMERIC>, ColBoolArray ARRAY<BOOL>, ColStringArray ARRAY<STRING(100)>, ColStringMaxArray ARRAY<STRING(MAX)>, ColBytesArray ARRAY<BYTES(100)>, ColBytesMaxArray ARRAY<BYTES(MAX)>, ColDateArray ARRAY<DATE>, ColTimestampArray ARRAY<TIMESTAMP>, ColJsonArray ARRAY<JSON>, ColComputed STRING(MAX) AS (ARRAY_TO_STRING(ColStringArray, ',')) STORED, ASC STRING(MAX)) primary key (ColInt64)", statement),
                    statement => AssertEqual("create table Track (TrackId INT64 NOT NULL, Title STRING(MAX), AlbumId INT64 not null) primary key (TrackId)", statement),
                    statement => AssertEqual("create index Idx_Singers_FullName on Singer (FullName)", statement),
                    statement => AssertEqual("alter table Album add constraint FK_8373D1C5 foreign key (SingerId) references Singer (SingerId)", statement),
                    statement => AssertEqual("alter table Track add constraint FK_1F357587 foreign key (AlbumId) references Album (AlbumId)", statement),
                    statement => AssertEqual("create unique index Idx_Albums_Title on Album (SingerId, Title)", statement)
                );
            });
        }

        private void AssertCreateBatch()
        {
            var requests = _fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>();
            Assert.Collection(requests, request =>
            {
                Assert.Collection(request.Statements,
                    statement => AssertEqual("create table Singer (SingerId INT64 NOT NULL, FirstName STRING(MAX), LastName STRING(MAX), FullName STRING(MAX), BirthDate DATE, Picture BYTES(MAX)) primary key (SingerId)", statement),
                    statement => AssertEqual("create table Album (AlbumId INT64 NOT NULL, SingerId INT64, Title STRING(MAX), ReleaseDate DATE) primary key (AlbumId)", statement),
                    statement => AssertEqual("create table TableWithAllColumnTypes (ColInt64 INT64 NOT NULL, ColFloat64 FLOAT64, ColNumeric NUMERIC, ColBool BOOL, ColString STRING(100), ColStringMax STRING(MAX), ColBytes BYTES(100), ColBytesMax BYTES(MAX), ColDate DATE, ColTimestamp TIMESTAMP, ColJson JSON, ColCommitTs TIMESTAMP OPTIONS (allow_commit_timestamp=true), ColInt64Array ARRAY<INT64>, ColFloat64Array ARRAY<FLOAT64>, ColNumericArray ARRAY<NUMERIC>, ColBoolArray ARRAY<BOOL>, ColStringArray ARRAY<STRING(100)>, ColStringMaxArray ARRAY<STRING(MAX)>, ColBytesArray ARRAY<BYTES(100)>, ColBytesMaxArray ARRAY<BYTES(MAX)>, ColDateArray ARRAY<DATE>, ColTimestampArray ARRAY<TIMESTAMP>, ColJsonArray ARRAY<JSON>, ColComputed STRING(MAX) AS (ARRAY_TO_STRING(ColStringArray, ',')) STORED, ASC STRING(MAX)) primary key (ColInt64)", statement),
                    statement => AssertEqual("create table Track (TrackId INT64 NOT NULL, Title STRING(MAX), AlbumId INT64 not null) primary key (TrackId)", statement),
                    statement => AssertEqual("create index Idx_Singers_FullName on Singer (FullName)", statement),
                    statement => AssertEqual("alter table Album add constraint FK_8373D1C5 foreign key (SingerId) references Singer (SingerId)", statement),
                    statement => AssertEqual("alter table Track add constraint FK_1F357587 foreign key (AlbumId) references Album (AlbumId)", statement)
                );
            });
        }

        private void AssertDropBatch()
        {
            var requests = _fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>();
            Assert.Collection(requests, request =>
            {
                Assert.Collection(request.Statements,
                    statement => AssertEqual("DROP INDEX Idx_Albums_Title", statement),
                    statement => AssertEqual("DROP INDEX Idx_Singers_FullName", statement),
                    statement => AssertEqual("alter table Album  drop constraint FK_8373D1C5", statement),
                    statement => AssertEqual("alter table Track  drop constraint FK_1F357587", statement),
                    statement => AssertEqual("drop table Track", statement),
                    statement => AssertEqual("drop table TableWithAllColumnTypes", statement),
                    statement => AssertEqual("drop table Album", statement),
                    statement => AssertEqual("drop table Singer", statement)
                );
            });
        }

        private string GetExpectedSpannerDropDdl() =>
            @"
    DROP INDEX Idx_Albums_Title;

    DROP INDEX Idx_Singers_FullName;

    
alter table Album  drop constraint FK_8373D1C5
;

    
alter table Track  drop constraint FK_1F357587
;

    drop table Track;

    drop table TableWithAllColumnTypes;

    drop table Album;

    drop table Singer;
";

        private string GetExpectedSpannerCreateDdl() =>
            @"
    DROP INDEX Idx_Albums_Title;

    DROP INDEX Idx_Singers_FullName;

    
alter table Album  drop constraint FK_8373D1C5
;

    
alter table Track  drop constraint FK_1F357587
;

    drop table Track;

    drop table TableWithAllColumnTypes;

    drop table Album;

    drop table Singer;

    create table Singer (
        SingerId INT64 NOT NULL,
       FirstName STRING(MAX),
       LastName STRING(MAX),
       FullName STRING(MAX),
       BirthDate DATE,
       Picture BYTES(MAX)
    ) primary key (
        SingerId
    );

    create table Album (
        AlbumId INT64 NOT NULL,
       SingerId INT64,
       Title STRING(MAX),
       ReleaseDate DATE
    ) primary key (
        AlbumId
    );

    create table TableWithAllColumnTypes (
        ColInt64 INT64 NOT NULL,
       ColFloat64 FLOAT64,
       ColNumeric NUMERIC,
       ColBool BOOL,
       ColString STRING(100),
       ColStringMax STRING(MAX),
       ColBytes BYTES(100),
       ColBytesMax BYTES(MAX),
       ColDate DATE,
       ColTimestamp TIMESTAMP,
       ColJson JSON,
       ColCommitTs TIMESTAMP OPTIONS (allow_commit_timestamp=true),
       ColInt64Array ARRAY<INT64>,
       ColFloat64Array ARRAY<FLOAT64>,
       ColNumericArray ARRAY<NUMERIC>,
       ColBoolArray ARRAY<BOOL>,
       ColStringArray ARRAY<STRING(100)>,
       ColStringMaxArray ARRAY<STRING(MAX)>,
       ColBytesArray ARRAY<BYTES(100)>,
       ColBytesMaxArray ARRAY<BYTES(MAX)>,
       ColDateArray ARRAY<DATE>,
       ColTimestampArray ARRAY<TIMESTAMP>,
       ColJsonArray ARRAY<JSON>,
       ColComputed STRING(MAX) AS (ARRAY_TO_STRING(ColStringArray, ',')) STORED,
       ASC STRING(MAX)
    ) primary key (
        ColInt64
    );

    create table Track (
        TrackId INT64 NOT NULL,
       Title STRING(MAX),
       AlbumId INT64 not null
    ) primary key (
        TrackId
    );

    create index Idx_Singers_FullName on Singer (FullName);

    alter table Album 
        add constraint FK_8373D1C5 
        foreign key (SingerId) 
        references Singer (SingerId);

    alter table Track 
        add constraint FK_1F357587 
        foreign key (AlbumId) 
        references Album (AlbumId);

    create unique index Idx_Albums_Title on Album (SingerId, Title);
";

        private string GetExpectedDefaultCreateDdl() =>
            @"
    
alter table Album  drop constraint FK_8373D1C5
;

    
alter table Track  drop constraint FK_1F357587
;

    drop table Singer;

    drop table Album;

    drop table TableWithAllColumnTypes;

    drop table Track;

    create table Singer (
        SingerId INT64 not null,
       FirstName STRING(MAX),
       LastName STRING(MAX),
       FullName STRING(MAX),
       BirthDate DATE,
       Picture BYTES(MAX),
       primary key (SingerId)
    );

    create table Album (
        AlbumId INT64 not null,
       SingerId INT64,
       Title STRING(MAX),
       ReleaseDate DATE,
       primary key (AlbumId),
      unique (SingerId, Title)
    );

    create table TableWithAllColumnTypes (
        ColInt64 INT64 not null,
       ColFloat64 FLOAT64,
       ColNumeric NUMERIC,
       ColBool BOOL,
       ColString STRING(100),
       ColStringMax STRING(MAX),
       ColBytes BYTES(100),
       ColBytesMax BYTES(MAX),
       ColDate DATE,
       ColTimestamp TIMESTAMP,
       ColJson JSON,
       ColCommitTs TIMESTAMP OPTIONS (allow_commit_timestamp=true) default PENDING_COMMIT_TIMESTAMP() ,
       ColInt64Array ARRAY<INT64>,
       ColFloat64Array ARRAY<FLOAT64>,
       ColNumericArray ARRAY<NUMERIC>,
       ColBoolArray ARRAY<BOOL>,
       ColStringArray ARRAY<STRING(100)>,
       ColStringMaxArray ARRAY<STRING(MAX)>,
       ColBytesArray ARRAY<BYTES(100)>,
       ColBytesMaxArray ARRAY<BYTES(MAX)>,
       ColDateArray ARRAY<DATE>,
       ColTimestampArray ARRAY<TIMESTAMP>,
       ColJsonArray ARRAY<JSON>,
       ColComputed STRING(MAX) AS (ARRAY_TO_STRING(ColStringArray, ',')) STORED,
       ASC STRING(MAX),
       primary key (ColInt64)
    );

    create table Track (
        TrackId INT64 not null,
       Title STRING(MAX),
       AlbumId INT64 not null,
       primary key (TrackId)
    );

    create index Idx_Singers_FullName on Singer (FullName);

    create unique index Idx_Albums_Title on Album (SingerId, Title);

    alter table Album 
        add constraint FK_8373D1C5 
        foreign key (SingerId) 
        references Singer (SingerId);

    alter table Track 
        add constraint FK_1F357587 
        foreign key (AlbumId) 
        references Album (AlbumId);
";

        private string GetExpectedInterleavedTableDdl() =>
            @"
    drop table InvoiceLineNotes;

    drop table InvoiceLines;

    drop table Invoices;

    create table Invoices (
        Id STRING(MAX) NOT NULL,
       Version INT64 not null,
       CreatedAt TIMESTAMP,
       LastUpdatedAt TIMESTAMP,
       Customer STRING(MAX) not null
    ) primary key (
        Id
    );

    create table InvoiceLines (
        Id STRING(MAX) not null,
       LineNumber INT64 NOT NULL,
       Version INT64 not null,
       Product STRING(MAX) not null,
       CreatedAt TIMESTAMP,
       LastUpdatedAt TIMESTAMP
    ) primary key (
        Id,
       LineNumber
    ), INTERLEAVE IN PARENT Invoices;

    create table InvoiceLineNotes (
        Id STRING(36) not null,
       LineNumber INT64 not null,
       NoteNumber INT64 NOT NULL,
       Version INT64 not null,
       Note STRING(MAX) not null,
       CreatedAt TIMESTAMP,
       LastUpdatedAt TIMESTAMP
    ) primary key (
        Id,
       LineNumber,
       NoteNumber
    ), INTERLEAVE IN PARENT InvoiceLines;
";
    }
}