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

using Google.Cloud.Spanner.Admin.Database.V1;
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

        public SpannerSchemaTests(NHibernateMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
            fixture.DatabaseAdminMock.Reset();
        }

        [Fact]
        public void SpannerExporterCanGenerateCreateModel()
        {
            // The SpannerSchemaExport can create a valid Spanner DDL script. The default NHibernate SchemaExport will
            // generate a DDL script that contains primary key constraints that are not compatible with Spanner.
            var exporter = new SpannerSchemaExport(_fixture.Configuration);
            exporter.SetDelimiter(";");
            var writer = new StringWriter();
            exporter.Create(writer, false);
            var ddl = writer.ToString();
            var expected = GetExpectedSpannerCreateDdl();
            Assert.Equal(expected, ddl);
        }

        [Fact]
        public void DefaultExporterCanGenerateCreateModel()
        {
            // The default NHibernate SchemaExport generates a DDL script that is not 100% compatible with Spanner. It
            // should still be able to generate a script that is at least as close at it can get to a valid Spanner
            // script, and not cause any errors.
            var exporter = new SchemaExport(_fixture.Configuration);
            exporter.SetDelimiter(";");
            var writer = new StringWriter();
            exporter.Create(writer, false);
            var ddl = writer.ToString();
            var expected = GetExpectedDefaultCreateDdl();
            Assert.Equal(expected, ddl);
        }

        [Fact]
        public void SpannerExporterCanGenerateDropModel()
        {
            var exporter = new SpannerSchemaExport(_fixture.Configuration);
            exporter.SetDelimiter(";");
            var writer = new StringWriter();
            exporter.Drop(writer, false);
            var ddl = writer.ToString();
            var expected = GetExpectedSpannerDropDdl();
            Assert.Equal(expected, ddl);
        }

        [Fact]
        public void SpannerExporterCreateWithStdOutExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.Create(false, true));

        [Fact]
        public void SpannerExporterCreateWithStdOutAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Create(false, true, conn));

        [Fact]
        public void SpannerExporterCreateWithActionExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.Create(s => { }, true));

        [Fact]
        public void SpannerExporterCreateWithActionAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Create(s => { }, true, conn));

        [Fact]
        public void SpannerExporterCreateWithWriterExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.Create((TextWriter)null, true));

        [Fact]
        public void SpannerExporterCreateWithWriterAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Create((TextWriter)null, true, conn));

        [Fact]
        public void SpannerExporterDropWithStdOutExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.Drop(false, true), true);

        [Fact]
        public void SpannerExporterDropWithStdOutAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Drop(false, true, conn), true);

        [Fact]
        public void SpannerExporterDropWithWriterExecutesBatch() =>
            SpannerExporterExecutesBatch(exporter => exporter.Drop(null, true), true);

        [Fact]
        public void SpannerExporterDropWithWriterAndConnExecutesBatch() =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Drop(null, true, conn), true);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteWithStdOutExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch(exporter => exporter.Execute(false, true, onlyDrop), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteWithActionExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch(exporter => exporter.Execute(s => { }, true, onlyDrop), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteWithActionAndConnExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Execute(s => { }, true, onlyDrop, conn, null), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteWithSdtOutAndConnAndTextWriterExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch((exporter, conn) => exporter.Execute(false, true, onlyDrop, conn, null), onlyDrop);

        [CombinatorialData]
        [Theory]
        public void SpannerExporterExecuteWithActionAndTextWriterExecutesBatch(bool onlyDrop) =>
            SpannerExporterExecutesBatch(exporter => exporter.Execute(s => { }, true, onlyDrop, null), onlyDrop);

        private void SpannerExporterExecutesBatch(Action<SpannerSchemaExport> action, bool onlyDrop = false)
        {
            var settings = new Dictionary<string, string>
            {
                {Environment.ConnectionProvider, typeof(TestConnectionProvider).AssemblyQualifiedName},
                {Environment.ConnectionString, _fixture.ConnectionString},
            };
            var exporter = new SpannerSchemaExport(_fixture.Configuration, settings);
            action.Invoke(exporter);
            AssertDropAndCreateBatch(onlyDrop);
        }

        private void SpannerExporterExecutesBatch(Action<SpannerSchemaExport, DbConnection> action, bool onlyDrop = false)
        {
            var exporter = new SpannerSchemaExport(_fixture.Configuration);
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
                    statement => Assert.Equal("alter table Album  drop constraint FK_8373D1C5", statement),
                    statement => Assert.Equal("alter table Track  drop constraint FK_1F357587", statement),
                    statement => Assert.Equal("drop table Singer", statement),
                    statement => Assert.Equal("drop table Album", statement),
                    statement => Assert.Equal("drop table TableWithAllColumnTypes", statement),
                    statement => Assert.Equal("drop table Track", statement),
                    statement => Assert.Equal("create table Singer (SingerId INT64 NOT NULL, FirstName STRING(MAX), LastName STRING(MAX), FullName STRING(MAX), BirthDate DATE, Picture BYTES(MAX)) primary key (SingerId)", statement),
                    statement => Assert.Equal("create table Album (AlbumId INT64 NOT NULL, Title STRING(MAX), ReleaseDate DATE, SingerId INT64) primary key (AlbumId)", statement),
                    statement => Assert.Equal("create table TableWithAllColumnTypes (ColInt64 INT64 NOT NULL, ColFloat64 FLOAT64, ColNumeric NUMERIC, ColBool BOOL, ColString STRING(100), ColStringMax STRING(MAX), ColBytes BYTES(100), ColBytesMax BYTES(MAX), ColDate DATE, ColTimestamp TIMESTAMP, ColJson JSON, ColCommitTs TIMESTAMP default PENDING_COMMIT_TIMESTAMP() , ColInt64Array ARRAY<INT64>, ColFloat64Array ARRAY<FLOAT64>, ColNumericArray ARRAY<NUMERIC>, ColBoolArray ARRAY<BOOL>, ColStringArray ARRAY<STRING(100)>, ColStringMaxArray ARRAY<STRING(MAX)>, ColBytesArray ARRAY<BYTES(100)>, ColBytesMaxArray ARRAY<BYTES(MAX)>, ColDateArray ARRAY<DATE>, ColTimestampArray ARRAY<TIMESTAMP>, ColJsonArray ARRAY<JSON>, ColComputed STRING(MAX), ASC STRING(MAX)) primary key (ColInt64)", statement),
                    statement => Assert.Equal("create table Track (TrackId INT64 NOT NULL, Title STRING(MAX), AlbumId INT64 not null) primary key (TrackId)", statement),
                    statement => Assert.Equal("alter table Album add constraint FK_8373D1C5 foreign key (SingerId) references Singer (SingerId)", statement),
                    statement => Assert.Equal("alter table Track add constraint FK_1F357587 foreign key (AlbumId) references Album (AlbumId)", statement)
                );
            });
        }

        private void AssertDropBatch()
        {
            var requests = _fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>();
            Assert.Collection(requests, request =>
            {
                Assert.Collection(request.Statements,
                    statement => Assert.Equal("alter table Album  drop constraint FK_8373D1C5", statement),
                    statement => Assert.Equal("alter table Track  drop constraint FK_1F357587", statement),
                    statement => Assert.Equal("drop table Singer", statement),
                    statement => Assert.Equal("drop table Album", statement),
                    statement => Assert.Equal("drop table TableWithAllColumnTypes", statement),
                    statement => Assert.Equal("drop table Track", statement)
                );
            });
        }

        private string GetExpectedSpannerDropDdl() =>
            @"
    
alter table Album  drop constraint FK_8373D1C5
;

    
alter table Track  drop constraint FK_1F357587
;

    drop table Singer;

    drop table Album;

    drop table TableWithAllColumnTypes;

    drop table Track;
";

        private string GetExpectedSpannerCreateDdl() =>
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
       Title STRING(MAX),
       ReleaseDate DATE,
       SingerId INT64
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
       ColCommitTs TIMESTAMP default PENDING_COMMIT_TIMESTAMP() ,
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
       ColComputed STRING(MAX),
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

    alter table Album 
        add constraint FK_8373D1C5 
        foreign key (SingerId) 
        references Singer (SingerId);

    alter table Track 
        add constraint FK_1F357587 
        foreign key (AlbumId) 
        references Album (AlbumId);
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
       Title STRING(MAX),
       ReleaseDate DATE,
       SingerId INT64,
       primary key (AlbumId)
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
       ColCommitTs TIMESTAMP default PENDING_COMMIT_TIMESTAMP() ,
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
       ColComputed STRING(MAX),
       ASC STRING(MAX),
       primary key (ColInt64)
    );

    create table Track (
        TrackId INT64 not null,
       Title STRING(MAX),
       AlbumId INT64 not null,
       primary key (TrackId)
    );

    alter table Album 
        add constraint FK_8373D1C5 
        foreign key (SingerId) 
        references Singer (SingerId);

    alter table Track 
        add constraint FK_1F357587 
        foreign key (AlbumId) 
        references Album (AlbumId);
";
    }
}