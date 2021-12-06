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

using System.IO;
using Xunit;

namespace Google.Cloud.Spanner.NHibernate.Tests
{
    public class SpannerSchemaExporterTests : IClassFixture<NHibernateMockServerFixture>
    {
        private readonly NHibernateMockServerFixture _fixture;

        public SpannerSchemaExporterTests(NHibernateMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        [Fact]
        public void ExporterCanCreateModel()
        {
            var exporter = new SpannerSchemaExport(_fixture.Configuration);
            exporter.SetDelimiter(";");
            var writer = new StringWriter();
            exporter.Create(writer, false);
            var ddl = writer.ToString();
            var expected = GetExpectedCreateDdl();
            Assert.Equal(expected, ddl);
        }

        private string GetExpectedCreateDdl() =>
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
        SingerId INT64 ,
       FirstName STRING(MAX),
       LastName STRING(MAX),
       FullName STRING(MAX),
       BirthDate DATE,
       Picture BYTES(MAX)
    ) primary key (
        SingerId
    );

    create table Album (
        AlbumId INT64 ,
       Title STRING(MAX),
       ReleaseDate DATE,
       SingerId INT64
    ) primary key (
        AlbumId
    );

    create table TableWithAllColumnTypes (
        ColInt64 INT64 ,
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
        TrackId INT64 ,
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
    }
}