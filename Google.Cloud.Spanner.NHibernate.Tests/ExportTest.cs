using Google.Api.Gax;
using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Connection.MockServer;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.NHibernate.Tests.Entities;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.Spanner.NHibernate.Tests
{
    public class ExportTest : IClassFixture<NHibernateMockServerFixture>
    {
        private readonly NHibernateMockServerFixture _fixture;

        public ExportTest(NHibernateMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        [Fact]
        public async Task GetSingerAsync_ReturnsNull_IfNotFound()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            AddEmptySingerResult(GetSelectSingerSql());

            var singer = await session.GetAsync<Singer>(1L);
            Assert.Null(singer);
        }
        
        [Fact]
        public async Task Basics()
        {
            var conn = new SpannerRetriableConnection(new SpannerConnection(new SpannerConnectionStringBuilder(_fixture.ConnectionString, ChannelCredentials.Insecure)
            {
                EmulatorDetection = EmulatorDetection.None,
            }));
            _fixture.SpannerMock.AddOrUpdateStatementResult("Update singers", StatementResult.CreateUpdateCount(1L));
            var cmd = conn.CreateDmlCommand("Update singers");
            // var cmd = conn.CreateDdlCommand("CREATE TABLE Foo");
            await cmd.ExecuteNonQueryAsync();

            // var requests = _fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>();
            // Assert.Collection(requests, request => Assert.Collection(request.Statements, statement => Assert.Equal("CREATE TABLE Foo", statement)));

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request => Assert.Equal("Update singers", request.Sql));
        }

        private static string GetSelectSingerSql() =>
            "SELECT singer0_.SingerId as singerid1_0_0_, singer0_.FirstName as firstname2_0_0_, singer0_.LastName as lastname3_0_0_, "
            + "singer0_.FullName as fullname4_0_0_, singer0_.BirthDate as birthdate5_0_0_, singer0_.Picture as picture6_0_0_ "
            + "FROM Singer singer0_ WHERE singer0_.SingerId=@p0";
        
        private void AddEmptySingerResult(string sql) => AddSingerResults(sql, new List<object[]>());

        private string AddSingerResult(string sql) =>
            AddSingerResults(sql, new List<object[]>
            {
                new object[] { 1L, "Alice", "Morrison", "Alice Morrison", "1998-05-12", Convert.ToBase64String(new byte[]{1,2,3}) },
            });

        private string AddSingerResults(string sql, IEnumerable<object[]> rows)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "singerid1_0_0_"),
                    Tuple.Create(V1.TypeCode.String, "firstname2_0_0_"),
                    Tuple.Create(V1.TypeCode.String, "lastname3_0_0_"),
                    Tuple.Create(V1.TypeCode.String, "fullname4_0_0_"),
                    Tuple.Create(V1.TypeCode.Date, "birthdate5_0_0_"),
                    Tuple.Create(V1.TypeCode.Bytes, "picture6_0_0_"),
                }, rows));
            return sql;
        }
    }
}