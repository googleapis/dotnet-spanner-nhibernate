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

using Google.Cloud.Spanner.Connection.MockServer;
using Google.Cloud.Spanner.NHibernate.Tests.Entities;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using NHibernate.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.NHibernate.Tests
{
    public class SpannerSingleTableInheritanceTests : IClassFixture<NHibernateMockServerFixture>
    {
        private readonly NHibernateMockServerFixture _fixture;

        public SpannerSingleTableInheritanceTests(NHibernateMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        [Fact]
        public async Task GetStudent_UsesDiscriminator()
        {
            AddGetStudentResult(GetStudentSql());
            using var session = _fixture.SessionFactory.OpenSession();

            var student = await session.GetAsync<Student>("1");
            Assert.Equal(123456L, student.StudentNumber);
        }

        [Fact]
        public async Task GetAllStudents_UsesDiscriminator()
        {
            AddQueryStudentResult(QueryStudentsSql());
            using var session = _fixture.SessionFactory.OpenSession();

            var students = await session.Query<Student>().ToListAsync();
            Assert.Collection(students,
                student => Assert.Equal(123456L, student.StudentNumber));
        }

        [CombinatorialData]
        [Theory]
        public async Task InsertStudent_InsertsDiscriminatorAndCommitTimestamp(bool explicitTransaction)
        {
            var insertSql =
                "INSERT INTO Persons " +
                "(Version, LastUpdatedAt, FirstName, LastName, StudentNumber, PersonType, CreatedAt, Id) VALUES " +
                "(@p0, @p1, @p2, @p3, @p4, 'Student', PENDING_COMMIT_TIMESTAMP(), @p5)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1));
            var selectFullNameSql = AddSelectStudentFullNameResult("Pete Allison");
            
            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = explicitTransaction ? session.BeginTransaction() : null;
            var student = new Student
            {
                FirstName = "Pete",
                LastName = "Allison",
                StudentNumber = 123456L,
            };
            var id = await session.SaveAsync(student);
            await (transaction?.CommitAsync() ?? session.FlushAsync());

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request =>
            {
                Assert.Equal(insertSql, request.Sql);
                Assert.Collection(request.Params.Fields,
                    param => Assert.Equal("1", param.Value.StringValue),
                    param => Assert.Equal(Value.KindOneofCase.NullValue, param.Value.KindCase),
                    param => Assert.Equal("Pete", param.Value.StringValue),
                    param => Assert.Equal("Allison", param.Value.StringValue),
                    param => Assert.Equal("123456", param.Value.StringValue),
                    param => Assert.Equal(id, param.Value.StringValue)
                );
            }, request =>
            {
                Assert.Equal(selectFullNameSql, request.Sql);
                Assert.Collection(request.Params.Fields, param => Assert.Equal(id, param.Value.StringValue));
            });
        }

        [CombinatorialData]
        [Theory]
        public async Task UpdateStudent_SetsCommitTimestamp(bool explicitTransaction)
        {
            var getSql = GetStudentSql();
            AddGetStudentResult(getSql);
            var updateSql =
                "UPDATE Persons SET Version = @p0, LastName = @p1, LastUpdatedAt = PENDING_COMMIT_TIMESTAMP() WHERE Id = @p2 AND Version = @p3";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1));
            var selectFullNameSql = AddSelectStudentFullNameResult("Pete Allison-Peterson");
            
            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = explicitTransaction ? session.BeginTransaction() : null;
            var student = await session.GetAsync<Student>("1");
            student.LastName = "Allison-Peterson";
            await (transaction?.CommitAsync() ?? session.FlushAsync());

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request =>
            {
                Assert.Equal(getSql, request.Sql);
                Assert.Collection(request.Params.Fields, param => Assert.Equal("1", param.Value.StringValue));
            }, request =>
            {
                Assert.Equal(updateSql, request.Sql);
                Assert.Collection(request.Params.Fields,
                    param => Assert.Equal("11", param.Value.StringValue),
                    param => Assert.Equal("Allison-Peterson", param.Value.StringValue),
                    param => Assert.Equal("1", param.Value.StringValue),
                    param => Assert.Equal("10", param.Value.StringValue)
                );
            }, request =>
            {
                Assert.Equal(selectFullNameSql, request.Sql);
                Assert.Collection(request.Params.Fields, param => Assert.Equal("1", param.Value.StringValue));
            });
        }

        [CombinatorialData]
        [Theory]
        public async Task DeleteStudent(bool explicitTransaction)
        {
            var getSql = GetStudentSql();
            AddGetStudentResult(getSql);
            var deleteSql = "DELETE FROM Persons WHERE Id = @p0 AND Version = @p1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(deleteSql, StatementResult.CreateUpdateCount(1));
            
            using var session = _fixture.SessionFactory.OpenSession();
            using var transaction = explicitTransaction ? session.BeginTransaction() : null;
            var student = await session.GetAsync<Student>("1");
            await session.DeleteAsync(student);
            await (transaction?.CommitAsync() ?? session.FlushAsync());

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests, request =>
            {
                Assert.Equal(getSql, request.Sql);
                Assert.Collection(request.Params.Fields, param => Assert.Equal("1", param.Value.StringValue));
            }, request =>
            {
                Assert.Equal(deleteSql, request.Sql);
                Assert.Collection(request.Params.Fields,
                    param => Assert.Equal("1", param.Value.StringValue),
                    param => Assert.Equal("10", param.Value.StringValue)
                );
            });
        }

        [Fact]
        public async Task InsertStudentUsingMutations_InsertsDiscriminatorAndCommitTimestamp()
        {
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.Always);
            using var transaction = session.BeginTransaction();
            var student = new Student
            {
                FirstName = "Pete",
                LastName = "Allison",
                StudentNumber = 123456L,
            };
            var id = await session.SaveAsync(student);
            await transaction.CommitAsync();

            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits, commit =>
            {
                Assert.Collection(commit.Mutations, mutation =>
                {
                    Assert.Equal(Mutation.OperationOneofCase.Insert, mutation.OperationCase);
                    Assert.Equal("Persons", mutation.Insert.Table);
                    Assert.Collection(mutation.Insert.Columns,
                        column => Assert.Equal("Version", column),
                        column => Assert.Equal("LastUpdatedAt", column),
                        column => Assert.Equal("FirstName", column),
                        column => Assert.Equal("LastName", column),
                        column => Assert.Equal("StudentNumber", column),
                        column => Assert.Equal("Id", column),
                        column => Assert.Equal("PersonType", column),
                        column => Assert.Equal("CreatedAt", column)
                    );
                    Assert.Collection(mutation.Insert.Values, cols => Assert.Collection(cols.Values,
                        col => Assert.Equal("1", col.StringValue),
                        col => Assert.Equal(Value.KindOneofCase.NullValue, col.KindCase),
                        col => Assert.Equal("Pete", col.StringValue),
                        col => Assert.Equal("Allison", col.StringValue),
                        col => Assert.Equal("123456", col.StringValue),
                        col => Assert.Equal(id, col.StringValue),
                        col => Assert.Equal("Student", col.StringValue),
                        col => Assert.Equal("spanner.commit_timestamp()", col.StringValue)
                    ));
                });
            });
        }

        [Fact]
        public async Task UpdateStudentUsingMutations_SetsCommitTimestamp()
        {
            var getSql = GetStudentSqlWithComments();
            AddGetStudentResult(getSql);
            var checkVersionSql = "SELECT 1 AS C FROM Persons WHERE Id = @p0 AND Version = @p1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(checkVersionSql, StatementResult.CreateResultSet(
                new []{new Tuple<TypeCode, string>(TypeCode.Int64, "C")},
                new []{ new object[] {1L}}));
            
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.Always);
            using var transaction = session.BeginTransaction();
            var student = await session.GetAsync<Student>("1");
            student.LastName = "Allison-Peterson";
            await transaction.CommitAsync();

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests,
                request =>
                {
                    Assert.Equal(getSql, request.Sql);
                    Assert.Collection(request.Params.Fields, param => Assert.Equal("1", param.Value.StringValue));
                },
                request =>
                {
                    Assert.Equal(checkVersionSql, request.Sql);
                    Assert.Collection(request.Params.Fields,
                        param => Assert.Equal("1", param.Value.StringValue), // Id
                        param => Assert.Equal("10", param.Value.StringValue) // Version
                    );
                }
            );
            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits, commit =>
            {
                Assert.Collection(commit.Mutations, mutation =>
                {
                    Assert.Equal(Mutation.OperationOneofCase.Update, mutation.OperationCase);
                    Assert.Equal("Persons", mutation.Update.Table);
                    Assert.Collection(mutation.Update.Columns,
                        column => Assert.Equal("Version", column),
                        column => Assert.Equal("LastName", column),
                        column => Assert.Equal("Id", column),
                        column => Assert.Equal("LastUpdatedAt", column)
                    );
                    Assert.Collection(mutation.Update.Values, cols => Assert.Collection(cols.Values,
                        col => Assert.Equal("11", col.StringValue),
                        col => Assert.Equal("Allison-Peterson", col.StringValue),
                        col => Assert.Equal("1", col.StringValue),
                        col => Assert.Equal("spanner.commit_timestamp()", col.StringValue)
                    ));
                });
            });
        }

        [Fact]
        public async Task DeleteStudentUsingMutations()
        {
            var getSql = GetStudentSqlWithComments();
            AddGetStudentResult(getSql);
            var checkVersionSql = "SELECT 1 AS C FROM Persons WHERE Id = @p0 AND Version = @p1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(checkVersionSql, StatementResult.CreateResultSet(
                new []{new Tuple<TypeCode, string>(TypeCode.Int64, "C")},
                new []{ new object[] {1L}}));
            
            using var session = _fixture.SessionFactoryUsingMutations.OpenSession().SetBatchMutationUsage(MutationUsage.Always);
            using var transaction = session.BeginTransaction();
            var student = await session.GetAsync<Student>("1");
            await session.DeleteAsync(student);
            await transaction.CommitAsync();

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>();
            Assert.Collection(requests,
                request =>
                {
                    Assert.Equal(getSql, request.Sql);
                    Assert.Collection(request.Params.Fields, param => Assert.Equal("1", param.Value.StringValue));
                },
                request =>
                {
                    Assert.Equal(checkVersionSql, request.Sql);
                    Assert.Collection(request.Params.Fields,
                        param => Assert.Equal("1", param.Value.StringValue), // Id
                        param => Assert.Equal("10", param.Value.StringValue) // Version
                    );
                }
            );
            var commits = _fixture.SpannerMock.Requests.OfType<CommitRequest>();
            Assert.Collection(commits, commit =>
            {
                Assert.Collection(commit.Mutations, mutation =>
                {
                    Assert.Equal(Mutation.OperationOneofCase.Delete, mutation.OperationCase);
                    Assert.Equal("Persons", mutation.Delete.Table);
                    Assert.Collection(mutation.Delete.KeySet.Keys, key => Assert.Collection(key.Values, keyCol => Assert.Equal("1", keyCol.StringValue)));
                });
            });
        }

        private string GetStudentSql() =>
            "SELECT student0_.Id as id1_6_0_, student0_.Version as version3_6_0_, student0_.CreatedAt as createdat4_6_0_, " +
            "student0_.LastUpdatedAt as lastupdatedat5_6_0_, student0_.FirstName as firstname6_6_0_, " +
            "student0_.LastName as lastname7_6_0_, student0_.FullName as fullname8_6_0_, student0_.StudentNumber as studentnumber9_6_0_ " +
            "FROM Persons student0_ WHERE student0_.Id=@p0 and student0_.PersonType='Student'";

        private string GetStudentSqlWithComments() =>
            "/* load Google.Cloud.Spanner.NHibernate.Tests.Entities.Student */ SELECT student0_.Id as id1_6_0_, student0_.Version as version3_6_0_, student0_.CreatedAt as createdat4_6_0_, student0_.LastUpdatedAt as lastupdatedat5_6_0_, student0_.FirstName as firstname6_6_0_, student0_.LastName as lastname7_6_0_, student0_.FullName as fullname8_6_0_, student0_.StudentNumber as studentnumber9_6_0_ FROM Persons student0_ WHERE student0_.Id=@p0 and student0_.PersonType='Student'";
        
        private string QueryStudentsSql() =>
            "select student0_.Id as id1_6_, student0_.Version as version3_6_, student0_.CreatedAt as createdat4_6_, " +
            "student0_.LastUpdatedAt as lastupdatedat5_6_, student0_.FirstName as firstname6_6_, student0_.LastName as lastname7_6_, " +
            "student0_.FullName as fullname8_6_, student0_.StudentNumber as studentnumber9_6_ " +
            "from Persons student0_ where student0_.PersonType='Student'";

        private void AddGetStudentResult(string sql) => AddSingleStudentResult(sql, "0_");

        private void AddQueryStudentResult(string sql) => AddSingleStudentResult(sql, "");
        
        private void AddSingleStudentResult(string sql, string suffix) =>
            AddStudentResults(sql, new List<object[]>
            {
                new object[] { "1", 10L, DateTime.Now, null, "Alice", "Morrison", "Alice Morrison", 123456L },
            }, suffix);

        private void AddStudentResults(string sql, IEnumerable<object[]> rows, string suffix) =>
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, $"id1_6_{suffix}"),
                    Tuple.Create(V1.TypeCode.Int64, $"version3_6_{suffix}"),
                    Tuple.Create(V1.TypeCode.Timestamp, $"createdat4_6_{suffix}"),
                    Tuple.Create(V1.TypeCode.Timestamp, $"lastupdatedat5_6_{suffix}"),
                    Tuple.Create(V1.TypeCode.String, $"firstname6_6_{suffix}"),
                    Tuple.Create(V1.TypeCode.String, $"lastname7_6_{suffix}"),
                    Tuple.Create(V1.TypeCode.String, $"fullname8_6_{suffix}"),
                    Tuple.Create(V1.TypeCode.Int64, $"studentnumber9_6_{suffix}"),
                }, rows));

        private string AddSelectStudentFullNameResult(string fullName)
        {
            var selectFullNameSql =
                $"SELECT student_.FullName as fullname8_6_ FROM Persons student_ WHERE student_.Id=@p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(selectFullNameSql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "fullname8_6_"),
                },
                new List<object[]>
                {
                    new object[] { fullName },
                }
            ));
            return selectFullNameSql;
        }
    }
}