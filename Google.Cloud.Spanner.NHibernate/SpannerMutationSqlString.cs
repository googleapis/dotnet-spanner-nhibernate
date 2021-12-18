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
using NHibernate.SqlCommand;
using System.Collections.Generic;

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// SqlString implementation that adds additional information to the Sql command
    /// so the command can also (potentially) be executed as a mutation on Spanner.
    /// 
    /// This also includes any SELECT statement that might be needed to execute an
    /// optimistic concurrency check, as mutations cannot include a WHERE clause.
    /// Executing a SELECT statement that includes a `WHERE Version=@version` in
    /// the transaction guarantees that the version has not changed, and the row is
    /// locked for the remainder of the transaction, meaning that we know that the
    /// condition will also be true when the transaction is committed. The SELECT
    /// statement will only be executed if the command is executed as a mutation.
    /// If the command is executed as a DML statement, the version check is
    /// included in the WHERE clause of the DML statement. 
    /// </summary>
    public class SpannerMutationSqlString : SqlString
    {
        public string MutationCommandText => $"{Operation} {Table}";
        public string Operation { get; }
        public string Table { get; }
        public string[] Columns { get; }
        public List<SpannerParameter> AdditionalParameters { get; } = new List<SpannerParameter>();
        public string[] WhereColumns { get; }
        public int WhereParamsStartIndex { get; }

        public SpannerMutationSqlString(SqlString dmlSqlString, string operation, string table, string[] columns) : base(dmlSqlString)
        {
            Operation = operation;
            Table = table;
            Columns = columns;
            WhereColumns = new string[] { };
        }

        public SpannerMutationSqlString(SqlString dmlSqlString, string operation, string table, string[] columns, string[] whereColumns, int whereParamsStartIndex) : base(dmlSqlString)
        {
            Operation = operation;
            Table = table;
            Columns = columns;
            WhereColumns = whereColumns;
            WhereParamsStartIndex = whereParamsStartIndex;
        }
    }
}