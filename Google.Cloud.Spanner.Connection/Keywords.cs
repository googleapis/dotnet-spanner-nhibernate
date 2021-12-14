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

using System.Collections.Generic;

namespace Google.Cloud.Spanner.Connection
{
    public static class Keywords
    {
        /// <summary>
        /// Reserved keywords for Cloud Spanner.
        /// Source: https://cloud.google.com/spanner/docs/reference/standard-sql/lexical#reserved_keywords
        /// </summary>
        public static readonly List<string> ReservedKeywords = new List<string>()
        {
            "ALL",
            "AND",
            "ANY",
            "ARRAY",
            "AS",
            "ASC",
            "ASSERT_ROWS_MODIFIED",
            "AT",
            "BETWEEN",
            "BY",
            "CASE",
            "CAST",
            "COLLATE",
            "CONTAINS",
            "CREATE",
            "CROSS",
            "CUBE",
            "CURRENT",
            "DEFAULT",
            "DEFINE",
            "DESC",
            "DISTINCT",
            "ELSE",
            "END",
            "ENUM",
            "ESCAPE",
            "EXCEPT",
            "EXCLUDE",
            "EXISTS",
            "EXTRACT",
            "FALSE",
            "FETCH",
            "FOLLOWING",
            "FOR",
            "FROM",
            "FULL",
            "GROUP",
            "GROUPING",
            "GROUPS",
            "HASH",
            "HAVING",
            "IF",
            "IGNORE",
            "IN",
            "INNER",
            "INTERSECT",
            "INTERVAL",
            "INTO",
            "IS",
            "JOIN",
            "LATERAL",
            "LEFT",
            "LIKE",
            "LIMIT",
            "LOOKUP",
            "MERGE",
            "NATURAL",
            "NEW",
            "NO",
            "NOT",
            "NULL",
            "NULLS",
            "OF",
            "ON",
            "OR",
            "ORDER",
            "OUTER",
            "OVER",
            "PARTITION",
            "PRECEDING",
            "PROTO",
            "RANGE",
            "RECURSIVE",
            "RESPECT",
            "RIGHT",
            "ROLLUP",
            "ROWS",
            "SELECT",
            "SET",
            "SOME",
            "STRUCT",
            "TABLESAMPLE",
            "THEN",
            "TO",
            "TREAT",
            "TRUE",
            "UNBOUNDED",
            "UNION",
            "UNNEST",
            "USING",
            "WHEN",
            "WHERE",
            "WINDOW",
            "WITH",
            "WITHIN",
        };
    }
}