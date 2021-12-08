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

using NHibernate;
using NHibernate.SqlCommand;
using System;
using System.Collections.Generic;
using System.Text;

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// Interceptor that converts specific comments to query hints for Cloud Spanner. This interceptor can be used in
    /// combination with the extension methods in the classes <see cref="ICriteriaExtensions"/>,
    /// <see cref="IQueryableExtensions"/> and <see cref="IQueryExtensions"/>. It is also possible to manually add the
    /// comments to queries.
    /// </summary>
    public class SpannerQueryHintInterceptor : EmptyInterceptor
    {
        public const string SpannerQueryHintsPrefix = "NHIBERNATE_HINTS:\n";
        private const string SpannerQueryHintsCommentPrefix = "/* NHIBERNATE_HINTS:\n";
        public const string SpannerStatementHintPrefix = "STATEMENT_HINT: ";
        public const string SpannerTableHintPrefix = "TABLE_HINT: ";
        
        public override SqlString OnPrepareStatement(SqlString sql)
        {
            if (!sql.StartsWithCaseInsensitive(SpannerQueryHintsCommentPrefix))
            {
                return sql;
            }
            // The following parsing assumes that the following is all true:
            // 1. None of the hints contain the string '*/'. This could in theory be possible if a table or index
            //    name contains this letter combination.
            // 2. None of the hints (including table and index names) contain a newline ('\n').
            var endOfFirstComment = sql.IndexOf("*/", 0, sql.Length, StringComparison.Ordinal);
            if (endOfFirstComment < SpannerQueryHintsCommentPrefix.Length)
            {
                return sql;
            }
            var actualHintsString = sql.Substring(SpannerQueryHintsCommentPrefix.Length,
                endOfFirstComment - SpannerQueryHintsCommentPrefix.Length);
            var actualHints = actualHintsString.Split("\n");
            sql = sql.Substring(endOfFirstComment + 2);
            foreach (var hint in actualHints)
            {
                if (hint.StartsWithCaseInsensitive(SpannerStatementHintPrefix))
                {
                    sql = ApplyStatementHint(sql, hint);
                }
                if (hint.StartsWithCaseInsensitive(SpannerTableHintPrefix))
                {
                    sql = ApplyTableHint(sql, hint);
                }
            }
            return sql;
        }

        private SqlString ApplyStatementHint(SqlString sql, SqlString statementHintString)
        {
            var hint = statementHintString.Substring(SpannerStatementHintPrefix.Length);
            return sql.Insert(0, hint);
        }

        private SqlString ApplyTableHint(SqlString sql, SqlString tableHintString)
        {
            var closingBacktickIndex =
                tableHintString.IndexOf("`", SpannerTableHintPrefix.Length + 1, tableHintString.Length, StringComparison.Ordinal);
            if (closingBacktickIndex < SpannerTableHintPrefix.Length + 1)
            {
                return sql;
            }
            var table = tableHintString.Substring(SpannerTableHintPrefix.Length + 1,
                closingBacktickIndex - SpannerTableHintPrefix.Length - 1);
            var hint = tableHintString.Substring(closingBacktickIndex + 1);
            var fromClause = $" FROM {table} ";
            var fromIndex = sql.IndexOfCaseInsensitive(fromClause);
            if (fromIndex > -1)
            {
                sql = sql.Insert(fromIndex + fromClause.Length - 1, hint);
            }
            var joinClause = $" JOIN {table} ";
            var joinIndex = -1;
            while ((joinIndex = sql.IndexOf(joinClause, joinIndex + 1, sql.Length, StringComparison.InvariantCultureIgnoreCase)) > -1)
            {
                sql = sql.Insert(joinIndex + joinClause.Length - 1, hint);
            }
            return sql;
        }

        internal static string AppendTableHintsString(StringBuilder builder, Dictionary<string, string> hints)
        {
            foreach (var hint in hints)
            {
                builder.Append($"{SpannerTableHintPrefix}`{hint.Key}`{hint.Value}\n");
            }
            return builder.ToString();
        }
    }
}