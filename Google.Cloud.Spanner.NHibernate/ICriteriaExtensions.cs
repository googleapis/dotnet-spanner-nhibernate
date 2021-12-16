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
using NHibernate;
using System.Collections.Generic;
using System.Text;

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// Extension methods for NHibernate <see cref="ICriteria"/> when working with Google Cloud Spanner.
    /// All methods in this class assume that the given <see cref="ICriteria"/> is used on a Google Cloud Spanner
    /// connection.
    /// </summary>
    public static class ICriteriaExtensions
    {
        /// <summary>
        /// Sets the statement hint to prepend to the given query.
        /// See https://cloud.google.com/spanner/docs/reference/standard-sql/query-syntax#statement_hints for more
        /// information on statement hints.
        /// 
        /// Calling this method will override any other hint(s) that may already have been set on the query.
        /// </summary>
        /// <param name="query">The query to set the statement hint on</param>
        /// <param name="hint">The statement hint to use for the query, e.g. @{OPTIMIZER_VERSION=1}</param>
        /// <returns>The query including the statement hint</returns>
        public static ICriteria SetStatementHint(this ICriteria query, string hint)
        {
            GaxPreconditions.CheckNotNull(query, nameof(query));
            GaxPreconditions.CheckNotNullOrEmpty(hint, nameof(hint));
            return query.SetComment(CreateStatementHintString(hint));
        }

        private static string CreateStatementHintString(string hint) =>
            $"{SpannerQueryHintInterceptor.SpannerQueryHintsPrefix}{SpannerQueryHintInterceptor.SpannerStatementHintPrefix}{hint}";
        
        /// <summary>
        /// Sets the table hint to use for the given query.
        /// See https://cloud.google.com/spanner/docs/reference/standard-sql/query-syntax#table_hints for more
        /// information on table hints.
        /// 
        /// Calling this method will override any other hint(s) that may already have been set on the query.
        /// </summary>
        /// <param name="query">The query to set the table hint on</param>
        /// <param name="table">The unquoted table name to apply the hint to</param>
        /// <param name="hint">The hint to add to the table, e.g. @{FORCE_INDEX=index_name}</param>
        /// <returns>The query including the table hint</returns>
        public static ICriteria SetTableHint(this ICriteria query, string table, string hint)
        {
            GaxPreconditions.CheckNotNull(query, nameof(query));
            return query.SetComment($"{SpannerQueryHintInterceptor.SpannerQueryHintsPrefix}{SpannerQueryHintInterceptor.SpannerTableHintPrefix}`{table}`{hint}");
        }
        
        /// <summary>
        /// Sets the table hints to use for the given query.
        /// Calling this method will override any other hint(s) that may already have been set on the query.
        /// </summary>
        /// <param name="query">The query to set the table hints on</param>
        /// <param name="hints">
        /// A dictionary containing the hints to use. The key of each entry should be the unquoted name of the table.
        /// The value should be the hint to add to the table, e.g. @{FORCE_INDEX=index_name}
        /// </param>
        /// <returns>The query including the table hints</returns>
        public static ICriteria SetTableHints(this ICriteria query, Dictionary<string, string> hints)
        {
            GaxPreconditions.CheckNotNull(query, nameof(query));
            GaxPreconditions.CheckNotNull(hints, nameof(hints));
            var builder = new StringBuilder($"{SpannerQueryHintInterceptor.SpannerQueryHintsPrefix}");
            return query.SetComment(SpannerQueryHintInterceptor.AppendTableHintsString(builder, hints).ToString());
        }

        /// <summary>
        /// Sets the statement and table hints to use for the given query.
        /// Calling this method will override any other hint(s) that may already have been set on the query.
        /// </summary>
        /// <param name="query">The query to set the statement and table hints on</param>
        /// <param name="statementHint">The statement hint to use for the query, e.g. @{OPTIMIZER_VERSION=1}</param>
        /// <param name="tableHints">
        /// A dictionary containing the hints to use. The key of each entry should be the unquoted name of the table.
        /// The value should be the hint to add to the table, e.g. @{FORCE_INDEX=index_name}
        /// </param>
        /// <returns>The query including the statement and table hints</returns>
        public static ICriteria SetStatementAndTableHints(this ICriteria query, string statementHint, Dictionary<string, string> tableHints)
        {
            GaxPreconditions.CheckNotNull(query, nameof(query));
            GaxPreconditions.CheckNotNullOrEmpty(statementHint, nameof(statementHint));
            GaxPreconditions.CheckNotNull(tableHints, nameof(tableHints));
            var builder = new StringBuilder(CreateStatementHintString(statementHint)).Append('\n');
            return query.SetComment(SpannerQueryHintInterceptor.AppendTableHintsString(builder, tableHints).ToString());
        }
        
        /// <summary>
        /// Sets the join hint to use for the given query.
        /// See https://cloud.google.com/spanner/docs/reference/standard-sql/query-syntax#join_hints for more
        /// information on join hints.
        /// 
        /// Calling this method will override any other hint(s) that may already have been set on the query.
        /// </summary>
        /// <param name="query">The query to set the join hint on</param>
        /// <param name="table">
        /// The unquoted name of the right-hand table of the join to apply the hint to.
        /// Example: `SELECT * FROM Albums LEFT JOIN Singers ...` requires the table name Singers to be specified to add
        /// a hint to the join.
        /// </param>
        /// <param name="hint">The hint to add to the join, e.g. @{JOIN_METHOD=HASH_JOIN}</param>
        /// <returns>The query including the join hint</returns>
        public static ICriteria SetJoinHint(this ICriteria query, string table, string hint)
        {
            GaxPreconditions.CheckNotNull(query, nameof(query));
            return query.SetComment($"{SpannerQueryHintInterceptor.SpannerQueryHintsPrefix}{SpannerQueryHintInterceptor.SpannerJoinHintPrefix}`{table}`{hint}");
        }
        
        /// <summary>
        /// Sets the join hints to use for the given query.
        /// See https://cloud.google.com/spanner/docs/reference/standard-sql/query-syntax#join_hints for more
        /// information on join hints.
        /// 
        /// Calling this method will override any other hint(s) that may already have been set on the query.
        /// </summary>
        /// <param name="query">The query to set the join hint on</param>
        /// <param name="hints">
        /// A dictionary containing the hints to use. The key of each entry should be the unquoted name of the
        /// right-hand table of the join.
        /// Example: `SELECT * FROM Albums LEFT JOIN Singers ...` requires the table name Singers to be specified to add
        /// a hint to the join.
        /// The value should be the hint to add to the join, e.g. @{JOIN_METHOD=HASH_JOIN}
        /// </param>
        /// <returns>The query including the join hints</returns>
        public static ICriteria SetJoinHints(this ICriteria query, Dictionary<string, string> hints)
        {
            GaxPreconditions.CheckNotNull(query, nameof(query));
            GaxPreconditions.CheckNotNull(hints, nameof(hints));
            var builder = new StringBuilder($"{SpannerQueryHintInterceptor.SpannerQueryHintsPrefix}");
            return query.SetComment(SpannerQueryHintInterceptor.AppendJoinHintsString(builder, hints).ToString());
        }

        /// <summary>
        /// Sets the statement and join hints to use for the given query.
        /// Calling this method will override any other hint(s) that may already have been set on the query.
        /// </summary>
        /// <param name="query">The query to set the statement and join hints on</param>
        /// <param name="statementHint">The statement hint to use for the query, e.g. @{OPTIMIZER_VERSION=1}</param>
        /// <param name="joinHints">
        /// A dictionary containing the hints to use. The key of each entry should be the unquoted name of the
        /// right-hand table of the join.
        /// The value should be the hint to add to the table, e.g. @{JOIN_METHOD=HASH_JOIN}
        /// </param>
        /// <returns>The query including the statement and join hints</returns>
        public static ICriteria SetStatementAndJoinHints(this ICriteria query, string statementHint, Dictionary<string, string> joinHints)
        {
            GaxPreconditions.CheckNotNull(query, nameof(query));
            GaxPreconditions.CheckNotNullOrEmpty(statementHint, nameof(statementHint));
            GaxPreconditions.CheckNotNull(joinHints, nameof(joinHints));
            var builder = new StringBuilder(CreateStatementHintString(statementHint)).Append('\n');
            return query.SetComment(SpannerQueryHintInterceptor.AppendJoinHintsString(builder, joinHints).ToString());
        }

        /// <summary>
        /// Sets the table and join hints to use for the given query.
        /// Calling this method will override any other hint(s) that may already have been set on the query.
        /// </summary>
        /// <param name="query">The query to set the table and join hints on</param>
        /// <param name="tableHints">
        /// A dictionary containing the hints to use. The key of each entry should be the unquoted name of the table.
        /// The value should be the hint to add to the table, e.g. @{FORCE_INDEX=index_name}
        /// </param>
        /// <param name="joinHints">
        /// A dictionary containing the hints to use. The key of each entry should be the unquoted name of the
        /// right-hand table of the join.
        /// The value should be the hint to add to the table, e.g. @{JOIN_METHOD=HASH_JOIN}
        /// </param>
        /// <returns>The query including the table and join hints</returns>
        public static ICriteria SetTableAndJoinHints(this ICriteria query, Dictionary<string, string> tableHints, Dictionary<string, string> joinHints)
        {
            GaxPreconditions.CheckNotNull(query, nameof(query));
            GaxPreconditions.CheckNotNull(tableHints, nameof(tableHints));
            GaxPreconditions.CheckNotNull(joinHints, nameof(joinHints));
            var builder = new StringBuilder($"{SpannerQueryHintInterceptor.SpannerQueryHintsPrefix}");
            builder = SpannerQueryHintInterceptor.AppendTableHintsString(builder, tableHints);
            builder = SpannerQueryHintInterceptor.AppendJoinHintsString(builder, joinHints);
            return query.SetComment(builder.ToString());
        }

        /// <summary>
        /// Sets the statement, table and join hints to use for the given query.
        /// Calling this method will override any other hint(s) that may already have been set on the query.
        /// </summary>
        /// <param name="query">The query to set the statement, table and join hints on</param>
        /// <param name="statementHint">The statement hint to use for the query, e.g. @{OPTIMIZER_VERSION=1}</param>
        /// <param name="tableHints">
        /// A dictionary containing the hints to use. The key of each entry should be the unquoted name of the table.
        /// The value should be the hint to add to the table, e.g. @{FORCE_INDEX=index_name}
        /// </param>
        /// <param name="joinHints">
        /// A dictionary containing the hints to use. The key of each entry should be the unquoted name of the
        /// right-hand table of the join.
        /// The value should be the hint to add to the table, e.g. @{JOIN_METHOD=HASH_JOIN}
        /// </param>
        /// <returns>The query including the statement, table and join hints</returns>
        public static ICriteria SetStatementAndTableAndJoinHints(this ICriteria query, string statementHint, Dictionary<string, string> tableHints, Dictionary<string, string> joinHints)
        {
            GaxPreconditions.CheckNotNull(query, nameof(query));
            GaxPreconditions.CheckNotNullOrEmpty(statementHint, nameof(statementHint));
            GaxPreconditions.CheckNotNull(tableHints, nameof(tableHints));
            GaxPreconditions.CheckNotNull(joinHints, nameof(joinHints));
            var builder = new StringBuilder(CreateStatementHintString(statementHint)).Append('\n');
            builder = SpannerQueryHintInterceptor.AppendTableHintsString(builder, tableHints);
            builder = SpannerQueryHintInterceptor.AppendJoinHintsString(builder, joinHints);
            return query.SetComment(builder.ToString());
        }
    }
}