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
    /// Extension methods for NHibernate <see cref="IQuery"/> when working with Google Cloud Spanner.
    /// All methods in this class assume that the given <see cref="IQuery"/> is used on a Google Cloud Spanner
    /// connection.
    /// </summary>
    public static class IQueryExtensions
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
        public static IQuery SetStatementHint(this IQuery query, string hint)
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
        public static IQuery SetTableHint(this IQuery query, string table, string hint)
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
        public static IQuery SetTableHints(this IQuery query, Dictionary<string, string> hints)
        {
            GaxPreconditions.CheckNotNull(query, nameof(query));
            GaxPreconditions.CheckNotNull(hints, nameof(hints));
            var builder = new StringBuilder($"{SpannerQueryHintInterceptor.SpannerQueryHintsPrefix}");
            return query.SetComment(SpannerQueryHintInterceptor.AppendTableHintsString(builder, hints));
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
        public static IQuery SetStatementAndTableHints(this IQuery query, string statementHint, Dictionary<string, string> tableHints)
        {
            GaxPreconditions.CheckNotNull(query, nameof(query));
            GaxPreconditions.CheckNotNullOrEmpty(statementHint, nameof(statementHint));
            GaxPreconditions.CheckNotNull(tableHints, nameof(tableHints));
            var builder = new StringBuilder(CreateStatementHintString(statementHint)).Append('\n');
            return query.SetComment(SpannerQueryHintInterceptor.AppendTableHintsString(builder, tableHints));
        }
    }
}