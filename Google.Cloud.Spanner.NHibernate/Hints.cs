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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// Builder class for <see cref="Hints"/>
    /// </summary>
    public sealed class HintsBuilder
    {
        internal HintsBuilder()
        {
        }
        
        internal string StatementHints { get; private set; } = "";

        internal Dictionary<string, string> TableHints { get; } = new Dictionary<string, string>();

        internal Dictionary<string, string> JoinHints { get; } = new Dictionary<string, string>();

        /// <summary>
        /// Sets the statement hint to use for the query.
        /// See https://cloud.google.com/spanner/docs/reference/standard-sql/query-syntax#statement_hints for more
        /// information on statement hints.
        /// </summary>
        /// <param name="hint">The hint to add in the format `@{hint_key=hint_value[, hint_key=hint_value, ...]}`</param>
        /// <returns>The builder</returns>
        public HintsBuilder SetStatementHint(string hint)
        {
            GaxPreconditions.CheckNotNullOrEmpty(hint, nameof(hint));
            GaxPreconditions.CheckArgument(hint.StartsWith("@{") && hint.EndsWith("}"), nameof(hint), "A statement hint must be given in the format `@{hint_key=hint_value[, hint_key=hint_value[, ...]]}");
            StatementHints = string.IsNullOrEmpty(StatementHints) ? hint : StatementHints + ", " + hint;
            return this;
        }

        /// <summary>
        /// Sets the table hint to use for a table.
        /// See https://cloud.google.com/spanner/docs/reference/standard-sql/query-syntax#table_hints for more
        /// information on table hints.
        /// </summary>
        /// <param name="table">The name of the table that the hint should be applied to</param>
        /// <param name="hint">The hint that should be applied to the table in the format `@{hint_key=hint_value[, hint_key=hint_value, ...]}`</param>
        /// <returns>The builder</returns>
        public HintsBuilder SetTableHint(string table, string hint)
        {
            GaxPreconditions.CheckNotNullOrEmpty(table, nameof(table));
            GaxPreconditions.CheckNotNullOrEmpty(hint, nameof(hint));
            GaxPreconditions.CheckArgument(hint.StartsWith("@{") && hint.EndsWith("}"), nameof(hint), "A table hint must be given in the format `@{hint_key=hint_value[, hint_key=hint_value[, ...]]}");
            var tableKey = table.ToLowerInvariant();
            GaxPreconditions.CheckArgument(!TableHints.ContainsKey(tableKey), nameof(hint), $"A table hint has already been set for table {table}");
            TableHints.Add(tableKey, hint);
            return this;
        }

        /// <summary>
        /// Sets the join hint to use for a table.
        /// See https://cloud.google.com/spanner/docs/reference/standard-sql/query-syntax#join_hints for more
        /// information on join hints.
        /// </summary>
        /// <param name="table">The name of the table that the hint should be applied to</param>
        /// <param name="hint">The hint that should be applied to the table in the format `@{hint_key=hint_value[, hint_key=hint_value, ...]}`</param>
        /// <returns>The builder</returns>
        public HintsBuilder SetJoinHint(string table, string hint)
        {
            GaxPreconditions.CheckNotNullOrEmpty(table, nameof(table));
            GaxPreconditions.CheckNotNullOrEmpty(hint, nameof(hint));
            GaxPreconditions.CheckArgument(hint.StartsWith("@{") && hint.EndsWith("}"), nameof(hint), "A join hint must be given in the format `@{hint_key=hint_value[, hint_key=hint_value[, ...]]}");
            var tableKey = table.ToLowerInvariant();
            GaxPreconditions.CheckArgument(!JoinHints.ContainsKey(tableKey), nameof(hint), $"A join hint has already been set for table {table}");
            JoinHints.Add(tableKey, hint);
            return this;
        }

        /// <summary>
        /// Builds an immutable Hints instance from this builder.
        /// </summary>
        /// <returns>An immutable Hints instance</returns>
        public Hints Build() => new Hints(this);
    }
    
    public sealed class Hints
    {
        /// <summary>
        /// Creates a builder for a <see cref="Hints"/> instance. Use this method to create a combination of different
        /// hint types.
        /// </summary>
        /// <returns>A builder</returns>
        public static HintsBuilder NewBuilder() => new HintsBuilder();

        /// <summary>
        /// Create a <see cref="Hints"/> instance containing only a statement hint.
        /// See https://cloud.google.com/spanner/docs/reference/standard-sql/query-syntax#statement_hints for more
        /// information on statement hints.
        /// </summary>
        /// <param name="hint">The hint to add in the format `@{hint_key=hint_value[, hint_key=hint_value, ...]}`</param>
        /// <returns>An immutable <see cref="Hints"/> instance that can be applied to a query</returns>
        public static Hints StatementHint(string hint) => NewBuilder().SetStatementHint(hint).Build();

        /// <summary>
        /// Create a <see cref="Hints"/> instance containing only a table hint.
        /// See https://cloud.google.com/spanner/docs/reference/standard-sql/query-syntax#table_hints for more
        /// information on table hints.
        /// </summary>
        /// <param name="table">The name of the table that the hint should be applied to</param>
        /// <param name="hint">The hint that should be applied to the table in the format `@{hint_key=hint_value[, hint_key=hint_value, ...]}`</param>
        /// <returns>An immutable <see cref="Hints"/> instance that can be applied to a query</returns>
        public static Hints TableHint(string table, string hint) => NewBuilder().SetTableHint(table, hint).Build();

        /// <summary>
        /// Create a <see cref="Hints"/> instance containing only a join hint.
        /// See https://cloud.google.com/spanner/docs/reference/standard-sql/query-syntax#join_hints for more
        /// information on join hints.
        /// </summary>
        /// <param name="table">The name of the table that the hint should be applied to</param>
        /// <param name="hint">The hint that should be applied to the table in the format `@{hint_key=hint_value[, hint_key=hint_value, ...]}`</param>
        /// <returns>The builder</returns>
        public static Hints JoinHint(string table, string hint) => NewBuilder().SetJoinHint(table, hint).Build();

        /// <summary>
        /// The statement hints in this Hints collection.
        /// </summary>
        public string StatementHints { get; }

        /// <summary>
        /// The table hints in this Hints collection.
        /// </summary>
        public ReadOnlyDictionary<string, string> TableHints { get; }

        /// <summary>
        /// The join hints in this Hints collection.
        /// </summary>
        public ReadOnlyDictionary<string, string> JoinHints { get; }

        internal string CommentString { get; }

        internal Hints(HintsBuilder builder)
        {
            StatementHints = builder.StatementHints;
            TableHints = new ReadOnlyDictionary<string, string>(builder.TableHints);
            JoinHints = new ReadOnlyDictionary<string, string>(builder.JoinHints);
            CommentString = BuildCommentString();
        }

        private string BuildCommentString()
        {
            var builder = new StringBuilder(SpannerQueryHintInterceptor.SpannerQueryHintsPrefix);
            if (!string.IsNullOrEmpty(StatementHints))
            {
                builder.Append(SpannerQueryHintInterceptor.SpannerStatementHintPrefix).Append(StatementHints).Append(" \n");
            }
            builder = SpannerQueryHintInterceptor.AppendTableHintsString(builder, TableHints);
            builder = SpannerQueryHintInterceptor.AppendJoinHintsString(builder, JoinHints);
            return builder.ToString();
        }
    }
}