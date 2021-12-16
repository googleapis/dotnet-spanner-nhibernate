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

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// Sql type definition that can be used with the Cloud Spanner NHibernate dialect to indicate that a column should
    /// allow the commit timestamp to be written to the column. That is; the following option will be added to the
    /// column: <code>OPTIONS (allow_commit_timestamp=true)</code>
    ///
    /// Usage only to set the option <code>allow_commit_timestamp=true</code>:
    /// <code>
    /// Property(x => x.LastUpdated, m => m.Column(c => c.SqlType(SpannerCommitTimestampSqlType.Instance)));
    /// </code>
    ///
    /// Example usage for automatically assigning the commit timestamp to a property on insert/update:
    /// <code>
    /// Persister&lt;SpannerSingleTableEntityPersister&gt;();
    /// Property(x => x.ColCommitTs, m =>
    /// {
    ///     // This ensures that `OPTIONS (allow_commit_timestamp=true)` is added to the column definition.
    ///     m.Column(c => c.SqlType(SpannerCommitTimestampSqlType.Instance));
    ///
    ///     // The following ensures that the SpannerSingleTableWithFixedValuesEntityPersister will set the column
    ///     // to the default value for both inserts and updates.
    ///     m.Insert(false); // This will prevent Hibernate from assigning a value to the column during inserts.
    ///     m.Update(false); // This will prevent Hibernate from assigning a value to the column during updates.
    ///
    ///     // This will automatically set the value of the column to the commit timestamp of the transaction
    ///     // when the record is updated/inserted.
    ///     m.Column(c => c.Default("PENDING_COMMIT_TIMESTAMP()"));
    /// });
    /// </code>
    /// 
    /// </summary>
    public static class SpannerCommitTimestampSqlType
    {
        public const string NullableInstance = "TIMESTAMP OPTIONS (allow_commit_timestamp=true)";
        public const string NotNullInstance = "TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)";
    }
}