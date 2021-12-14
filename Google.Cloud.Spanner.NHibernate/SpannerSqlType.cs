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
using Google.Cloud.Spanner.Data;
using NHibernate.SqlTypes;

namespace Google.Cloud.Spanner.NHibernate
{
    public class SpannerSqlType : SqlType
    {
        public SpannerDbType SpannerDbType { get; }
        
        public SpannerDbType ArrayElementType { get; }
        
        public SpannerSqlType(SpannerDbType spannerDbType) : base(spannerDbType.DbType)
        {
            SpannerDbType = GaxPreconditions.CheckNotNull(spannerDbType, nameof(spannerDbType));
        }
        
        public SpannerSqlType(SpannerDbType spannerDbType, SpannerDbType arrayElementType) : base(spannerDbType.DbType)
        {
            SpannerDbType = GaxPreconditions.CheckNotNull(spannerDbType, nameof(spannerDbType));
            ArrayElementType = GaxPreconditions.CheckNotNull(arrayElementType, nameof(arrayElementType));
        }
    }
}