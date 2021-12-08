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

namespace Google.Cloud.Spanner.NHibernate
{
    internal class SpannerSchemaExportDialect : SpannerDialect
    {
        // Cloud Spanner does not support any identity columns, but this allows us to skip the primary key generation
        // during schema export.
        public override bool GenerateTablePrimaryKeyConstraintForIdentityColumn => false;
        
        public override string IdentityColumnString => "NOT NULL";

        public override string GetTableComment(string comment) => $" {comment}";
    }
}