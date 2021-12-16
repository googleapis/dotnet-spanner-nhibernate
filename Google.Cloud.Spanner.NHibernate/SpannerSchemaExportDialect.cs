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

        // Cloud Spanner does not support unique constraints, but this makes sure that NHibernate tries to generate
        // the unique constraint in the table itself. The Spanner SchemaExport then prohibits the actual generation of
        // the constraint by simulating that the constraint contains a nullable column, and that it does not support
        // nullable columns in unique constraints.
        public override bool SupportsUniqueConstraintInCreateAlterTable => true;

        // Cloud Spanner does support null in unique indexes, but this prevents NHibernate from generating unique
        // constraints, which are not supported by Cloud Spanner.
        public override bool SupportsNullInUnique => false;
        
        public override string IdentityColumnString => "NOT NULL";

        public override string GetTableComment(string comment) => $" {comment}";

        public override string GetAddForeignKeyConstraintString(string constraintName, string[] foreignKey,
            string referencedTable, string[] primaryKey, bool referencesPrimaryKey) =>
            Equals("INTERLEAVE IN PARENT", constraintName)
                ? ""
                : base.GetAddForeignKeyConstraintString(constraintName, foreignKey, referencedTable, primaryKey,
                    referencesPrimaryKey);

        public override string GetIfExistsDropConstraint(string catalog, string schema, string table, string name) =>
            Equals("INTERLEAVE IN PARENT", name)
                ? "/* This statement is skipped as it is a many-to-one relationship that is defined by an INTERLEAVE IN PARENT relationship"
                : base.GetIfExistsDropConstraint(catalog, schema, table, name);

        public override string GetIfExistsDropConstraintEnd(string catalog, string schema, string table, string name) =>
            Equals("INTERLEAVE IN PARENT", name)
                ? "*/"
                : base.GetIfExistsDropConstraintEnd(catalog, schema, table, name);
    }
}