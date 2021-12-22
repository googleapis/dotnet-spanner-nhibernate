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
    /// Foreign key name that instructs the Cloud Spanner NHibernate driver to treat a foreign key definition as an
    /// `INTERLEAVE IN PARENT` clause instead. Set this as the name of a foreign key to make sure the driver will
    /// generate a parent-child table instead of a foreign key constraint.
    ///
    /// Example:
    /// <code>
    /// ComponentAsId(x => x.TrackIdentifier, m =>
    /// {
    ///     m.ManyToOne(id => id.Album, albumMapper =>
    ///     {
    ///         albumMapper.Column(c =>
    ///         {
    ///             c.Name("Id");
    ///             c.NotNullable(true);
    ///             c.Length(36);
    ///         });
    ///         albumMapper.ForeignKey(InterleavedTableForeignKey.InterleaveInParent);
    ///     });
    /// });
    /// m.Property(id => id.TrackNumber, m => m.NotNullable(true));
    /// </code>
    /// </summary>
    public static class InterleavedTableForeignKey
    {
        public const string InterleaveInParent = "INTERLEAVE IN PARENT";
    }
}