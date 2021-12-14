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

using System.Collections.Generic;
using NHibernate.Hql.Ast.ANTLR.Tree;
using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;

namespace Google.Cloud.Spanner.NHibernate.IntegrationTests.SampleEntities
{
    public class AlbumWithVersion
    {
        public virtual string Id { get; set; }
        public virtual int Version { get; set; }
        public virtual SingerWithVersion Singer { get; set; }
        public virtual string Title { get; set; }
        public virtual SpannerDate ReleaseDate { get; set; }
    }

    public class AlbumWithVersionMapping : ClassMapping<AlbumWithVersion>
    {
        public AlbumWithVersionMapping()
        {
            Persister<SpannerSingleTableEntityPersister>();
            DynamicUpdate(true);
            Table("AlbumsWithVersion");
            Version(x => x.Version, mapper => mapper.Generated(VersionGeneration.Never));
            Id(x => x.Id, m =>
            {
                m.Generator(new UUIDHexGeneratorDef());
                m.Length(36);
            });
            ManyToOne(x => x.Singer, m =>
            {
                m.NotNullable(true);
                m.Column(c => c.Length(36));
                m.ForeignKey("FK_Albums_Singers_WithVersion");
                m.UniqueKey("Idx_AlbumsWithVersions_Title");
            });
            Property(x => x.Title, m =>
            {
                m.NotNullable(true);
                m.Length(100);
                m.UniqueKey("Idx_AlbumsWithVersions_Title");
            });
            Property(x => x.ReleaseDate);
        }
    }
}