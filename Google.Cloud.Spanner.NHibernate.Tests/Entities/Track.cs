using NHibernate.Mapping.ByCode.Conformist;

namespace Google.Cloud.Spanner.NHibernate.Tests.Entities
{
    public class Track
    {
        public Track()
        {
        }
        
        public virtual long TrackId { get; set; }
        public virtual string Title { get; set; }
        public virtual Album Album { get; set; }
    }

    public class TrackMapping : ClassMapping<Track>
    {
        public TrackMapping()
        {
            Id(x => x.TrackId);
            Property(x => x.Title);
            ManyToOne(x => x.Album, m => m.Column("AlbumId"));
        }
    }
}