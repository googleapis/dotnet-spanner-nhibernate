using Google.Cloud.Spanner.Data;

namespace Google.Cloud.Spanner.NHibernate
{
    public interface ISpannerType
    {
        SpannerDbType GetSpannerDbType();
    }
}