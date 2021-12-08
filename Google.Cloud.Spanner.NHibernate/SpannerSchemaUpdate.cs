using NHibernate.Cfg;
using NHibernate.Tool.hbm2ddl;
using System.Collections.Generic;

namespace Google.Cloud.Spanner.NHibernate
{
    public class SpannerSchemaUpdate : SchemaUpdate
    {
        public SpannerSchemaUpdate(Configuration cfg) :
            this(cfg, new Dictionary<string, string> {{Environment.Dialect, typeof(SpannerSchemaExportDialect).AssemblyQualifiedName}})
        {
        }

        public SpannerSchemaUpdate(Configuration cfg, IDictionary<string, string> configProperties) : base(cfg, configProperties)
        {
        }

        public SpannerSchemaUpdate(Configuration cfg, Settings settings) : base(cfg, settings)
        {
        }
    }
}