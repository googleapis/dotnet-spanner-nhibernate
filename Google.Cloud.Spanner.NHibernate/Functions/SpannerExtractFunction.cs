using NHibernate;
using NHibernate.Dialect.Function;
using System.Text.RegularExpressions;

namespace Google.Cloud.Spanner.NHibernate.Functions
{
    public class SpannerExtractFunction : SQLFunctionTemplate, IFunctionGrammar
    {
        internal SpannerExtractFunction() : base(NHibernateUtil.Int32, "extract(?1 ?2 AT TIME ZONE 'UTC')")
        {
        }

        bool IFunctionGrammar.IsSeparator(string token)
        {
            return false;
        }

        bool IFunctionGrammar.IsKnownArgument(string token)
        {
            return Regex.IsMatch(token, "DATE|ISOYEAR|YEAR|QUARTER|MONTH|WEEK|ISOWEEK|DAYOFYEAR|DAY|DAYOFWEEK|HOUR|MINUTE|SECOND|MILLISECOND|MICROSECOND|NANOSECOND|FROM",
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        }
    }
}