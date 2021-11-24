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

using NHibernate;
using NHibernate.Dialect.Function;
using System.Text.RegularExpressions;

namespace Google.Cloud.Spanner.NHibernate.Functions
{
    /// <summary>
    /// Server side function mapping for DateTime properties.
    /// </summary>
    public class SpannerExtractFunction : SQLFunctionTemplate, IFunctionGrammar
    {
        internal SpannerExtractFunction() : base(NHibernateUtil.Int32, "extract(?1 ?2 AT TIME ZONE 'UTC')")
        {
        }

        bool IFunctionGrammar.IsSeparator(string token) => false;

        bool IFunctionGrammar.IsKnownArgument(string token) =>
            Regex.IsMatch(token, "DATE|ISOYEAR|YEAR|QUARTER|MONTH|WEEK|ISOWEEK|DAYOFYEAR|DAY|DAYOFWEEK|HOUR|MINUTE|SECOND|MILLISECOND|MICROSECOND|NANOSECOND|FROM",
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }
}