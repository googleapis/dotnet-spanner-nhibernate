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
using NHibernate.Linq;
using System.Linq;

namespace Google.Cloud.Spanner.NHibernate
{
    /// <summary>
    /// Extension methods for NHibernate <see cref="IQueryable"/> when working with Google Cloud Spanner.
    /// All methods in this class assume that the given <see cref="IQueryable"/> is used on a Google Cloud Spanner
    /// connection.
    /// </summary>
    public static class IQueryableExtensions
    {
        /// <summary>
        /// Sets the hints to use for this query.
        /// </summary>
        /// <param name="query">The query to set the hints on</param>
        /// <param name="hints">The hints to set</param>
        /// <typeparam name="T"></typeparam>
        /// <returns>The query with the hints</returns>
        public static IQueryable<T> SetHints<T>(this IQueryable<T> query, Hints hints)
        {
            GaxPreconditions.CheckNotNull(hints, nameof(hints));
            return query.WithOptions(o => o.SetComment(hints.CommentString));
        }
    }
}