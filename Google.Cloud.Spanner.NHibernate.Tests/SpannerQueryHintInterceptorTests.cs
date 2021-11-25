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

using NHibernate.SqlCommand;
using Xunit;

namespace Google.Cloud.Spanner.NHibernate.Tests
{
    public class SpannerQueryHintInterceptorTests
    {
        [InlineData("SELECT * FROM Singers WHERE FullName LIKE @p0", "SELECT * FROM Singers WHERE FullName LIKE @p0")]
        [InlineData("/*some random comment*/SELECT * FROM Singers WHERE FullName LIKE @p0", "/*some random comment*/SELECT * FROM Singers WHERE FullName LIKE @p0")]
        [InlineData("/* NHIBERNATE_HINTS:\n*/SELECT * FROM Singers WHERE FullName LIKE @p0", "SELECT * FROM Singers WHERE FullName LIKE @p0")]
        [InlineData("/* NHIBERNATE_HINTS:\nSTATEMENT_HINT: @{OPTIMIZER_VERSION=1}*/SELECT * FROM Singers WHERE FullName LIKE @p0", "@{OPTIMIZER_VERSION=1}SELECT * FROM Singers WHERE FullName LIKE @p0")]
        [InlineData("/* NHIBERNATE_HINTS:\nSTATEMENT_HINT: @{OPTIMIZER_VERSION=1}\nSTATEMENT_HINT: @{USE_ADDITIONAL_PARALLELISM=TRUE}\n*/SELECT * FROM Singers WHERE FullName LIKE @p0", "@{USE_ADDITIONAL_PARALLELISM=TRUE}@{OPTIMIZER_VERSION=1}SELECT * FROM Singers WHERE FullName LIKE @p0")]
        [Theory]
        public void StatementHints(string input, string output)
        {
            var interceptor = new SpannerQueryHintInterceptor();
            Assert.Equal(output, interceptor.OnPrepareStatement(new SqlString(input)).ToString());
        }
        
        [InlineData("SELECT * FROM Singers WHERE FullName LIKE @p0", "SELECT * FROM Singers WHERE FullName LIKE @p0")]
        [InlineData("/*some random comment*/SELECT * FROM Singers WHERE FullName LIKE @p0", "/*some random comment*/SELECT * FROM Singers WHERE FullName LIKE @p0")]
        [InlineData("/* NHIBERNATE_HINTS:\n*/SELECT * FROM Singers WHERE FullName LIKE @p0", "SELECT * FROM Singers WHERE FullName LIKE @p0")]
        [InlineData("/* NHIBERNATE_HINTS:\nTABLE_HINT: `Singers`@{FORCE_INDEX=Idx_Singers_FullName}*/SELECT * FROM Singers WHERE FullName LIKE @p0", "SELECT * FROM Singers@{FORCE_INDEX=Idx_Singers_FullName} WHERE FullName LIKE @p0")]
        [InlineData("/* NHIBERNATE_HINTS:\nTABLE_HINT: `Singers`@{FORCE_INDEX=Idx_Singers_FullName}\nTABLE_HINT: `Albums`@{FORCE_INDEX=Idx_Albums_Title}*/SELECT * FROM Singers LEFT OUTER JOIN Albums ON Albums.SingerId=Singers.SingerId WHERE FullName LIKE @p0", "SELECT * FROM Singers@{FORCE_INDEX=Idx_Singers_FullName} LEFT OUTER JOIN Albums@{FORCE_INDEX=Idx_Albums_Title} ON Albums.SingerId=Singers.SingerId WHERE FullName LIKE @p0")]
        [InlineData("/* NHIBERNATE_HINTS:\nTABLE_HINT: `Singers`@{FORCE_INDEX=Idx_Singers_FullName}\nTABLE_HINT: `Albums`@{FORCE_INDEX=Idx_Albums_Title}\nTABLE_HINT: `Tracks`@{FORCE_INDEX=Idx_Tracks_Title}*/SELECT * FROM Singers LEFT OUTER JOIN Albums ON Albums.SingerId=Singers.SingerId JOIN Tracks ON Tracks.AlbumId=Albums.AlbumId WHERE FullName LIKE @p0", "SELECT * FROM Singers@{FORCE_INDEX=Idx_Singers_FullName} LEFT OUTER JOIN Albums@{FORCE_INDEX=Idx_Albums_Title} ON Albums.SingerId=Singers.SingerId JOIN Tracks@{FORCE_INDEX=Idx_Tracks_Title} ON Tracks.AlbumId=Albums.AlbumId WHERE FullName LIKE @p0")]
        [InlineData("/* NHIBERNATE_HINTS:\nTABLE_HINT: `Singer`@{FORCE_INDEX=Idx_Singers_FullName}*/SELECT * FROM Singers WHERE FullName LIKE @p0", "SELECT * FROM Singers WHERE FullName LIKE @p0")]
        [Theory]
        public void TableHints(string input, string output)
        {
            var interceptor = new SpannerQueryHintInterceptor();
            Assert.Equal(output, interceptor.OnPrepareStatement(new SqlString(input)).ToString());
        }

        [InlineData("/* NHIBERNATE_HINTS:\nSTATEMENT_HINT: @{OPTIMIZER_VERSION=1}\nSTATEMENT_HINT: @{USE_ADDITIONAL_PARALLELISM=TRUE}\nTABLE_HINT: `Singers`@{FORCE_INDEX=Idx_Singers_FullName}\nTABLE_HINT: `Albums`@{FORCE_INDEX=Idx_Albums_Title}\nTABLE_HINT: `Tracks`@{FORCE_INDEX=Idx_Tracks_Title}*/SELECT * FROM Singers LEFT OUTER JOIN Albums ON Albums.SingerId=Singers.SingerId JOIN Tracks ON Tracks.AlbumId=Albums.AlbumId WHERE FullName LIKE @p0", "@{USE_ADDITIONAL_PARALLELISM=TRUE}@{OPTIMIZER_VERSION=1}SELECT * FROM Singers@{FORCE_INDEX=Idx_Singers_FullName} LEFT OUTER JOIN Albums@{FORCE_INDEX=Idx_Albums_Title} ON Albums.SingerId=Singers.SingerId JOIN Tracks@{FORCE_INDEX=Idx_Tracks_Title} ON Tracks.AlbumId=Albums.AlbumId WHERE FullName LIKE @p0")]
        [Theory]
        public void StatementAndTableHints(string input, string output)
        {
            var interceptor = new SpannerQueryHintInterceptor();
            Assert.Equal(output, interceptor.OnPrepareStatement(new SqlString(input)).ToString());
        }
    }
}