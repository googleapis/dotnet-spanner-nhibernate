/* Copyright 2021 Google LLC
* 
* Licensed under the Apache License, Version 2.0 (the "License")
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     https://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

CREATE TABLE Singers (
  SingerId  STRING(36) NOT NULL,
  FirstName STRING(200),
  LastName  STRING(200) NOT NULL,
) PRIMARY KEY (SingerId);

CREATE TABLE Albums (
  SingerId STRING(36) NOT NULL,
  AlbumId  STRING(36) NOT NULL,
  Title    STRING(100) NOT NULL,
) PRIMARY KEY (SingerId, AlbumId), INTERLEAVE IN PARENT Singers;

CREATE TABLE Tracks (
  SingerId STRING(36) NOT NULL,
  AlbumId  STRING(36) NOT NULL,
  TrackId  STRING(36) NOT NULL,
  Title    STRING(200) NOT NULL,
) PRIMARY KEY (SingerId, AlbumId, TrackId), INTERLEAVE IN PARENT Albums;
