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
  Id        STRING(36) NOT NULL,
  FirstName STRING(200),
  LastName  STRING(200) NOT NULL,
  FullName  STRING(400) NOT NULL AS (COALESCE(FirstName || ' ', '') || LastName) STORED,
  BirthDate DATE,
  Picture   BYTES(MAX),
  Version   INT64 NOT NULL,
  CreatedAt        TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  LastUpdatedAt    TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (Id);

CREATE INDEX Idx_Singers_FullName ON Singers (FullName);

CREATE TABLE Albums (
  Id          STRING(36) NOT NULL,
  SingerId    STRING(36) NOT NULL,
  Title       STRING(100) NOT NULL,
  ReleaseDate DATE,
  Version     INT64 NOT NULL,
  CONSTRAINT FK_Albums_Singers FOREIGN KEY (SingerId) REFERENCES Singers (Id),
) PRIMARY KEY (Id);

CREATE TABLE Tracks (
  Id              STRING(36) NOT NULL,
  AlbumId         STRING(36) NOT NULL,
  Title           STRING(200) NOT NULL,
  Duration        NUMERIC,
  LyricsLanguages ARRAY<STRING(2)>,
  Lyrics          ARRAY<STRING(MAX)>,
  Version         INT64 NOT NULL,
  CONSTRAINT FK_Tracks_Albums FOREIGN KEY (AlbumId) REFERENCES Albums (Id),
) PRIMARY KEY (Id);

CREATE UNIQUE INDEX Idx_Tracks_AlbumId_Title ON Tracks (AlbumId, Title);

CREATE TABLE Venues (
  Id        STRING(36) NOT NULL,
  Code      STRING(10) NOT NULL,
  Name      STRING(100),
  Active    BOOL NOT NULL,
  Version   INT64 NOT NULL,
) PRIMARY KEY (Id);

CREATE TABLE Concerts (
  Id        STRING(36) NOT NULL,
  VenueId   STRING(36) NOT NULL,
  StartTime TIMESTAMP NOT NULL,
  SingerId  STRING(36) NOT NULL,
  Title     STRING(200),
  Version   INT64 NOT NULL,
  CONSTRAINT FK_Concerts_Venues FOREIGN KEY (VenueId) REFERENCES Venues (Id),
  CONSTRAINT FK_Concerts_Singers FOREIGN KEY (SingerId) REFERENCES Singers (Id),
) PRIMARY KEY (Id);

CREATE TABLE Performances (
  Id               STRING(36) NOT NULL,
  ConcertId        STRING(36) NOT NULL,
  TrackId          STRING(36) NOT NULL,
  StartTime        TIMESTAMP,
  Rating           FLOAT64,
  CreatedAt        TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  LastUpdatedAt    TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  Version          INT64 NOT NULL,
  CONSTRAINT FK_Performances_Concerts FOREIGN KEY (ConcertId) REFERENCES Concerts (Id),
  CONSTRAINT FK_Performances_Tracks FOREIGN KEY (TrackId) REFERENCES Tracks (Id),
) PRIMARY KEY (Id);
