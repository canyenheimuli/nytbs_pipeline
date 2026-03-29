-- ** DB Setup Script (creates the initial tables and metadata) **
-- * Table Creation *
-- Weekly Lists Table
IF OBJECT_ID(N'dbo.weekly_lists', N'U') IS NULL
  BEGIN
    CREATE TABLE dbo.weekly_lists (
      list_id SMALLINT NOT NULL,
      book_rank SMALLINT NOT NULL,
      isbn13 VARCHAR(20) NOT NULL,	
      rank_last_period SMALLINT NOT NULL,
      periods_on_list SMALLINT NOT NULL, 
      pub_date DATE NOT NULL,
      pub_date_year SMALLINT NULL,
      pub_date_month SMALLINT NULL,
      retrieval_date DATE NOT NULL,
      PRIMARY KEY (list_id, book_rank, pub_date)
    );
  END
GO

-- Monthly Lists Table (PRODUCTION)
IF OBJECT_ID(N'dbo.monthly_lists', N'U') IS NULL
  BEGIN
    CREATE TABLE dbo.monthly_lists (
      list_id SMALLINT NOT NULL,
      book_rank SMALLINT NOT NULL,
      isbn13 VARCHAR(20) NOT NULL,	
      rank_last_period SMALLINT NOT NULL,
      periods_on_list SMALLINT NOT NULL, 
      pub_date DATE NOT NULL,
      pub_date_year SMALLINT NULL,
      pub_date_month SMALLINT NULL,
      retrieval_date DATE NOT NULL,
      PRIMARY KEY (list_id, book_rank, pub_date)
    );
  END
GO

-- Monthly Lists Table (STAGING)
IF OBJECT_ID(N'dbo.monthly_lists_staged', N'U') IS NULL
  BEGIN
    CREATE TABLE dbo.monthly_lists_staged (
      list_id SMALLINT NOT NULL,
      book_rank SMALLINT NOT NULL,
      isbn13 VARCHAR(20) NOT NULL,	
      rank_last_period SMALLINT NOT NULL,
      periods_on_list SMALLINT NOT NULL, 
      pub_date DATE NOT NULL,
      pub_date_year SMALLINT NULL,
      pub_date_month SMALLINT NULL,
      retrieval_date DATE NOT NULL,
      PRIMARY KEY (list_id, book_rank, pub_date)
    );
  END
GO

-- List Names Table
IF OBJECT_ID(N'dbo.list_names', N'U') IS NULL
  BEGIN
    CREATE TABLE dbo.list_names (
      list_id SMALLINT NOT NULL,
      list_name VARCHAR(100) NOT NULL,
      PRIMARY KEY (list_id)
    );
  END
GO

-- Books Table
IF OBJECT_ID(N'dbo.books', N'U') IS NULL
  BEGIN
    CREATE TABLE dbo.books (
      isbn13 VARCHAR(20) NOT NULL,	
      title VARCHAR(100) NOT NULL,	
      author VARCHAR(100) NOT NULL, 
      book_descr VARCHAR(MAX) NULL, 	
      publisher VARCHAR(100) NULL,
      book_image VARCHAR(MAX) NULL,
      PRIMARY KEY (isbn13)
    );
  END
GO

-- * Constraints *
-- Weeklies to List Names
BEGIN 
  ALTER TABLE dbo.weekly_lists
  ADD CONSTRAINT FK_weeklies_lists
  FOREIGN KEY (list_id)
  REFERENCES dbo.list_names (list_id);
END
GO

-- Weeklies to Books
BEGIN 
  ALTER TABLE dbo.weekly_lists
  ADD CONSTRAINT FK_weeklies_books
  FOREIGN KEY (isbn13)
  REFERENCES dbo.books (isbn13);
END
GO

-- Monthlies to List Names
BEGIN 
  ALTER TABLE dbo.monthly_lists
  ADD CONSTRAINT FK_monthlies_lists
  FOREIGN KEY (list_id)
  REFERENCES dbo.list_names (list_id);
END
GO

-- Monthlies to Books
BEGIN 
  ALTER TABLE dbo.monthly_lists
  ADD CONSTRAINT FK_monthlies_books
  FOREIGN KEY (isbn13)
  REFERENCES dbo.books (isbn13);
END
GO

-- Staging Monthlies to List Names
BEGIN 
  ALTER TABLE dbo.monthly_lists_staged
  ADD CONSTRAINT FK_monthlies_staged_lists
  FOREIGN KEY (list_id)
  REFERENCES dbo.list_names (list_id);
END
GO

-- Staging Monthlies to Books
BEGIN 
  ALTER TABLE dbo.monthly_lists_staged
  ADD CONSTRAINT FK_monthlies_staged_books
  FOREIGN KEY (isbn13)
  REFERENCES dbo.books (isbn13);
END
GO
