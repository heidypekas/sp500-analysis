/*
   Creación de base de datos
*/
CREATE DATABASE S&P_500;
USE S&P_500;

/*
   Creación de tablas
*/
CREATE TABLE [dbo].[Companies] (
    [Date]   DATE        NOT NULL,
    [Symbol] VARCHAR (5) NOT NULL,
    [Close]  FLOAT (53)  NULL,
    CONSTRAINT [PK_Companies] PRIMARY KEY CLUSTERED ([Date] ASC, [Symbol] ASC),
    CONSTRAINT [FK_Companies_Profiels] FOREIGN KEY ([Symbol]) REFERENCES [dbo].[CompanyProfiles] ([Symbol])
);

CREATE TABLE [dbo].[CompanyProfiles] (
    [Symbol]       VARCHAR (5)   NOT NULL,
    [Company]      VARCHAR (200) NULL,
    [Sector]       VARCHAR (200) NULL,
    [Headquarters] VARCHAR (200) NULL,
    [Fundada]      VARCHAR (50)  NULL,
    CONSTRAINT [PK_CompanyProfiles] PRIMARY KEY CLUSTERED ([Symbol] ASC)
);

select * from Companies;

select * from CompanyProfiles;

/*
DELETE from Companies;
DELETE from CompanyProfiles;
*/
