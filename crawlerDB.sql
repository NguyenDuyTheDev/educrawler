--------------------------
CREATE TABLE IF NOT EXISTS public."Keyword"
(
    "ID" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( CYCLE INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "Name" character(30) COLLATE pg_catalog."default" NOT NULL,
    "TotalArticles" integer DEFAULT 0,
    "TotalWords" integer DEFAULT 0,
    CONSTRAINT "Keyword_pkey" PRIMARY KEY ("ID")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Keyword"
    OWNER to educrawler_user;

------------------------------
CREATE TABLE IF NOT EXISTS public."SupportedFileType"
(
    "ID" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "Type" character(10) COLLATE pg_catalog."default",
    CONSTRAINT "SupportedFileType_pkey" PRIMARY KEY ("ID")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."SupportedFileType"
    OWNER to educrawler_user;

-------------------------------------
CREATE TABLE IF NOT EXISTS public."File"
(
    "ID" integer NOT NULL,
    "Name" text COLLATE pg_catalog."default",
    "Location" text COLLATE pg_catalog."default",
    "Type" text COLLATE pg_catalog."default",
    "Source" text COLLATE pg_catalog."default",
    CONSTRAINT "File_pkey" PRIMARY KEY ("ID")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."File"
    OWNER to educrawler_user;

----------------------------------
CREATE TABLE IF NOT EXISTS public."User"
(
    "ID" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "Username" text COLLATE pg_catalog."default" NOT NULL,
    "Password" text COLLATE pg_catalog."default" NOT NULL,
    "OnlineStatus" boolean DEFAULT false,
    "LastAccess" timestamp without time zone,
    "AccountStatus" text COLLATE pg_catalog."default" DEFAULT 'Good'::text,
    "FullName" text COLLATE pg_catalog."default",
    "Phone" text COLLATE pg_catalog."default",
    "Mail" text COLLATE pg_catalog."default",
    "Role" text COLLATE pg_catalog."default" DEFAULT 'User'::text,
    "SystemLanguage" text COLLATE pg_catalog."default" DEFAULT 'Vietnamese'::text,
    "SystemMode" text COLLATE pg_catalog."default" DEFAULT 'Light'::text,
    CONSTRAINT "User_pkey" PRIMARY KEY ("ID")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."User"
    OWNER to educrawler_user;

-------------------------------------
-- Table: public.Spider

-- DROP TABLE IF EXISTS public."Spider";

CREATE TABLE IF NOT EXISTS public."Spider"
(
    "ID" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "Url" text COLLATE pg_catalog."default",
    "Status" text COLLATE pg_catalog."default",
    "CrawlStatus" text COLLATE pg_catalog."default",
    "LastRunDate" timestamp without time zone,
    "LastEndDate" timestamp without time zone,
    "RunTime" integer DEFAULT 0,
    "isBlocked" boolean DEFAULT true,
    "Delay" real DEFAULT 2,
    "GraphDeep" integer DEFAULT 3,
    "MaxThread" integer DEFAULT 8,
    "CreatedByID" integer,
    CONSTRAINT "Spider_pkey" PRIMARY KEY ("ID"),
    CONSTRAINT "Spider_CreatedByID_fkey" FOREIGN KEY ("CreatedByID")
        REFERENCES public."User" ("ID") MATCH SIMPLE
        ON UPDATE SET NULL
        ON DELETE SET NULL
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Spider"
    OWNER to educrawler_user;

---------------------------------------------
-- Table: public.SpiderKeyword

-- DROP TABLE IF EXISTS public."SpiderKeyword";

CREATE TABLE IF NOT EXISTS public."SpiderKeyword"
(
    "KeywordID" integer NOT NULL,
    "SpiderID" integer NOT NULL,
    CONSTRAINT "SpiderKeyword_pkey" PRIMARY KEY ("KeywordID", "SpiderID"),
    CONSTRAINT "SpiderKeyword_KeywordID_fkey" FOREIGN KEY ("KeywordID")
        REFERENCES public."Keyword" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT "SpiderKeyword_SpiderID_fkey" FOREIGN KEY ("SpiderID")
        REFERENCES public."Spider" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."SpiderKeyword"
    OWNER to educrawler_user;

-------------------------------------------
-- Table: public.SpiderSupportedFileType

-- DROP TABLE IF EXISTS public."SpiderSupportedFileType";

CREATE TABLE IF NOT EXISTS public."SpiderSupportedFileType"
(
    "SpiderID" integer NOT NULL,
    "FileTypeID" integer NOT NULL,
    CONSTRAINT "SpiderSupportedFileType_pkey" PRIMARY KEY ("SpiderID", "FileTypeID"),
    CONSTRAINT "FileTypeIDpk" FOREIGN KEY ("FileTypeID")
        REFERENCES public."SupportedFileType" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID,
    CONSTRAINT "SpiderIDpk" FOREIGN KEY ("SpiderID")
        REFERENCES public."Spider" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."SpiderSupportedFileType"
    OWNER to educrawler_user;

-------------------------------------
-- Table: public.CrawlRules

-- DROP TABLE IF EXISTS public."CrawlRules";

CREATE TABLE IF NOT EXISTS public."CrawlRules"
(
    "ID" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "Tag" text COLLATE pg_catalog."default",
    "HTMLClassName" text COLLATE pg_catalog."default",
    "HTMLIDName" text COLLATE pg_catalog."default",
    CONSTRAINT "CrawlRules_pkey" PRIMARY KEY ("ID")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."CrawlRules"
    OWNER to educrawler_user;

-------------------------------------
-- Table: public.WebpageSpider

-- DROP TABLE IF EXISTS public."WebpageSpider";

CREATE TABLE IF NOT EXISTS public."WebpageSpider"
(
    "ID" integer NOT NULL,
    CONSTRAINT "WebpageSpider_pkey" PRIMARY KEY ("ID"),
    CONSTRAINT "IDfk" FOREIGN KEY ("ID")
        REFERENCES public."Spider" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."WebpageSpider"
    OWNER to educrawler_user;

----------------------------------------
-- Table: public.WebpageSpiderCrawlRules

-- DROP TABLE IF EXISTS public."WebpageSpiderCrawlRules";

CREATE TABLE IF NOT EXISTS public."WebpageSpiderCrawlRules"
(
    "SpiderID" integer NOT NULL,
    "CrawlRulesID" integer NOT NULL,
    CONSTRAINT "WebpageSpiderCrawlRules_pkey" PRIMARY KEY ("SpiderID", "CrawlRulesID"),
    CONSTRAINT "CrawlRulesID_fk" FOREIGN KEY ("CrawlRulesID")
        REFERENCES public."CrawlRules" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID,
    CONSTRAINT "SpiderID_fk" FOREIGN KEY ("SpiderID")
        REFERENCES public."WebpageSpider" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."WebpageSpiderCrawlRules"
    OWNER to educrawler_user;

------------------------------------
-- Table: public.WebsiteSpider

-- DROP TABLE IF EXISTS public."WebsiteSpider";

CREATE TABLE IF NOT EXISTS public."WebsiteSpider"
(
    "ID" integer NOT NULL,
    "TotalPage" integer DEFAULT 0,
    "CrawlSuccess" integer DEFAULT 0,
    "CrawlFail" integer DEFAULT 0,
    CONSTRAINT "WebsiteSpider_pkey" PRIMARY KEY ("ID"),
    CONSTRAINT "IDfk" FOREIGN KEY ("ID")
        REFERENCES public."Spider" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."WebsiteSpider"
    OWNER to educrawler_user;

--------------------------------------
-- Table: public.Subfolder

-- DROP TABLE IF EXISTS public."Subfolder";

CREATE TABLE IF NOT EXISTS public."Subfolder"
(
    "SpiderID" integer NOT NULL,
    "SubfolderID" integer NOT NULL,
    "Name" text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT "Subfolder_pkey" PRIMARY KEY ("SpiderID", "SubfolderID"),
    CONSTRAINT "Subfolder_SpiderID_key" UNIQUE ("SpiderID"),
    CONSTRAINT "Subfolder_SubfolderID_key" UNIQUE ("SubfolderID"),
    CONSTRAINT "Subfolder_SpiderID_fkey" FOREIGN KEY ("SpiderID")
        REFERENCES public."WebsiteSpider" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Subfolder"
    OWNER to educrawler_user;

---------------------------------------
-- Table: public.SubfolderCrawlRules

-- DROP TABLE IF EXISTS public."SubfolderCrawlRules";

CREATE TABLE IF NOT EXISTS public."SubfolderCrawlRules"
(
    "SubFolderID" integer NOT NULL,
    "SpiderID" integer NOT NULL,
    "CrawlRulesId" integer NOT NULL,
    CONSTRAINT "SubfolderCrawlRules_pkey" PRIMARY KEY ("SubFolderID", "SpiderID", "CrawlRulesId"),
    CONSTRAINT "CrawlRules_fk" FOREIGN KEY ("CrawlRulesId")
        REFERENCES public."CrawlRules" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID,
    CONSTRAINT "SubfolderCrawlRules_SpiderID_fkey" FOREIGN KEY ("SpiderID")
        REFERENCES public."Subfolder" ("SpiderID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID,
    CONSTRAINT "SubfolderCrawlRules_SubFolderID_fkey" FOREIGN KEY ("SubFolderID")
        REFERENCES public."Subfolder" ("SubfolderID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."SubfolderCrawlRules"
    OWNER to educrawler_user;

-------------------------------------------------
-- Table: public.UrlSearchRules

-- DROP TABLE IF EXISTS public."UrlSearchRules";

CREATE TABLE IF NOT EXISTS public."UrlSearchRules"
(
    "ID" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "HTMLIDName" text COLLATE pg_catalog."default",
    "HTMLClassName" text COLLATE pg_catalog."default",
    "SubFolderID" integer NOT NULL,
    "SpiderID" integer NOT NULL,
    CONSTRAINT "UrlSearchRules_pkey" PRIMARY KEY ("ID", "SubFolderID", "SpiderID"),
    CONSTRAINT "UrlSearchRules_SpiderID_fkey" FOREIGN KEY ("SpiderID")
        REFERENCES public."Subfolder" ("SpiderID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID,
    CONSTRAINT "UrlSearchRules_SubFolderID_fkey" FOREIGN KEY ("SubFolderID")
        REFERENCES public."Subfolder" ("SubfolderID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."UrlSearchRules"
    OWNER to educrawler_user;

-----------------------------------
-- Table: public.CrawlHistory

-- DROP TABLE IF EXISTS public."CrawlHistory";

CREATE TABLE IF NOT EXISTS public."CrawlHistory"
(
    "ID" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "Url" text COLLATE pg_catalog."default",
    "CrawlStatus" text COLLATE pg_catalog."default" DEFAULT CURRENT_TIMESTAMP,
    "RunDate" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "EndDate" timestamp without time zone,
    "RunTime" integer DEFAULT 0,
    "IsBlocked" boolean DEFAULT true,
    "SpiderID" integer,
    CONSTRAINT "CrawlHistory_pkey" PRIMARY KEY ("ID"),
    CONSTRAINT "CrawlHistory_SpiderID_fkey" FOREIGN KEY ("SpiderID")
        REFERENCES public."Spider" ("ID") MATCH SIMPLE
        ON UPDATE SET NULL
        ON DELETE SET NULL
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."CrawlHistory"
    OWNER to educrawler_user;

----------------------------------------
-- Table: public.WebsiteSpiderHistory

-- DROP TABLE IF EXISTS public."WebsiteSpiderHistory";

CREATE TABLE IF NOT EXISTS public."WebsiteSpiderHistory"
(
    "ID" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "TotalPage" integer DEFAULT 0,
    "CrawlSuccess" integer DEFAULT 0,
    "CrawlFail" integer DEFAULT 0,
    CONSTRAINT "WebsiteSpiderHistory_pkey" PRIMARY KEY ("ID"),
    CONSTRAINT "WebsiteSpiderHistory_ID_fkey" FOREIGN KEY ("ID")
        REFERENCES public."CrawlHistory" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."WebsiteSpiderHistory"
    OWNER to educrawler_user;

----------------------------------------
-- Table: public.Article

-- DROP TABLE IF EXISTS public."Article";

CREATE TABLE IF NOT EXISTS public."Article"
(
    "Id" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( CYCLE INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "Domain" text COLLATE pg_catalog."default",
    "Url" text COLLATE pg_catalog."default",
    "FileName" text COLLATE pg_catalog."default",
    "Content" text COLLATE pg_catalog."default",
    "Images" text COLLATE pg_catalog."default",
    "Files" text COLLATE pg_catalog."default",
    "LastUpdate" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "CrawlStatus" text COLLATE pg_catalog."default",
    "Note" text COLLATE pg_catalog."default",
    "SpiderId" integer,
    "Title" text COLLATE pg_catalog."default",
    "FirstCrawlDate" timestamp without time zone,
    CONSTRAINT "Article_pkey" PRIMARY KEY ("Id"),
    CONSTRAINT "Article_SpiderId_fkey" FOREIGN KEY ("SpiderId")
        REFERENCES public."Spider" ("ID") MATCH SIMPLE
        ON UPDATE SET NULL
        ON DELETE SET NULL
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Article"
    OWNER to educrawler_user;

---------------------------------------------------
-- Table: public.ArticleFile

-- DROP TABLE IF EXISTS public."ArticleFile";

CREATE TABLE IF NOT EXISTS public."ArticleFile"
(
    "ArticleID" integer NOT NULL,
    "FileID" integer NOT NULL,
    CONSTRAINT "ArticleFile_pkey" PRIMARY KEY ("FileID", "ArticleID"),
    CONSTRAINT "ArticleFile_ArticleID_fkey" FOREIGN KEY ("ArticleID")
        REFERENCES public."Article" ("Id") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT "ArticleFile_FileID_fkey" FOREIGN KEY ("FileID")
        REFERENCES public."File" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."ArticleFile"
    OWNER to educrawler_user;

-------------------------------------------------
-- Table: public.ArticleKeyword

-- DROP TABLE IF EXISTS public."ArticleKeyword";

CREATE TABLE IF NOT EXISTS public."ArticleKeyword"
(
    "KeywordID" integer NOT NULL,
    "ArticleID" integer NOT NULL,
    "TotalWords" integer DEFAULT 0,
    CONSTRAINT "ArticleKeyword_pkey" PRIMARY KEY ("KeywordID", "ArticleID"),
    CONSTRAINT "ArticleKeyword_ArticleID_fkey" FOREIGN KEY ("ArticleID")
        REFERENCES public."Article" ("Id") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID,
    CONSTRAINT "ArticleKeyword_KeywordID_fkey" FOREIGN KEY ("KeywordID")
        REFERENCES public."Keyword" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."ArticleKeyword"
    OWNER to educrawler_user;

----------------------------------------------
-- Table: public.PostprocessingArticle

-- DROP TABLE IF EXISTS public."PostprocessingArticle";

CREATE TABLE IF NOT EXISTS public."PostprocessingArticle"
(
    "ID" integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "Title" text COLLATE pg_catalog."default",
    "Domain" text COLLATE pg_catalog."default",
    "Url" text COLLATE pg_catalog."default",
    "Content" text COLLATE pg_catalog."default",
    "LastUpdate" timestamp without time zone,
    CONSTRAINT "PostprocessingArticle_pkey" PRIMARY KEY ("ID")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."PostprocessingArticle"
    OWNER to educrawler_user;

-----------------------------------------------------
-- Table: public.PostprocessingArticleFile

-- DROP TABLE IF EXISTS public."PostprocessingArticleFile";

CREATE TABLE IF NOT EXISTS public."PostprocessingArticleFile"
(
    "PostprocessingArticleID" integer NOT NULL,
    "FileID" integer NOT NULL,
    CONSTRAINT "PostprocessingArticleFile_pkey" PRIMARY KEY ("PostprocessingArticleID", "FileID"),
    CONSTRAINT "PostprocessingArticleFile_FileID_fkey" FOREIGN KEY ("FileID")
        REFERENCES public."File" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT "PostprocessingArticleFile_PostprocessingArticleID_fkey" FOREIGN KEY ("PostprocessingArticleID")
        REFERENCES public."PostprocessingArticle" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."PostprocessingArticleFile"
    OWNER to educrawler_user;

---------------------------------------------------------
-- Table: public.PostprocessingArticleKeyword

-- DROP TABLE IF EXISTS public."PostprocessingArticleKeyword";

CREATE TABLE IF NOT EXISTS public."PostprocessingArticleKeyword"
(
    "KeywordID" integer NOT NULL,
    "PostprocessingArticleID" integer NOT NULL,
    "TotalWords" integer DEFAULT 0,
    CONSTRAINT "PostprocessingArticleKeyword_pkey" PRIMARY KEY ("KeywordID", "PostprocessingArticleID"),
    CONSTRAINT "PostprocessingArticleKeyword_KeywordID_fkey" FOREIGN KEY ("KeywordID")
        REFERENCES public."Keyword" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT "PostprocessingArticleKeyword_PostprocessingArticleID_fkey" FOREIGN KEY ("PostprocessingArticleID")
        REFERENCES public."PostprocessingArticle" ("ID") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."PostprocessingArticleKeyword"
    OWNER to educrawler_user;