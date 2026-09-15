/*
 * Top-of-grammar ANTLR4 file for the OpenIVM Spark extension.
 *
 * Adds these productions to Spark's surface SQL:
 *
 *   CREATE MATERIALIZED VIEW <multipart_identifier>
 *     [USING <provider>] [<table_clauses>] [CLUSTER BY (<cols>)] AS <query>
 *
 *   REFRESH MATERIALIZED VIEW <multipart_identifier>
 *
 *   ALTER MATERIALIZED VIEW <multipart_identifier>
 *     ADVANCE SOURCE VERSIONS (<source>=<version> [, ...])
 *
 *   DROP MATERIALIZED VIEW [IF EXISTS] <multipart_identifier>
 *
 *   EXPLAIN CREATE MATERIALIZED VIEW ... AS <query>       (dry-run verdict)
 *
 *   SHOW REFRESH SQL FOR CREATE MATERIALIZED VIEW ... AS <query>  (dry-run SQL)
 *
 *   SHOW OPENIVM REFRESH PROFILE
 *
 *   SHOW OPENIVM QUERY LOG
 *
 * The grammar is invoked only when `IvmParser.parsePlan` sees a statement
 * whose head matches one of the forms above; otherwise the input is
 * delegated to Spark's own parser unchanged.
 *
 * Notes:
 *
 *  - `query` is captured as raw text (the `.+?` token pattern) and re-parsed
 *    via Spark's `ParserInterface.parseQuery(...)`. This means we automatically
 *    support every query construct Spark 3.5 can parse — including future
 *    additions — without re-stating Spark's full grammar here.
 *
 *  - Identifiers follow Spark's convention: dotted-multipart with optional
 *    backtick quoting per part. We deliberately do NOT support double-quoted
 *    identifiers (that's DuckDB syntax) — Spark backticks are the canonical
 *    quoting style.
 *
 *  - The reserved word `MATERIALIZED` does not exist in Spark 3.5's
 *    `SqlBaseParser.g4` (verified by RESEARCH.md §6.2), so introducing it as
 *    a keyword in this grammar has zero collision risk.
 */
grammar IvmSqlBase;

ivmStatement
    : explainCreateMaterializedView
    | showRefreshSql
    | createMaterializedView
    | refreshMaterializedView
    | advanceMaterializedViewSourceVersions
    | dropMaterializedView
    | showOpenivmRefreshProfile
    | showOpenivmQueryLog
    ;

createMaterializedView
    : CREATE MATERIALIZED VIEW (IF NOT EXISTS)? multipartIdentifier
      (USING tableProvider=identifier)?
      (TBLPROPERTIES tableProperties)?
      partitionedClause?
      clusterByClause?
      AS queryBody
    ;

partitionedClause
    : PARTITIONED BY '(' multipartIdentifier (',' multipartIdentifier)* ')'
    ;

clusterByClause
    : CLUSTER BY '(' multipartIdentifier (',' multipartIdentifier)* ')'
    ;

explainCreateMaterializedView
    : EXPLAIN createMaterializedView
    ;

showRefreshSql
    : SHOW REFRESH SQL FOR createMaterializedView
    ;

refreshMaterializedView
    : REFRESH MATERIALIZED VIEW multipartIdentifier
    ;

advanceMaterializedViewSourceVersions
    : ALTER MATERIALIZED VIEW multipartIdentifier
      ADVANCE SOURCE VERSIONS '(' sourceVersionEntry (',' sourceVersionEntry)* ')'
    ;

sourceVersionEntry
    : multipartIdentifier EQ INTEGER_VALUE
    ;

dropMaterializedView
    : DROP MATERIALIZED VIEW (IF EXISTS)? multipartIdentifier
    ;

showOpenivmRefreshProfile
    : SHOW OPENIVM REFRESH PROFILE
    ;

showOpenivmQueryLog
    : SHOW OPENIVM QUERY LOG
    ;

queryBody
    : .+?
    ;

tableProperties
    : '(' tableProperty (',' tableProperty)* ')'
    ;

tableProperty
    : key=propertyKey EQ? value=propertyValue
    ;

propertyKey
    : identifier ( '.' identifier )*
    | STRING
    ;

propertyValue
    : INTEGER_VALUE
    | DECIMAL_VALUE
    | BOOLEAN_VALUE
    | STRING
    ;

multipartIdentifier
    : identifier ('.' identifier)*
    ;

identifier
    : IDENTIFIER
    | BACKQUOTED_IDENTIFIER
    | nonReserved
    ;

nonReserved
    : IF | NOT | EXISTS | USING | PARTITIONED | TBLPROPERTIES
    | AS  | BY  | DROP   | REFRESH | SHOW | OPENIVM | PROFILE
    | QUERY | LOG | EXPLAIN | CLUSTER | SQL | FOR | ALTER | ADVANCE
    | SOURCE | VERSIONS
    ;

CREATE        : [Cc][Rr][Ee][Aa][Tt][Ee];
MATERIALIZED  : [Mm][Aa][Tt][Ee][Rr][Ii][Aa][Ll][Ii][Zz][Ee][Dd];
VIEW          : [Vv][Ii][Ee][Ww];
REFRESH       : [Rr][Ee][Ff][Rr][Ee][Ss][Hh];
ALTER         : [Aa][Ll][Tt][Ee][Rr];
ADVANCE       : [Aa][Dd][Vv][Aa][Nn][Cc][Ee];
SOURCE        : [Ss][Oo][Uu][Rr][Cc][Ee];
VERSIONS      : [Vv][Ee][Rr][Ss][Ii][Oo][Nn][Ss];
DROP          : [Dd][Rr][Oo][Pp];
IF            : [Ii][Ff];
NOT           : [Nn][Oo][Tt];
EXISTS        : [Ee][Xx][Ii][Ss][Tt][Ss];
USING         : [Uu][Ss][Ii][Nn][Gg];
AS            : [Aa][Ss];
BY            : [Bb][Yy];
PARTITIONED   : [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn][Ee][Dd];
TBLPROPERTIES : [Tt][Bb][Ll][Pp][Rr][Oo][Pp][Ee][Rr][Tt][Ii][Ee][Ss];
SHOW          : [Ss][Hh][Oo][Ww];
OPENIVM       : [Oo][Pp][Ee][Nn][Ii][Vv][Mm];
PROFILE       : [Pp][Rr][Oo][Ff][Ii][Ll][Ee];
QUERY         : [Qq][Uu][Ee][Rr][Yy];
LOG           : [Ll][Oo][Gg];
EXPLAIN       : [Ee][Xx][Pp][Ll][Aa][Ii][Nn];
CLUSTER       : [Cc][Ll][Uu][Ss][Tt][Ee][Rr];
SQL           : [Ss][Qq][Ll];
FOR           : [Ff][Oo][Rr];

EQ            : '=' | '==';

INTEGER_VALUE : DIGIT+;
DECIMAL_VALUE : DIGIT+ '.' DIGIT* | '.' DIGIT+ | DIGIT+ ('.' DIGIT*)? [eE] [+-]? DIGIT+;
BOOLEAN_VALUE : [Tt][Rr][Uu][Ee] | [Ff][Aa][Ll][Ss][Ee];

IDENTIFIER             : (LETTER | '_') (LETTER | DIGIT | '_')*;
BACKQUOTED_IDENTIFIER  : '`' ( ~('`') | '``' )* '`';
STRING                 : '\'' ( ~('\'' | '\\') | ('\\' .) | '\'\'')* '\'';

fragment LETTER : [a-zA-Z];
fragment DIGIT  : [0-9];

SIMPLE_COMMENT : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN);
BRACKETED_COMMENT : '/*' .*? '*/' -> channel(HIDDEN);
WS : [ \r\n\t]+ -> channel(HIDDEN);
