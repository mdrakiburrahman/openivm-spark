/*
 * Top-of-grammar ANTLR4 file for the OpenIVM Spark extension.
 *
 * Adds four productions to Spark's surface SQL:
 *
 *   CREATE MATERIALIZED VIEW <multipart_identifier>
 *     [USING <provider>] [<table_clauses>] AS <query>
 *
 *   REFRESH MATERIALIZED VIEW <multipart_identifier>
 *
 *   DROP MATERIALIZED VIEW [IF EXISTS] <multipart_identifier>
 *
 *   SHOW OPENIVM REFRESH PROFILE
 *
 * The grammar is invoked only when `IvmParser.parsePlan` sees a statement
 * whose head matches one of the four forms above; otherwise the input is
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
    : createMaterializedView
    | refreshMaterializedView
    | dropMaterializedView
    | showOpenivmRefreshProfile
    ;

createMaterializedView
    : CREATE MATERIALIZED VIEW (IF NOT EXISTS)? multipartIdentifier
      (USING tableProvider=identifier)?
      (TBLPROPERTIES tableProperties)?
      (PARTITIONED BY '(' multipartIdentifier (',' multipartIdentifier)* ')')?
      AS queryBody
    ;

refreshMaterializedView
    : REFRESH MATERIALIZED VIEW multipartIdentifier
    ;

dropMaterializedView
    : DROP MATERIALIZED VIEW (IF EXISTS)? multipartIdentifier
    ;

showOpenivmRefreshProfile
    : SHOW OPENIVM REFRESH PROFILE
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
    ;

CREATE        : [Cc][Rr][Ee][Aa][Tt][Ee];
MATERIALIZED  : [Mm][Aa][Tt][Ee][Rr][Ii][Aa][Ll][Ii][Zz][Ee][Dd];
VIEW          : [Vv][Ii][Ee][Ww];
REFRESH       : [Rr][Ee][Ff][Rr][Ee][Ss][Hh];
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
