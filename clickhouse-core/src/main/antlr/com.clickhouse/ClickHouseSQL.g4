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

// Modified based on ClickHouse commit 6930605 on Oct 14, 2021
//   https://github.com/ClickHouse/ClickHouse/blob/master/utils/antlr/ClickHouseLexer.g4 and
//   https://github.com/ClickHouse/ClickHouse/blob/master/utils/antlr/ClickHouseParser.g4
////

grammar ClickHouseSQL;

// Top-level statements

queryStmt: query (INTO OUTFILE STRING_LITERAL)? (FORMAT identifierOrNull)? (SEMICOLON)? | insertStmt;
query
    : alterStmt     // DDL
    | attachStmt    // DDL
    | checkStmt
    | createStmt    // DDL
    | describeStmt
    | dropStmt      // DDL
    | existsStmt
    | explainStmt
    | killStmt      // DDL
    | optimizeStmt  // DDL
    | renameStmt    // DDL
    | selectUnionStmt
    | setStmt
    | showStmt
    | systemStmt
    | truncateStmt  // DDL
    | useStmt
    | watchStmt
    ;

// ALTER statement

alterStmt
    : ALTER TABLE tableIdentifier clusterClause? alterTableClause (COMMA alterTableClause)*  # AlterTableStmt
    ;

alterTableClause
    : ADD COLUMN (IF NOT EXISTS)? tableColumnDfnt (AFTER nestedIdentifier)?           # AlterTableClauseAddColumn
    | ADD INDEX (IF NOT EXISTS)? tableIndexDfnt (AFTER nestedIdentifier)?             # AlterTableClauseAddIndex
    | ADD PROJECTION (IF NOT EXISTS)? tableProjectionDfnt (AFTER nestedIdentifier)?   # AlterTableClauseAddProjection
    | ATTACH partitionClause (FROM tableIdentifier)?                                  # AlterTableClauseAttach
    | CLEAR COLUMN (IF EXISTS)? nestedIdentifier (IN partitionClause)?                # AlterTableClauseClearColumn
    | CLEAR INDEX (IF EXISTS)? nestedIdentifier (IN partitionClause)?                 # AlterTableClauseClearIndex
    | CLEAR PROJECTION (IF EXISTS)? nestedIdentifier (IN partitionClause)?            # AlterTableClauseClearProjection
    | COMMENT COLUMN (IF EXISTS)? nestedIdentifier STRING_LITERAL                     # AlterTableClauseComment
    | DELETE WHERE columnExpr                                                         # AlterTableClauseDelete
    | DETACH partitionClause                                                          # AlterTableClauseDetach
    | DROP COLUMN (IF EXISTS)? nestedIdentifier                                       # AlterTableClauseDropColumn
    | DROP INDEX (IF EXISTS)? nestedIdentifier                                        # AlterTableClauseDropIndex
    | DROP PROJECTION (IF EXISTS)? nestedIdentifier                                   # AlterTableClauseDropProjection
    | DROP partitionClause                                                            # AlterTableClauseDropPartition
    | FREEZE partitionClause?                                                         # AlterTableClauseFreezePartition
    | MATERIALIZE INDEX (IF EXISTS)? nestedIdentifier (IN partitionClause)?           # AlterTableClauseMaterializeIndex
    | MATERIALIZE PROJECTION (IF EXISTS)? nestedIdentifier (IN partitionClause)?      # AlterTableClauseMaterializeProjection
    | MODIFY COLUMN (IF EXISTS)? nestedIdentifier codecExpr                           # AlterTableClauseModifyCodec
    | MODIFY COLUMN (IF EXISTS)? nestedIdentifier COMMENT STRING_LITERAL              # AlterTableClauseModifyComment
    | MODIFY COLUMN (IF EXISTS)? nestedIdentifier REMOVE tableColumnPropertyType      # AlterTableClauseModifyRemove
    | MODIFY COLUMN (IF EXISTS)? tableColumnDfnt                                      # AlterTableClauseModify
    | MODIFY ORDER BY columnExpr                                                      # AlterTableClauseModifyOrderBy
    | MODIFY ttlClause                                                                # AlterTableClauseModifyTTL
    | MOVE partitionClause ( TO DISK STRING_LITERAL
                           | TO VOLUME STRING_LITERAL
                           | TO TABLE tableIdentifier
                           )                                                          # AlterTableClauseMovePartition
    | REMOVE TTL                                                                      # AlterTableClauseRemoveTTL
    | RENAME COLUMN (IF EXISTS)? nestedIdentifier TO nestedIdentifier                 # AlterTableClauseRename
    | REPLACE partitionClause FROM tableIdentifier                                    # AlterTableClauseReplace
    | UPDATE assignmentExprList whereClause                                           # AlterTableClauseUpdate
    ;

assignmentExprList: assignmentExpr (COMMA assignmentExpr)*;
assignmentExpr: nestedIdentifier EQ_SINGLE columnExpr;

tableColumnPropertyType: ALIAS | CODEC | COMMENT | DEFAULT | MATERIALIZED | TTL;

partitionClause
    : PARTITION columnExpr         // actually we expect here any form of tuple of literals
    | PARTITION ID STRING_LITERAL
    ;

// ATTACH statement
attachStmt
    : ATTACH DICTIONARY tableIdentifier clusterClause?  # AttachDictionaryStmt
    ;

// CHECK statement

checkStmt: CHECK TABLE tableIdentifier partitionClause?;

// CREATE statement

createStmt
    : (ATTACH | CREATE) DATABASE (IF NOT EXISTS)? databaseIdentifier clusterClause? engineExpr?                                                                                       # CreateDatabaseStmt
    | (ATTACH | CREATE (OR REPLACE)? | REPLACE) DICTIONARY (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? dictionarySchemaClause dictionaryEngineClause                                          # CreateDictionaryStmt
    | (ATTACH | CREATE) LIVE VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? (WITH TIMEOUT DECIMAL_LITERAL?)? destinationClause? tableSchemaClause? subqueryClause   # CreateLiveViewStmt
    | (ATTACH | CREATE) MATERIALIZED VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause? (destinationClause | engineClause POPULATE?) subqueryClause  # CreateMaterializedViewStmt
    | (ATTACH | CREATE (OR REPLACE)? | REPLACE) TEMPORARY? TABLE (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause? engineClause? subqueryClause?                                 # CreateTableStmt
    | (ATTACH | CREATE) (OR REPLACE)? VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause? subqueryClause                                              # CreateViewStmt
    ;

dictionarySchemaClause: LPAREN dictionaryAttrDfnt (COMMA dictionaryAttrDfnt)* RPAREN;
dictionaryAttrDfnt
    : identifier columnTypeExpr ( DEFAULT literal
                                | EXPRESSION columnExpr
                                | HIERARCHICAL
                                | INJECTIVE
                                | IS_OBJECT_ID
                                )*
                                ;
dictionaryEngineClause
    : dictionaryPrimaryKeyClause? ( sourceClause
                                  | lifetimeClause
                                  | layoutClause
                                  | rangeClause
                                  | dictionarySettingsClause
                                  )*
                                  ;
dictionaryPrimaryKeyClause: PRIMARY KEY columnExprList;
dictionaryArgExpr: identifier (identifier (LPAREN RPAREN)? | literal);
sourceClause: SOURCE LPAREN identifier LPAREN dictionaryArgExpr* RPAREN RPAREN;
lifetimeClause: LIFETIME LPAREN ( DECIMAL_LITERAL
                                | MIN DECIMAL_LITERAL MAX DECIMAL_LITERAL
                                | MAX DECIMAL_LITERAL MIN DECIMAL_LITERAL
                                ) RPAREN;
layoutClause: LAYOUT LPAREN identifier LPAREN dictionaryArgExpr* RPAREN RPAREN;
rangeClause: RANGE LPAREN (MIN identifier MAX identifier | MAX identifier MIN identifier) RPAREN;
dictionarySettingsClause: SETTINGS LPAREN settingExprList RPAREN;

clusterClause: ON CLUSTER (identifier | STRING_LITERAL);
uuidClause: UUID STRING_LITERAL;
destinationClause: TO tableIdentifier;
subqueryClause: AS selectUnionStmt;
tableSchemaClause
    : LPAREN tableElementExpr (COMMA tableElementExpr)* RPAREN  # SchemaDescriptionClause
    | AS tableIdentifier                                        # SchemaAsTableClause
    | AS tableFunctionExpr                                      # SchemaAsFunctionClause
    ;
engineClause
    : engineExpr ( orderByClause
                 | partitionByClause
                 | primaryKeyClause
                 | sampleByClause
                 | ttlClause
                 | settingsClause
                 )*
                 ;
partitionByClause: PARTITION BY columnExpr;
primaryKeyClause: PRIMARY KEY columnExpr;
sampleByClause: SAMPLE BY columnExpr;
ttlClause: TTL ttlExpr (COMMA ttlExpr)*;

// make ENGINE optional for convenient
engineExpr: ENGINE? EQ_SINGLE? identifierOrNull (LPAREN columnExprList? RPAREN)?;
tableElementExpr
    : tableColumnDfnt                                                              # TableElementExprColumn
    | CONSTRAINT identifier CHECK columnExpr                                       # TableElementExprConstraint
    | INDEX tableIndexDfnt                                                         # TableElementExprIndex
    | PROJECTION tableProjectionDfnt                                               # TableElementExprProjection
    ;
tableColumnDfnt
    : nestedIdentifier columnTypeExpr tableColumnPropertyExpr? (COMMENT STRING_LITERAL)? codecExpr? (TTL columnExpr)?
    | nestedIdentifier columnTypeExpr? tableColumnPropertyExpr (COMMENT STRING_LITERAL)? codecExpr? (TTL columnExpr)?
    ;
tableColumnPropertyExpr: (DEFAULT | MATERIALIZED | ALIAS) columnExpr;
tableIndexDfnt: nestedIdentifier columnExpr TYPE columnTypeExpr GRANULARITY DECIMAL_LITERAL;
tableProjectionDfnt: nestedIdentifier projectionSelectStmt;
codecExpr: CODEC LPAREN codecArgExpr (COMMA codecArgExpr)* RPAREN;
codecArgExpr: identifier (LPAREN columnExprList? RPAREN)?;
ttlExpr: columnExpr (DELETE | TO DISK STRING_LITERAL | TO VOLUME STRING_LITERAL)?;

// DESCRIBE statement

describeStmt: (DESCRIBE | DESC) TABLE? tableExpr;

// DROP statement

dropStmt
    : (DETACH | DROP) DATABASE (IF EXISTS)? databaseIdentifier clusterClause?                                  # DropDatabaseStmt
    | (DETACH | DROP) (DICTIONARY | TEMPORARY? TABLE | VIEW) (IF EXISTS)? tableIdentifier clusterClause? (NO DELAY)?  # DropTableStmt
    ;

// EXISTS statement

existsStmt
    : EXISTS DATABASE databaseIdentifier                             # ExistsDatabaseStmt
    | EXISTS (DICTIONARY | TEMPORARY? TABLE | VIEW)? tableIdentifier # ExistsTableStmt
    ;

// EXPLAIN statement

explainStmt
    : EXPLAIN AST query     # ExplainASTStmt
    | EXPLAIN SYNTAX query  # ExplainSyntaxStmt
    ;

// INSERT statement

insertStmt: INSERT INTO TABLE? (tableIdentifier | FUNCTION tableFunctionExpr) columnsClause? dataClause;

columnsClause: LPAREN nestedIdentifier (COMMA nestedIdentifier)* RPAREN;
dataClause
    : FORMAT identifier              # DataClauseFormat
    | VALUES                         # DataClauseValues
    | selectUnionStmt SEMICOLON? EOF # DataClauseSelect
    ;

// KILL statement

killStmt
    : KILL MUTATION clusterClause? whereClause (SYNC | ASYNC | TEST)?  # KillMutationStmt
    ;

// OPTIMIZE statement

optimizeStmt: OPTIMIZE TABLE tableIdentifier clusterClause? partitionClause? FINAL? DEDUPLICATE?;

// RENAME statement

renameStmt: RENAME TABLE tableIdentifier TO tableIdentifier (COMMA tableIdentifier TO tableIdentifier)* clusterClause?;

// PROJECTION SELECT statement

projectionSelectStmt:
    LPAREN
    withClause?
    SELECT columnExprList
    groupByClause?
    projectionOrderByClause?
    RPAREN
    ;

// SELECT statement

selectUnionStmt: selectStmtWithParens (UNION ALL selectStmtWithParens)*;
selectStmtWithParens: selectStmt | LPAREN selectUnionStmt RPAREN;
selectStmt:
    withClause?
    SELECT DISTINCT? topClause? columnExprList
    fromClause?
    arrayJoinClause?
    windowClause?
    prewhereClause?
    whereClause?
    groupByClause? (WITH (CUBE | ROLLUP))? (WITH TOTALS)?
    havingClause?
    orderByClause?
    limitByClause?
    limitClause?
    settingsClause?
    ;

withClause: WITH columnExprList;
topClause: TOP DECIMAL_LITERAL (WITH TIES)?;
fromClause: FROM joinExpr;
arrayJoinClause: (LEFT | INNER)? ARRAY JOIN columnExprList;
windowClause: WINDOW identifier AS LPAREN windowExpr RPAREN;
prewhereClause: PREWHERE columnExpr;
whereClause: WHERE columnExpr;
groupByClause: GROUP BY ((CUBE | ROLLUP) LPAREN columnExprList RPAREN | columnExprList);
havingClause: HAVING columnExpr;
orderByClause: ORDER BY orderExprList;
projectionOrderByClause: ORDER BY columnExprList;
limitByClause: LIMIT limitExpr BY columnExprList;
limitClause: LIMIT limitExpr (WITH TIES)?;
settingsClause: SETTINGS settingExprList;

joinExpr
    : joinExpr (GLOBAL | LOCAL)? joinOp? JOIN joinExpr joinConstraintClause  # JoinExprOp
    | joinExpr joinOpCross joinExpr                                          # JoinExprCrossOp
    | tableExpr FINAL? sampleClause?                                         # JoinExprTable
    | LPAREN joinExpr RPAREN                                                 # JoinExprParens
    ;
joinOp
    : ((ALL | ANY | ASOF)? INNER | INNER (ALL | ANY | ASOF)? | (ALL | ANY | ASOF))  # JoinOpInner
    | ( (SEMI | ALL | ANTI | ANY | ASOF)? (LEFT | RIGHT) OUTER?
      | (LEFT | RIGHT) OUTER? (SEMI | ALL | ANTI | ANY | ASOF)?
      )                                                                             # JoinOpLeftRight
    | ((ALL | ANY)? FULL OUTER? | FULL OUTER? (ALL | ANY)?)                         # JoinOpFull
    ;
joinOpCross
    : (GLOBAL|LOCAL)? CROSS JOIN
    | COMMA
    ;
joinConstraintClause
    : ON columnExprList
    | USING LPAREN columnExprList RPAREN
    | USING columnExprList
    ;

sampleClause: SAMPLE ratioExpr (OFFSET ratioExpr)?;
limitExpr: columnExpr ((COMMA | OFFSET) columnExpr)?;
orderExprList: orderExpr (COMMA orderExpr)*;
orderExpr: columnExpr (ASCENDING | DESCENDING | DESC)? (NULLS (FIRST | LAST))? (COLLATE STRING_LITERAL)?;
ratioExpr: numberLiteral (SLASH numberLiteral)?;
settingExprList: settingExpr (COMMA settingExpr)*;
settingExpr: identifier EQ_SINGLE literal;

windowExpr: winPartitionByClause? winOrderByClause? winFrameClause?;
winPartitionByClause: PARTITION BY columnExprList;
winOrderByClause: ORDER BY orderExprList;
winFrameClause: (ROWS | RANGE) winFrameExtend;
winFrameExtend
    : winFrameBound                             # frameStart
    | BETWEEN winFrameBound AND winFrameBound   # frameBetween
    ;
winFrameBound: (CURRENT ROW | UNBOUNDED PRECEDING | UNBOUNDED FOLLOWING | numberLiteral PRECEDING | numberLiteral FOLLOWING);
//rangeClause: RANGE LPAREN (MIN identifier MAX identifier | MAX identifier MIN identifier) RPAREN;


// SET statement

setStmt: SET settingExprList;

// SHOW statements

showStmt
    : SHOW CREATE DATABASE databaseIdentifier                                                                     # showCreateDatabaseStmt
    | SHOW CREATE DICTIONARY tableIdentifier                                                                      # showCreateDictionaryStmt
    | SHOW CREATE TEMPORARY? TABLE? tableIdentifier                                                               # showCreateTableStmt
    | SHOW DATABASES                                                                                              # showDatabasesStmt
    | SHOW DICTIONARIES (FROM databaseIdentifier)?                                                                # showDictionariesStmt
    | SHOW TEMPORARY? TABLES ((FROM | IN) databaseIdentifier)? (LIKE STRING_LITERAL | whereClause)? limitClause?  # showTablesStmt
    ;

// SYSTEM statements

systemStmt
    : SYSTEM FLUSH DISTRIBUTED tableIdentifier
    | SYSTEM FLUSH LOGS
    | SYSTEM RELOAD DICTIONARIES
    | SYSTEM RELOAD DICTIONARY tableIdentifier
    | SYSTEM (START | STOP) (DISTRIBUTED SENDS | FETCHES | TTL? MERGES) tableIdentifier
    | SYSTEM (START | STOP) REPLICATED SENDS
    | SYSTEM SYNC REPLICA tableIdentifier
    ;

// TRUNCATE statements

truncateStmt: TRUNCATE TEMPORARY? TABLE? (IF EXISTS)? tableIdentifier clusterClause?;

// USE statement

useStmt: USE databaseIdentifier;

// WATCH statement

watchStmt: WATCH tableIdentifier EVENTS? (LIMIT DECIMAL_LITERAL)?;



// Columns

columnTypeExpr
    : identifier                                                                             # ColumnTypeExprSimple   // UInt64
    | identifier LPAREN identifier columnTypeExpr (COMMA identifier columnTypeExpr)* RPAREN  # ColumnTypeExprNested   // Nested
    | identifier LPAREN enumValue (COMMA enumValue)* RPAREN                                  # ColumnTypeExprEnum     // Enum
    | identifier LPAREN columnTypeExpr (COMMA columnTypeExpr)* RPAREN                        # ColumnTypeExprComplex  // Array, Tuple
    | identifier LPAREN columnExprList? RPAREN                                               # ColumnTypeExprParam    // FixedString(N)
    ;
columnExprList: columnsExpr (COMMA columnsExpr)*;
columnsExpr
    : (tableIdentifier DOT)? ASTERISK  # ColumnsExprAsterisk
    | LPAREN selectUnionStmt RPAREN    # ColumnsExprSubquery
    // NOTE: asterisk and subquery goes before |columnExpr| so that we can mark them as multi-column expressions.
    | columnExpr                       # ColumnsExprColumn
    ;
columnExpr
    : CASE columnExpr? (WHEN columnExpr THEN columnExpr)+ (ELSE columnExpr)? END          # ColumnExprCase
    | CAST LPAREN columnExpr AS columnTypeExpr RPAREN                                     # ColumnExprCast
    | DATE STRING_LITERAL                                                                 # ColumnExprDate
    | EXTRACT LPAREN interval FROM columnExpr RPAREN                                      # ColumnExprExtract
    | INTERVAL columnExpr interval                                                        # ColumnExprInterval
    | SUBSTRING LPAREN columnExpr FROM columnExpr (FOR columnExpr)? RPAREN                # ColumnExprSubstring
    | TIMESTAMP STRING_LITERAL                                                            # ColumnExprTimestamp
    | TRIM LPAREN (BOTH | LEADING | TRAILING) STRING_LITERAL FROM columnExpr RPAREN       # ColumnExprTrim
    | identifier (LPAREN columnExprList? RPAREN) OVER LPAREN windowExpr RPAREN            # ColumnExprWinFunction
    | identifier (LPAREN columnExprList? RPAREN) OVER identifier                          # ColumnExprWinFunctionTarget
    | identifier (LPAREN columnExprList? RPAREN)? LPAREN DISTINCT? columnArgList? RPAREN  # ColumnExprFunction
    | literal                                                                             # ColumnExprLiteral

    // FIXME(ilezhankin): this part looks very ugly, maybe there is another way to express it
    | columnExpr LBRACKET columnExpr RBRACKET                                             # ColumnExprArrayAccess
    | columnExpr DOT DECIMAL_LITERAL                                                      # ColumnExprTupleAccess
    | DASH columnExpr                                                                     # ColumnExprNegate
    | columnExpr ( ASTERISK                                                               // multiply
                 | SLASH                                                                  // divide
                 | PERCENT                                                                // modulo
                 ) columnExpr                                                             # ColumnExprPrecedence1
    | columnExpr ( PLUS                                                                   // plus
                 | DASH                                                                   // minus
                 | CONCAT                                                                 // concat
                 ) columnExpr                                                             # ColumnExprPrecedence2
    | columnExpr ( EQ_DOUBLE                                                              // equals
                 | EQ_SINGLE                                                              // equals
                 | NOT_EQ                                                                 // notEquals
                 | LE                                                                     // lessOrEquals
                 | GE                                                                     // greaterOrEquals
                 | LT                                                                     // less
                 | GT                                                                     // greater
                 | GLOBAL? NOT? IN                                                        // in, notIn, globalIn, globalNotIn
                 | NOT? (LIKE | ILIKE)                                                    // like, notLike, ilike, notILike
                 ) columnExpr                                                             # ColumnExprPrecedence3
    | columnExpr IS NOT? NULL_SQL                                                         # ColumnExprIsNull
    | NOT columnExpr                                                                      # ColumnExprNot
    | columnExpr AND columnExpr                                                           # ColumnExprAnd
    | columnExpr OR columnExpr                                                            # ColumnExprOr
    // TODO(ilezhankin): `BETWEEN a AND b AND c` is parsed in a wrong way: `BETWEEN (a AND b) AND c`
    | columnExpr NOT? BETWEEN columnExpr AND columnExpr                                   # ColumnExprBetween
    | <assoc=right> columnExpr QUERY columnExpr COLON columnExpr                          # ColumnExprTernaryOp
    | columnExpr (alias | AS identifier)                                                  # ColumnExprAlias

    | (tableIdentifier DOT)? ASTERISK                                                     # ColumnExprAsterisk  // single-column only
    | LPAREN selectUnionStmt RPAREN                                                       # ColumnExprSubquery  // single-column only
    | LPAREN columnExpr RPAREN                                                            # ColumnExprParens    // single-column only
    | LPAREN columnExprList RPAREN                                                        # ColumnExprTuple
    | LBRACKET columnExprList? RBRACKET                                                   # ColumnExprArray
    | columnIdentifier                                                                    # ColumnExprIdentifier
    ;
columnArgList: columnArgExpr (COMMA columnArgExpr)*;
columnArgExpr: columnLambdaExpr | columnExpr;
columnLambdaExpr:
    ( LPAREN identifier (COMMA identifier)* RPAREN
    |        identifier (COMMA identifier)*
    )
    ARROW columnExpr
    ;
columnIdentifier: (tableIdentifier DOT)? nestedIdentifier;
nestedIdentifier: identifier (DOT identifier)?;

// Tables

tableExpr
    : tableIdentifier                    # TableExprIdentifier
    | tableFunctionExpr                  # TableExprFunction
    | LPAREN selectUnionStmt RPAREN      # TableExprSubquery
    | tableExpr (alias | AS identifier)  # TableExprAlias
    ;
tableFunctionExpr: identifier LPAREN tableArgList? RPAREN;
tableIdentifier: (databaseIdentifier DOT)? identifier;
tableArgList: tableArgExpr (COMMA tableArgExpr)*;
tableArgExpr
    : nestedIdentifier
    | tableFunctionExpr
    | literal
    ;

// Databases

databaseIdentifier: identifier;

// Basics

floatingLiteral
    : FLOATING_LITERAL
    | DOT (DECIMAL_LITERAL | OCTAL_LITERAL)
    | DECIMAL_LITERAL DOT (DECIMAL_LITERAL | OCTAL_LITERAL)?  // can't move this to the lexer or it will break nested tuple access: t.1.2
    ;
numberLiteral: (PLUS | DASH)? (floatingLiteral | OCTAL_LITERAL | DECIMAL_LITERAL | HEXADECIMAL_LITERAL | INF | NAN_SQL);
literal
    : numberLiteral
    | STRING_LITERAL
    | NULL_SQL
    ;
interval: SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR;
keyword
    // except NULL_SQL, INF, NAN_SQL
    : AFTER | ALIAS | ALL | ALTER | AND | ANTI | ANY | ARRAY | AS | ASCENDING | ASOF | AST | ASYNC | ATTACH | BETWEEN | BOTH | BY | CASE
    | CAST | CHECK | CLEAR | CLUSTER | CODEC | COLLATE | COLUMN | COMMENT | CONSTRAINT | CREATE | CROSS | CUBE | CURRENT | DATABASE
    | DATABASES | DATE | DEDUPLICATE | DEFAULT | DELAY | DELETE | DESCRIBE | DESC | DESCENDING | DETACH | DICTIONARIES | DICTIONARY | DISK
    | DISTINCT | DISTRIBUTED | DROP | ELSE | END | ENGINE | EVENTS | EXISTS | EXPLAIN | EXPRESSION | EXTRACT | FETCHES | FINAL | FIRST
    | FLUSH | FOR | FOLLOWING | FOR | FORMAT | FREEZE | FROM | FULL | FUNCTION | GLOBAL | GRANULARITY | GROUP | HAVING | HIERARCHICAL | ID
    | IF | ILIKE | IN | INDEX | INJECTIVE | INNER | INSERT | INTERVAL | INTO | IS | IS_OBJECT_ID | JOIN | JSON_FALSE | JSON_TRUE | KEY
    | KILL | LAST | LAYOUT | LEADING | LEFT | LIFETIME | LIKE | LIMIT | LIVE | LOCAL | LOGS | MATERIALIZE | MATERIALIZED | MAX | MERGES
    | MIN | MODIFY | MOVE | MUTATION | NO | NOT | NULLS | OFFSET | ON | OPTIMIZE | OR | ORDER | OUTER | OUTFILE | OVER | PARTITION
    | POPULATE | PRECEDING | PREWHERE | PRIMARY | RANGE | RELOAD | REMOVE | RENAME | REPLACE | REPLICA | REPLICATED | RIGHT | ROLLUP | ROW
    | ROWS | SAMPLE | SELECT | SEMI | SENDS | SET | SETTINGS | SHOW | SOURCE | START | STOP | SUBSTRING | SYNC | SYNTAX | SYSTEM | TABLE
    | TABLES | TEMPORARY | TEST | THEN | TIES | TIMEOUT | TIMESTAMP | TOTALS | TRAILING | TRIM | TRUNCATE | TO | TOP | TTL | TYPE
    | UNBOUNDED | UNION | UPDATE | USE | USING | UUID | VALUES | VIEW | VOLUME | WATCH | WHEN | WHERE | WINDOW | WITH
    ;
keywordForAlias
    : DATE | FIRST | ID | KEY
    ;
alias: IDENTIFIER | keywordForAlias;  // |interval| can't be an alias, otherwise 'INTERVAL 1 SOMETHING' becomes ambiguous.
identifier: IDENTIFIER | interval | keyword;
identifierOrNull: identifier | NULL_SQL;  // NULL_SQL can be only 'Null' here.
enumValue: STRING_LITERAL EQ_SINGLE numberLiteral;


// NOTE: don't forget to add new keywords to the parser rule "keyword"!

// Keywords

ADD: A D D;
AFTER: A F T E R;
ALIAS: A L I A S;
ALL: A L L;
ALTER: A L T E R;
AND: A N D;
ANTI: A N T I;
ANY: A N Y;
ARRAY: A R R A Y;
AS: A S;
ASCENDING: A S C | A S C E N D I N G;
ASOF: A S O F;
AST: A S T;
ASYNC: A S Y N C;
ATTACH: A T T A C H;
BETWEEN: B E T W E E N;
BOTH: B O T H;
BY: B Y;
CASE: C A S E;
CAST: C A S T;
CHECK: C H E C K;
CLEAR: C L E A R;
CLUSTER: C L U S T E R;
CODEC: C O D E C;
COLLATE: C O L L A T E;
COLUMN: C O L U M N;
COMMENT: C O M M E N T;
CONSTRAINT: C O N S T R A I N T;
CREATE: C R E A T E;
CROSS: C R O S S;
CUBE: C U B E;
CURRENT: C U R R E N T;
DATABASE: D A T A B A S E;
DATABASES: D A T A B A S E S;
DATE: D A T E;
DAY: D A Y;
DEDUPLICATE: D E D U P L I C A T E;
DEFAULT: D E F A U L T;
DELAY: D E L A Y;
DELETE: D E L E T E;
DESC: D E S C;
DESCENDING: D E S C E N D I N G;
DESCRIBE: D E S C R I B E;
DETACH: D E T A C H;
DICTIONARIES: D I C T I O N A R I E S;
DICTIONARY: D I C T I O N A R Y;
DISK: D I S K;
DISTINCT: D I S T I N C T;
DISTRIBUTED: D I S T R I B U T E D;
DROP: D R O P;
ELSE: E L S E;
END: E N D;
ENGINE: E N G I N E;
EVENTS: E V E N T S;
EXISTS: E X I S T S;
EXPLAIN: E X P L A I N;
EXPRESSION: E X P R E S S I O N;
EXTRACT: E X T R A C T;
FETCHES: F E T C H E S;
FINAL: F I N A L;
FIRST: F I R S T;
FLUSH: F L U S H;
FOLLOWING: F O L L O W I N G;
FOR: F O R;
FORMAT: F O R M A T;
FREEZE: F R E E Z E;
FROM: F R O M;
FULL: F U L L;
FUNCTION: F U N C T I O N;
GLOBAL: G L O B A L;
GRANULARITY: G R A N U L A R I T Y;
GROUP: G R O U P;
HAVING: H A V I N G;
HIERARCHICAL: H I E R A R C H I C A L;
HOUR: H O U R;
ID: I D;
IF: I F;
ILIKE: I L I K E;
IN: I N;
INDEX: I N D E X;
INF: I N F | I N F I N I T Y;
INJECTIVE: I N J E C T I V E;
INNER: I N N E R;
INSERT: I N S E R T;
INTERVAL: I N T E R V A L;
INTO: I N T O;
IS: I S;
IS_OBJECT_ID: I S UNDERSCORE O B J E C T UNDERSCORE I D;
JOIN: J O I N;
KEY: K E Y;
KILL: K I L L;
LAST: L A S T;
LAYOUT: L A Y O U T;
LEADING: L E A D I N G;
LEFT: L E F T;
LIFETIME: L I F E T I M E;
LIKE: L I K E;
LIMIT: L I M I T;
LIVE: L I V E;
LOCAL: L O C A L;
LOGS: L O G S;
MATERIALIZE: M A T E R I A L I Z E;
MATERIALIZED: M A T E R I A L I Z E D;
MAX: M A X;
MERGES: M E R G E S;
MIN: M I N;
MINUTE: M I N U T E;
MODIFY: M O D I F Y;
MONTH: M O N T H;
MOVE: M O V E;
MUTATION: M U T A T I O N;
NAN_SQL: N A N; // conflicts with macro NAN
NO: N O;
NOT: N O T;
NULL_SQL: N U L L; // conflicts with macro NULL
NULLS: N U L L S;
OFFSET: O F F S E T;
ON: O N;
OPTIMIZE: O P T I M I Z E;
OR: O R;
ORDER: O R D E R;
OUTER: O U T E R;
OUTFILE: O U T F I L E;
OVER: O V E R;
PARTITION: P A R T I T I O N;
POPULATE: P O P U L A T E;
PRECEDING: P R E C E D I N G;
PREWHERE: P R E W H E R E;
PRIMARY: P R I M A R Y;
PROJECTION: P R O J E C T I O N;
QUARTER: Q U A R T E R;
RANGE: R A N G E;
RELOAD: R E L O A D;
REMOVE: R E M O V E;
RENAME: R E N A M E;
REPLACE: R E P L A C E;
REPLICA: R E P L I C A;
REPLICATED: R E P L I C A T E D;
RIGHT: R I G H T;
ROLLUP: R O L L U P;
ROW: R O W;
ROWS: R O W S;
SAMPLE: S A M P L E;
SECOND: S E C O N D;
SELECT: S E L E C T;
SEMI: S E M I;
SENDS: S E N D S;
SET: S E T;
SETTINGS: S E T T I N G S;
SHOW: S H O W;
SOURCE: S O U R C E;
START: S T A R T;
STOP: S T O P;
SUBSTRING: S U B S T R I N G;
SYNC: S Y N C;
SYNTAX: S Y N T A X;
SYSTEM: S Y S T E M;
TABLE: T A B L E;
TABLES: T A B L E S;
TEMPORARY: T E M P O R A R Y;
TEST: T E S T;
THEN: T H E N;
TIES: T I E S;
TIMEOUT: T I M E O U T;
TIMESTAMP: T I M E S T A M P;
TO: T O;
TOP: T O P;
TOTALS: T O T A L S;
TRAILING: T R A I L I N G;
TRIM: T R I M;
TRUNCATE: T R U N C A T E;
TTL: T T L;
TYPE: T Y P E;
UNBOUNDED: U N B O U N D E D;
UNION: U N I O N;
UPDATE: U P D A T E;
USE: U S E;
USING: U S I N G;
UUID: U U I D;
VALUES: V A L U E S;
VIEW: V I E W;
VOLUME: V O L U M E;
WATCH: W A T C H;
WEEK: W E E K;
WHEN: W H E N;
WHERE: W H E R E;
WINDOW: W I N D O W;
WITH: W I T H;
YEAR: Y E A R | Y Y Y Y;

JSON_FALSE: 'false';
JSON_TRUE: 'true';

// Tokens

IDENTIFIER
    : (LETTER | UNDERSCORE) (LETTER | UNDERSCORE | DEC_DIGIT)*
    | BACKQUOTE ( ~([\\`]) | (BACKSLASH .) | (BACKQUOTE BACKQUOTE) )* BACKQUOTE
    | QUOTE_DOUBLE ( ~([\\"]) | (BACKSLASH .) | (QUOTE_DOUBLE QUOTE_DOUBLE) )* QUOTE_DOUBLE
    ;
FLOATING_LITERAL
    : HEXADECIMAL_LITERAL DOT HEX_DIGIT* (P | E) (PLUS | DASH)? DEC_DIGIT+
    | HEXADECIMAL_LITERAL (P | E) (PLUS | DASH)? DEC_DIGIT+
    | DECIMAL_LITERAL DOT DEC_DIGIT* E (PLUS | DASH)? DEC_DIGIT+
    | DOT DECIMAL_LITERAL E (PLUS | DASH)? DEC_DIGIT+
    | DECIMAL_LITERAL E (PLUS | DASH)? DEC_DIGIT+
    ;
OCTAL_LITERAL: '0' OCT_DIGIT+;
DECIMAL_LITERAL: DEC_DIGIT+;
HEXADECIMAL_LITERAL: '0' X HEX_DIGIT+;

// It's important that quote-symbol is a single character.
STRING_LITERAL: QUOTE_SINGLE ( ~([\\']) | (BACKSLASH .) | (QUOTE_SINGLE QUOTE_SINGLE) )* QUOTE_SINGLE;

// Alphabet and allowed symbols

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];

fragment LETTER: [a-zA-Z];
fragment OCT_DIGIT: [0-7];
fragment DEC_DIGIT: [0-9];
fragment HEX_DIGIT: [0-9a-fA-F];

ARROW: '->';
ASTERISK: '*';
BACKQUOTE: '`';
BACKSLASH: '\\';
COLON: ':';
COMMA: ',';
CONCAT: '||';
DASH: '-';
DOT: '.';
EQ_DOUBLE: '==';
EQ_SINGLE: '=';
GE: '>=';
GT: '>';
LBRACE: '{';
LBRACKET: '[';
LE: '<=';
LPAREN: '(';
LT: '<';
NOT_EQ: '!=' | '<>';
PERCENT: '%';
PLUS: '+';
QUERY: '?';
QUOTE_DOUBLE: '"';
QUOTE_SINGLE: '\'';
RBRACE: '}';
RBRACKET: ']';
RPAREN: ')';
SEMICOLON: ';';
SLASH: '/';
UNDERSCORE: '_';

// Comments and whitespace

MULTI_LINE_COMMENT: '/*' .*? '*/' -> skip;
SINGLE_LINE_COMMENT: '--' ~('\n'|'\r')* ('\n' | '\r' | EOF) -> skip;
WHITESPACE: [ \u000B\u000C\t\r\n] -> skip;  // '\n' can be part of multiline single query
