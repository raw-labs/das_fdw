/*-------------------------------------------------------------------------
 *
 * das_fdw.h
 * 		Foreign-data wrapper for remote DAS servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2024, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		das_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef das_FDW_H
#define das_FDW_H

#include "access/tupdesc.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/pathnodes.h"
#include "utils/rel.h"
#include "grpc_client.h"

/* Macro for list API backporting. */
#if PG_VERSION_NUM < 130000
#define das_list_concat(l1, l2) list_concat(l1, list_copy(l2))
#else
#define das_list_concat(l1, l2) list_concat((l1), (l2))
#endif

/*
 * Structure that holds the initialized DAS connection object.
 */
typedef struct DAS {
    char* das_url;
    char* das_type;
	char *das_id;
    HTAB* pushability_hash;
	bool orderby_supported;
	bool join_supported;
	bool aggregation_supported;
} DAS;

/*
 * Structure to hold information for constructing a whole-row reference value
 * for a single base relation involved in a pushed down join.
 */
typedef struct
{
	/*
	 * Tuple descriptor for whole-row reference. We can not use the base
	 * relation's tuple descriptor as it is, since it might have information
	 * about dropped attributes.
	 */
	TupleDesc	tupdesc;

	/*
	 * Positions of the required attributes in the tuple fetched from the
	 * foreign server.
	 */
	int		   *attr_pos;

	/* Position of attribute indicating NULL-ness of whole-row reference */
	int			wr_null_ind_pos;

	/* Values and null array for holding column values. */
	Datum	   *values;
	bool	   *nulls;
} DASWRState;

/*
 * FDW-specific information for ForeignScanState
 * fdw_state.
 */
typedef struct DASFdwExecState
{
 	/* Connection and query execution */
	DAS 	  		 *das;
	das_opt        *dasFdwOptions;/* DAS FDW options */
    SqlQueryIterator *iterator;       /* Iterator for fetching rows */
    char             *query;          /* Query string */
    List             *retrieved_attrs;/* List of target attribute numbers */
    AttInMetadata    *attinmeta;      /* Attribute input metadata */
    MemoryContext     temp_cxt;       /* Context for per-tuple temporary data */
	int 			 *attnums;
	Oid 			 *pgtypes;
	int32 			 *pgtypmods;

    /* Field mapping */
    int               num_fields;     /* Number of fields in the result set */
    int              *field_map;      /* Mapping from field positions to attribute numbers */
	HTAB 			 *colname_to_attnum;

    /* Whole-row reference handling */
    int               max_relid;      /* Maximum relation ID */
    Bitmapset        *relids;         /* Set of relation IDs involved in the scan */
    DASWRState    **wr_states;      /* Whole-row construction information */
    TupleDesc         wr_tupdesc;     /* Tuple descriptor for the result */
    Datum            *wr_values;      /* Array for holding column values */
    bool             *wr_nulls;       /* Array for holding null flags */

    /* Other state */
    int               numParams;      /* Number of parameters passed to query */
    FmgrInfo         *param_flinfo;   /* Output conversion functions */
    List             *param_exprs;    /* Executable expressions for param values */
    const char      **param_values;   /* Textual values of query parameters */
    Oid              *param_types;    /* Types of query parameters */

	int				  p_nums;			/* number of parameters to transmit */
	FmgrInfo	     *p_flinfo;		/* output conversion functions for them */
	bool			  has_var_size_col;	/* true if fetching var size columns */
	int			     *wr_attrs_pos;	/* Array mapping the attributes in the
									 * ForeignScan result to those in the rows
									 * fetched from the foreign server.  The array
									 * is indexed by the attribute numbers in the
									 * ForeignScan. */
	DASWRState    **daswrstates;	/* whole-row construction information for
									 * each base relation involved in the
									 * pushed down join. */
	AttrNumber		  rowidAttno;		/* attnum of resjunk rowid column */
	char 		     *attname;	/* attname of rowid column */
	char 		     *table_name;	/* table name */

	uint64	     	  plan_id; /* unique identifier for this plan */

} DASFdwExecState;

typedef struct DASFdwRelationInfo
{
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool		pushdown_safe;

	/* baserestrictinfo clauses, broken down into safe and unsafe subsets. */
	List	   *remote_conds;
	List	   *local_conds;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

	/* True means that the query_pathkeys is safe to push down */
	bool		qp_is_pushdown_safe;

	/*
	 * Name of the relation while EXPLAINing ForeignScan.  It is used for join
	 * relations but is set for all relations.  For join relation, the name
	 * indicates which foreign tables are being joined and the join type used.
	 */
	StringInfo	relation_name;

	/* Join information */
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;
	JoinType	jointype;
	List	   *joinclauses;
	/* Grouping information */
	List	   *grouped_tlist;

	/* Upper relation information */
	UpperRelationKind stage;

	/* DAS FDW options */
	das_opt  *options;

	DAS 	  *das;

	uint64	   	plan_id; /* unique identifier for this plan */

} DASFdwRelationInfo;


/* DAS Column List */
typedef struct DASColumn
{
	int			attnum;			/* Attribute number */
	char	   *attname;		/* Attribute name */
	int			atttype;		/* Attribute type */
} DASColumn;

/* option.c headers */
extern bool das_is_valid_option(const char *option, Oid context);
extern das_opt *das_get_options(Oid foreigntableid, bool is_foreigntable);

/* depare.c headers */
extern bool das_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel,
								  Expr *expr, bool is_remote_cond);
extern void das_deparse_select_stmt_for_rel(StringInfo buf,
											  PlannerInfo *root,
											  RelOptInfo *rel, List *tlist,
											  List *remote_conds,
											  List *pathkeys,
											  bool has_final_sort,
											  bool has_limit,
											  List **retrieved_attrs,
											  List **params_list);
extern const char *das_get_jointype_name(JoinType jointype);
extern bool das_is_foreign_param(PlannerInfo *root, RelOptInfo *baserel,
								   Expr *expr);
extern bool das_is_foreign_pathkey(PlannerInfo *root, RelOptInfo *baserel,
									 PathKey *pathkey);
extern char *das_get_sortby_direction_string(EquivalenceMember *em,
											   PathKey *pathkey,
											   HTAB* pushability_hash);
extern EquivalenceMember *das_find_em_for_rel(PlannerInfo *root,
												EquivalenceClass *ec,
												RelOptInfo *rel);
extern EquivalenceMember *das_find_em_for_rel_target(PlannerInfo *root,
													   EquivalenceClass *ec,
													   RelOptInfo *rel);
extern bool das_is_builtin(Oid objectId);
extern char *das_quote_identifier(const char *str, char quotechar);

/* das_connection.c headers */
DAS *das_get_connection(ForeignServer *server, UserMapping *user,
							das_opt *opt);
DAS *das_fdw_connect(das_opt *opt);
void das_cleanup_connection(void);
void das_release_connection(DAS *conn);

#endif							/* das_FDW_H */
