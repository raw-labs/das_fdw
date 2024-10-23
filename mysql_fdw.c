/*-------------------------------------------------------------------------
 *
 * mysql_fdw.c
 * 		Foreign-data wrapper for remote MySQL servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2024, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		mysql_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

/*
 * Must be included before mysql.h as it has some conflicting definitions like
 * list_length, etc.
 */
#include "mysql_fdw.h"

// #include "pg_query.h"

#include <dlfcn.h>
// #include <errmsg.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "catalog/heap.h"
#include "catalog/pg_type.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "mysql_pushability.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#if PG_VERSION_NUM >= 140000
#include "optimizer/appendinfo.h"
#endif
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/optimizer.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#if PG_VERSION_NUM >= 160000
#include "parser/parse_relation.h"
#endif
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/* Declarations for dynamic loading */
PG_MODULE_MAGIC;

/*
 * In PG 9.5.1 the number will be 90501,
 * our version is 2.9.2 so number will be 20902
 */
#define CODE_VERSION   20902

/*
 * The number of rows in a foreign relation are estimated to be so less that
 * an in-memory sort on those many rows wouldn't cost noticeably higher than
 * the underlying scan. Hence for now, cost sorts same as underlying scans.
 */
#define DEFAULT_MYSQL_SORT_MULTIPLIER 1


/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum mysqlFdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, mysqlFdwScanPrivateSelectSql));
 */
enum mysqlFdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	mysqlFdwScanPrivateSelectSql,

	/* Integer list of attribute numbers retrieved by the SELECT */
	mysqlFdwScanPrivateRetrievedAttrs,

	/* Unique identifier for the query plan */
	mysqlFdwScanPrivateUniquePlanId,

	/*
	 * String describing join i.e. names of relations being joined and types
	 * of join, added when the scan is join
	 */
	mysqlFdwScanPrivateRelations,

	/*
	 * List of Var node lists for constructing the whole-row references of
	 * base relations involved in pushed down join.
	 */
	mysqlFdwPrivateWholeRowLists,

	/*
	 * Targetlist representing the result fetched from the foreign server if
	 * whole-row references are involved.
	 */
	mysqlFdwPrivateScanTList
};

/*
 * This enum describes what's kept in the fdw_private list for a ForeignPath.
 * We store:
 *
 * 1) Boolean flag showing if the remote query has the final sort
 * 2) Boolean flag showing if the remote query has the LIMIT clause
 */
enum FdwPathPrivateIndex
{
	/* has-final-sort flag (as an integer Value node) */
	FdwPathPrivateHasFinalSort,
	/* has-limit flag (as an integer Value node) */
	FdwPathPrivateHasLimit
};

extern PGDLLEXPORT void _PG_init(void);
extern Datum mysql_fdw_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(mysql_fdw_handler);
PG_FUNCTION_INFO_V1(mysql_fdw_version);
// PG_FUNCTION_INFO_V1(mysql_display_pushdown_list);

/* Global query counter */
static pg_atomic_uint64 global_query_counter;

/* Unique identifier for this FDW plugin */
static int fdw_instance_pid;

/*
 * FDW callback routines
 */
// static void mysqlExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void mysqlBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *mysqlIterateForeignScan(ForeignScanState *node);
static void mysqlReScanForeignScan(ForeignScanState *node);
static void mysqlEndForeignScan(ForeignScanState *node);

static List *mysqlPlanForeignModify(PlannerInfo *root, ModifyTable *plan,
									Index resultRelation, int subplan_index);
static void mysqlBeginForeignModify(ModifyTableState *mtstate,
									ResultRelInfo *resultRelInfo,
									List *fdw_private, int subplan_index,
									int eflags);
static TupleTableSlot *mysqlExecForeignInsert(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
#if PG_VERSION_NUM >= 140000
static void mysqlAddForeignUpdateTargets(PlannerInfo *root,
										 Index rtindex,
										 RangeTblEntry *target_rte,
										 Relation target_relation);
#else
static void mysqlAddForeignUpdateTargets(Query *parsetree,
										 RangeTblEntry *target_rte,
										 Relation target_relation);
#endif
static TupleTableSlot *mysqlExecForeignUpdate(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
static TupleTableSlot *mysqlExecForeignDelete(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
static void mysqlEndForeignModify(EState *estate,
								  ResultRelInfo *resultRelInfo);

static void mysqlGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
								   Oid foreigntableid);
static void mysqlGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
								 Oid foreigntableid);
// static bool mysqlAnalyzeForeignTable(Relation relation,
// 									 AcquireSampleRowsFunc *func,
// 									 BlockNumber *totalpages);
static ForeignScan *mysqlGetForeignPlan(PlannerInfo *root,
										RelOptInfo *foreignrel,
										Oid foreigntableid,
										ForeignPath *best_path, List *tlist,
										List *scan_clauses, Plan *outer_plan);
static void mysqlEstimateCosts(PlannerInfo *root, RelOptInfo *baserel,
							   Cost *startup_cost, Cost *total_cost,
							   Oid foreigntableid);
static void mysqlGetForeignJoinPaths(PlannerInfo *root,
									 RelOptInfo *joinrel,
									 RelOptInfo *outerrel,
									 RelOptInfo *innerrel,
									 JoinType jointype,
									 JoinPathExtraData *extra);
static bool mysqlRecheckForeignScan(ForeignScanState *node,
									TupleTableSlot *slot);

static void mysqlGetForeignUpperPaths(PlannerInfo *root,
									  UpperRelationKind stage,
									  RelOptInfo *input_rel,
									  RelOptInfo *output_rel,
									  void *extra);
static List *mysqlImportForeignSchema(ImportForeignSchemaStmt *stmt,
									  Oid serverOid);

static void mysqlBeginForeignInsert(ModifyTableState *mtstate,
									ResultRelInfo *resultRelInfo);
static void mysqlEndForeignInsert(EState *estate,
								  ResultRelInfo *resultRelInfo);

/*
 * Helper functions
 */
// bool mysql_load_library(void);
static void mysql_fdw_exit(int code, Datum arg);
// static bool mysql_is_column_unique(Oid foreigntableid);

static void prepare_query_params(PlanState *node,
								 List *fdw_exprs,
								 int numParams,
								 FmgrInfo **param_flinfo,
								 List **param_exprs,
								 const char ***param_values,
								 Oid **param_types);
static bool mysql_foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel,
								  JoinType jointype, RelOptInfo *outerrel,
								  RelOptInfo *innerrel,
								  JoinPathExtraData *extra);
static List *mysql_adjust_whole_row_ref(PlannerInfo *root,
										List *scan_var_list,
										List **whole_row_lists,
										Bitmapset *relids);
static List *mysql_build_scan_list_for_baserel(Oid relid, Index varno,
											   Bitmapset *attrs_used,
											   List **retrieved_attrs);
static void mysql_build_whole_row_constr_info(MySQLFdwExecState *festate,
											  TupleDesc tupdesc,
											  Bitmapset *relids,
											  int max_relid,
											  List *whole_row_lists,
											  List *scan_tlist,
											  List *fdw_scan_tlist);
static HeapTuple mysql_get_tuple_with_whole_row(MySQLFdwExecState *festate,
												Datum *values, bool *nulls);
static HeapTuple mysql_form_whole_row(MySQLWRState *wr_state, Datum *values,
									  bool *nulls);
static bool mysql_foreign_grouping_ok(PlannerInfo *root,
									  RelOptInfo *grouped_rel,
									  Node *havingQual);
static void mysql_add_foreign_grouping_paths(PlannerInfo *root,
											 RelOptInfo *input_rel,
											 RelOptInfo *grouped_rel,
											 GroupPathExtraData *extra);
static List *mysql_get_useful_ecs_for_relation(PlannerInfo *root,
											   RelOptInfo *rel);
static List *mysql_get_useful_pathkeys_for_relation(PlannerInfo *root,
													RelOptInfo *rel);
#if PG_VERSION_NUM >= 170000
static void mysql_add_paths_with_pathkeys(PlannerInfo *root,
										  RelOptInfo *rel,
										  Path *epq_path,
										  Cost base_startup_cost,
										  Cost base_total_cost,
										  List *restrictlist);
#else
static void mysql_add_paths_with_pathkeys(PlannerInfo *root,
										  RelOptInfo *rel,
										  Path *epq_path,
										  Cost base_startup_cost,
										  Cost base_total_cost);
#endif
static void mysql_add_foreign_ordered_paths(PlannerInfo *root,
											RelOptInfo *input_rel,
											RelOptInfo *ordered_rel);
static void mysql_add_foreign_final_paths(PlannerInfo *root,
										  RelOptInfo *input_rel,
										  RelOptInfo *final_rel,
										  FinalPathExtraData *extra);
#if PG_VERSION_NUM >= 160000
static TargetEntry *mysql_tlist_member_match_var(Var *var, List *targetlist);
static List *mysql_varlist_append_unique_var(List *varlist, Var *var);
#endif

static List *getUpdateTargetAttrs(PlannerInfo *root, RangeTblEntry *rte, int unique_attnum);
#if PG_VERSION_NUM >= 140000
static char *mysql_remove_quotes(char *s1);
#endif

/*
 * Library load-time initialization, sets on_proc_exit() callback for
 * backend shutdown.
 */
void
_PG_init(void)
{
	/* Initialize the global query counter */
    pg_atomic_init_u64(&global_query_counter, 0);

	/* Initialize the unique identifier for this FDW instance */
	fdw_instance_pid = MyProcPid;

	on_proc_exit(&mysql_fdw_exit, PointerGetDatum(NULL));
}

/*
 * mysql_fdw_exit
 * 		Exit callback function.
 */
static void
mysql_fdw_exit(int code, Datum arg)
{
	// TODO (msb): Clean/close connections?
	// mysql_cleanup_connection();
}

/*
 * Foreign-data wrapper handler function: return
 * a struct with pointers to my callback routines.
 */
Datum
mysql_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	fdwroutine->GetForeignRelSize = mysqlGetForeignRelSize;
	fdwroutine->GetForeignPaths = mysqlGetForeignPaths;
	fdwroutine->GetForeignPlan = mysqlGetForeignPlan;
	fdwroutine->BeginForeignScan = mysqlBeginForeignScan;
	fdwroutine->IterateForeignScan = mysqlIterateForeignScan;
	fdwroutine->ReScanForeignScan = mysqlReScanForeignScan;
	fdwroutine->EndForeignScan = mysqlEndForeignScan;

	/* Functions for updating foreign tables */
	fdwroutine->AddForeignUpdateTargets = mysqlAddForeignUpdateTargets;
	fdwroutine->PlanForeignModify = mysqlPlanForeignModify;
	fdwroutine->BeginForeignModify = mysqlBeginForeignModify;
	fdwroutine->ExecForeignInsert = mysqlExecForeignInsert;
	fdwroutine->ExecForeignUpdate = mysqlExecForeignUpdate;
	fdwroutine->ExecForeignDelete = mysqlExecForeignDelete;
	fdwroutine->EndForeignModify = mysqlEndForeignModify;

	/* Function for EvalPlanQual rechecks */
	fdwroutine->RecheckForeignScan = mysqlRecheckForeignScan;

	/* Support functions for EXPLAIN */
	// fdwroutine->ExplainForeignScan = mysqlExplainForeignScan;

	// /* Support functions for ANALYZE */
	// fdwroutine->AnalyzeForeignTable = mysqlAnalyzeForeignTable;

	/* Support functions for IMPORT FOREIGN SCHEMA */
	fdwroutine->ImportForeignSchema = mysqlImportForeignSchema;

	/* Partition routing and/or COPY from */
	fdwroutine->BeginForeignInsert = mysqlBeginForeignInsert;
	fdwroutine->EndForeignInsert = mysqlEndForeignInsert;

	/* Support functions for join push-down */
	fdwroutine->GetForeignJoinPaths = mysqlGetForeignJoinPaths;

	/* Support functions for upper relation push-down */
	fdwroutine->GetForeignUpperPaths = mysqlGetForeignUpperPaths;

	PG_RETURN_POINTER(fdwroutine);
}

/* Define the hash table entry structure */
typedef struct ColnameAttnumEntry
{
    char    colname[NAMEDATALEN];
    int     attnum;
} ColnameAttnumEntry;

static HTAB *
build_colname_to_attnum_map(TupleDesc tupleDesc)
{
    HASHCTL     ctl;
    HTAB       *colname_to_attnum;
    int         i;

    /* Initialize hash table */
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = NAMEDATALEN;
    ctl.entrysize = sizeof(ColnameAttnumEntry);
    ctl.hcxt = CurrentMemoryContext;
    colname_to_attnum = hash_create("colname_to_attnum_map",
                                    tupleDesc->natts,
                                    &ctl,
                                    HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

    /* Populate the hash table */
    for (i = 0; i < tupleDesc->natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);
        char   *attname = NameStr(attr->attname);
        bool    found;
        ColnameAttnumEntry *entry;

        entry = (ColnameAttnumEntry *) hash_search(colname_to_attnum,
                                                   attname,
                                                   HASH_ENTER,
                                                   &found);
        entry->attnum = i;

		elog(WARNING, "Adding column %s with attnum %d", attname, i);
    }

    return colname_to_attnum;
}

/*
 * mysqlBeginForeignScan
 *      Initiate access to the database, including handling of joins and GROUP BY pushdowns.
 */
static void
mysqlBeginForeignScan(ForeignScanState *node, int eflags)
{
    MySQLFdwExecState  *festate;
    EState     		   *estate = node->ss.ps.state;
    ForeignScan 	   *fsplan = (ForeignScan *) node->ss.ps.plan;
    RangeTblEntry 	   *rte;
    mysql_opt  		   *options;
    Oid         		userid;
    ForeignServer 	   *server;
    UserMapping 	   *user;
    ForeignTable 	   *table;
    int         		rtindex;
	int					numParams;
	ListCell   		   *lc;
    List       		   *fdw_private = fsplan->fdw_private;
	TupleTableSlot 	   *tupleSlot = node->ss.ss_ScanTupleSlot;
	TupleDesc			tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	int 				bindnum = 0;
	char				unique_plan_id[64];

    elog(DEBUG1, "mysqlBeginForeignScan: Begin");

    /*
     * Allocate and initialize MySQLFdwExecState.
     */
    festate = (MySQLFdwExecState *) palloc0(sizeof(MySQLFdwExecState));
    node->fdw_state = (void *) festate;

	/*
	 * If whole-row references are involved in pushed down join extract the
	 * information required to construct those.
	 */
	if (list_length(fdw_private) >= mysqlFdwPrivateScanTList)
	{
		List	   *whole_row_lists = list_nth(fdw_private,
											   mysqlFdwPrivateWholeRowLists);
		List	   *scan_tlist = list_nth(fdw_private,
										  mysqlFdwPrivateScanTList);
		TupleDesc	scan_tupdesc = ExecTypeFromTL(scan_tlist);

		mysql_build_whole_row_constr_info(festate, tupleDescriptor,
										  fsplan->fs_relids,
										  list_length(node->ss.ps.state->es_range_table),
										  whole_row_lists, scan_tlist,
										  fsplan->fdw_scan_tlist);

		/* Change tuple descriptor to match the result from foreign server. */
		tupleDescriptor = scan_tupdesc;
	}

    /*
     * Identify which user to do the remote access as.
     */
    if (fsplan->scan.scanrelid > 0)
        rtindex = fsplan->scan.scanrelid;
    else
#if PG_VERSION_NUM >= 160000
        rtindex = bms_next_member(fsplan->fs_base_relids, -1);
#else
        rtindex = bms_next_member(fsplan->fs_relids, -1);
#endif

#if PG_VERSION_NUM >= 160000
    rte = exec_rt_fetch(rtindex, estate);
    userid = fsplan->checkAsUser ? fsplan->checkAsUser : GetUserId();
#else
    rte = rt_fetch(rtindex, estate->es_range_table);
    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
#endif

    /* Get info about foreign table. */
    table = GetForeignTable(rte->relid);
    server = GetForeignServer(table->serverid);
    user = GetUserMapping(userid, server->serverid);

    /* Fetch the options. */
    options = mysql_get_options(rte->relid, true);

    /* Establish connection to DAS (Data Access Service). */
    festate->das = das_get_connection(server, user, options);

    /* Stash away the state info we have already. */
    festate->query = strVal(list_nth(fsplan->fdw_private,
                                     mysqlFdwScanPrivateSelectSql));
    festate->retrieved_attrs = list_nth(fsplan->fdw_private,
                                        mysqlFdwScanPrivateRetrievedAttrs);
	festate->plan_id = DatumGetInt64(((Const *) list_nth(fsplan->fdw_private, mysqlFdwScanPrivateUniquePlanId))->constvalue);
	elog(WARNING, "got here4.3  with plan_id = %lu", festate->plan_id);											   
    festate->attinmeta = TupleDescGetAttInMetadata(tupleDescriptor);
    // festate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
    //                                           "mysql_fdw temporary data",
    //                                           ALLOCSET_DEFAULT_SIZES);

    /* Prepare and send the remote SQL query. */
    char *remote_sql_query = mysql_remove_quotes(pstrdup(festate->query));
    elog(WARNING, "About to send query: %s", remote_sql_query);

    /* Initialize the iterator. */
	snprintf(unique_plan_id, sizeof(unique_plan_id), "%d-%lu", fdw_instance_pid, festate->plan_id);
    festate->iterator = sql_query_iterator_init(options, remote_sql_query, unique_plan_id);
    if (!festate->iterator)
        ereport(ERROR,
                (errmsg("Failed to initialize SQL query iterator")));

	/* Prepare for output conversion of parameters used in remote query. */
	numParams = list_length(fsplan->fdw_exprs);
	festate->numParams = numParams;
	if (numParams > 0)
		prepare_query_params((PlanState *) node,
							 fsplan->fdw_exprs,
							 numParams,
							 &festate->param_flinfo,
							 &festate->param_exprs,
							 &festate->param_values,
							 &festate->param_types);

	/* Store the attribute numbers of the columns to be fetched. */
	festate->attnums = (int *) palloc0(tupleDescriptor->natts * sizeof(int));
	festate->pgtypes = (Oid *) palloc0(tupleDescriptor->natts * sizeof(Oid));
	festate->pgtypmods = (int32 *) palloc0(tupleDescriptor->natts * sizeof(int32));
	foreach(lc, festate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc) - 1;
		Oid			pgtype = TupleDescAttr(tupleDescriptor, attnum)->atttypid;
		int32		pgtypmod = TupleDescAttr(festate->attinmeta->tupdesc, attnum)->atttypmod;

		if (TupleDescAttr(tupleDescriptor, attnum)->attisdropped)
			continue;

		festate->attnums[bindnum] = attnum;
		festate->pgtypes[bindnum] = pgtype;
		festate->pgtypmods[bindnum] = pgtypmod;
		bindnum++;
	}

	elog(WARNING, "End of mysqlBeginForeignScan");
}

/*
 * Logs the details of each attribute in the TupleDesc.
 */
static void LogTupleDescDetails(TupleDesc tupdesc) {
    int natts = tupdesc->natts;

    elog(WARNING, "Logging TupleDesc Details:");
    for (int i = 0; i < natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        
        // Get the type name from the type OID
        char *type_name = format_type_be(attr->atttypid);
        
        // Log attribute details
        elog(WARNING, "Column %d: Name='%s', Type='%s' (OID=%u), Typmod=%d, AttNo=%d, IsDropped=%d",
             i + 1, // Attribute numbers start at 1
             NameStr(attr->attname),
             type_name,
             attr->atttypid,
             attr->atttypmod,
             attr->attnum,
             attr->attisdropped);
        
        // Free the type name string allocated by format_type_be
        pfree(type_name);
    }
}

/*
 * mysqlIterateForeignScan
 *      Iterate and get the rows one by one from MySQL, handling joins and GROUP BY pushdowns.
 */
static TupleTableSlot *
mysqlIterateForeignScan(ForeignScanState *node)
{
    MySQLFdwExecState *festate = (MySQLFdwExecState *) node->fdw_state;
    TupleTableSlot  *tupleSlot = node->ss.ss_ScanTupleSlot;
    Datum           *dvalues;
    bool            *nulls;
    HeapTuple        tup;
	AttInMetadata *attinmeta = festate->attinmeta;
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
    List	   *fdw_private = fsplan->fdw_private;
	int i;

	int natts = festate->attinmeta->tupdesc->natts;

    elog(WARNING, "IterateForeignScan begin");

    ExecClearTuple(tupleSlot);

    /* Initialize values and nulls */

	dvalues = palloc0(natts * sizeof(Datum));
	nulls = palloc(natts * sizeof(bool));
	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, natts * sizeof(bool));

    /* Fetch the next record */
    bool has_next = sql_query_iterator_next(festate->iterator, festate->attnums, dvalues, nulls, festate->pgtypes, festate->pgtypmods);

    if (has_next)
    {
        elog(WARNING, "Record found");

		if (list_length(fdw_private) >= mysqlFdwPrivateScanTList)
		{
			elog(WARNING, "Whole-row references are involved in pushed down join");

			/* Construct tuple with whole-row references. */
			tup = mysql_get_tuple_with_whole_row(festate, dvalues, nulls);
		}
		else
		{
			elog(WARNING, "Whole-row references are not involved in pushed down join");

			LogTupleDescDetails(attinmeta->tupdesc);

			/* Form the Tuple using Datums */
			tup = heap_form_tuple(attinmeta->tupdesc, dvalues, nulls);

			elog(WARNING, "Tuple formed");
		}

        if (tup)
        {
            ExecStoreHeapTuple(tup, tupleSlot, false);
            elog(WARNING, "Tuple stored");
        }
        else
        {
            elog(WARNING, "Failed to form tuple");
        }

		/*
		 * Release locally palloc'd space and values of pass-by-reference
		 * datums, as well.
		 */
		for (i = 0; i < natts; i++)
		{
			if (dvalues[i] && !TupleDescAttr(attinmeta->tupdesc, i)->attbyval)
				pfree(DatumGetPointer(dvalues[i]));
		}
		pfree(dvalues);
		pfree(nulls);
	}

	return tupleSlot;
}

/*
 * mysqlEndForeignScan
 *      Finish scanning foreign table and dispose of objects used for this scan
 */
static void
mysqlEndForeignScan(ForeignScanState *node)
{
    MySQLFdwExecState *festate = (MySQLFdwExecState *) node->fdw_state;

    elog(WARNING, "EndForeignScan begin");

    if (festate)
    {
        /* Close the iterator */
        if (festate->iterator)
        {
            sql_query_iterator_close(festate->iterator);
            festate->iterator = NULL;
        }
    }

    elog(WARNING, "EndForeignScan end");
}


/*
 * mysqlReScanForeignScan
 * 		Rescan table, possibly with new parameters
 */
static void
mysqlReScanForeignScan(ForeignScanState *node)
{
	MySQLFdwExecState *festate = (MySQLFdwExecState *) node->fdw_state;

	/*
	 * Set the query_executed flag to false so that the query will be executed
	 * in mysqlIterateForeignScan().
	 */
	// festate->query_executed = false;

}

/*
 * mysqlGetForeignRelSize
 * 		Create a FdwPlan for a scan on the foreign table
 */
static void
mysqlGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
					   Oid foreigntableid)
{
	double		rows;
	double		width;
	Bitmapset  *attrs_used = NULL;
	// mysql_opt  *options;
	Oid			userid = GetUserId();
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;
	MySQLFdwRelationInfo *fpinfo;
	ListCell   *lc;
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	const char *database;
	const char *relname;
	const char *refname;
	StringInfoData sql;
	List	   *retrieved_attrs = NULL;

	fpinfo = (MySQLFdwRelationInfo *) palloc0(sizeof(MySQLFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Base foreign tables need to be push down always. */
	fpinfo->pushdown_safe = true;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	elog(WARNING, "got here1");

	/* Fetch options */
	fpinfo->options = mysql_get_options(foreigntableid, true);

	elog(WARNING, "got here2");
	elog(WARNING, "das_url = %s", fpinfo->options->das_url);
	elog(WARNING, "das_id = %s", fpinfo->options->das_id);
	elog(WARNING, "das_type = %s", fpinfo->options->das_type);
	/* Connect to the server */
	// conn = mysql_get_connection(server, user, options);
	fpinfo->das = das_get_connection(server, user, fpinfo->options);

	elog(WARNING, "got here3");

	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &attrs_used);

	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		if (mysql_is_foreign_expr(root, baserel, ri->clause, false))
			fpinfo->remote_conds = lappend(fpinfo->remote_conds, ri);
		else
			fpinfo->local_conds = lappend(fpinfo->local_conds, ri);
	}

	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);

	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}


	initStringInfo(&sql);

	mysql_deparse_select_stmt_for_rel(&sql, root, baserel, NULL,
										fpinfo->remote_conds, NULL, false,
										false, &retrieved_attrs, NULL);

	elog(WARNING, "got here3.1 with sql = %s", sql.data);		

	get_query_estimate(fpinfo->options, sql.data, &rows, &width);

	baserel->rows = rows;
	baserel->tuples = rows;

	elog(WARNING, "got here4");

	/* Set the unique identifier */
	fpinfo->plan_id = pg_atomic_fetch_add_u64(&global_query_counter, 1);
	elog(WARNING, "got here4.1  with plan_id = %lu", fpinfo->plan_id);

	/*
	 * Set the name of relation in fpinfo, while we are constructing it here.
	 * It will be used to build the string describing the join relation in
	 * EXPLAIN output.  We can't know whether VERBOSE option is specified or
	 * not, so always schema-qualify the foreign table name.
	 */
	fpinfo->relation_name = makeStringInfo();
	// database = fpinfo->options->svr_database;
	relname = get_rel_name(foreigntableid);
	refname = rte->eref->aliasname;
	// appendStringInfo(fpinfo->relation_name, "%s.%s",
					//  quote_identifier(database), quote_identifier(relname));
	appendStringInfo(fpinfo->relation_name, "%s",
					 quote_identifier(relname));	
	if (*refname && strcmp(refname, relname) != 0)
		appendStringInfo(fpinfo->relation_name, " %s",
						 quote_identifier(rte->eref->aliasname));

	elog(WARNING, "got here5");						 
}

/*
 * mysqlEstimateCosts
 * 		Estimate the remote query cost
 */
static void
mysqlEstimateCosts(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost,
				   Cost *total_cost, Oid foreigntableid)
{
	*startup_cost = 25;

	*total_cost = baserel->rows + *startup_cost;
}

/*
 * mysqlGetForeignPaths
 * 		Get the foreign paths
 */
static void
mysqlGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
					 Oid foreigntableid)
{
	Cost		startup_cost;
	Cost		total_cost;
	DAS 	   *das;

	elog(WARNING, "getPaths here1");

	// /* Estimate costs */
	mysqlEstimateCosts(root, baserel, &startup_cost, &total_cost,
					   foreigntableid);

					   elog(WARNING, "getPaths here1.1");

	/* Create a ForeignPath node and add it as only possible path */
#if PG_VERSION_NUM >= 170000
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 NULL,	/* default pathtarget */
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,	/* no pathkeys */
									 baserel->lateral_relids,
									 NULL,	/* no extra plan */
									 NIL, /* no fdw_restrictinfo list */
									 NIL));	/* no fdw_private data */
#else
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 NULL,	/* default pathtarget */
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,	/* no pathkeys */
									 baserel->lateral_relids,
									 NULL,	/* no extra plan */
									 NIL));	/* no fdw_private list */
#endif

elog(WARNING, "getPaths here1.2");

	das = ((MySQLFdwRelationInfo *) baserel->fdw_private)->das;

	if (das->orderby_supported) {
		/* Add paths with pathkeys */
#if PG_VERSION_NUM >= 170000
		mysql_add_paths_with_pathkeys(root, baserel, NULL, startup_cost,
									  total_cost, NIL);
#else
		mysql_add_paths_with_pathkeys(root, baserel, NULL, startup_cost,
									  total_cost);
#endif
	}

	elog(WARNING, "getPaths here2");
}


/*
 * mysqlGetForeignPlan
 * 		Get a foreign scan plan node
 */
static ForeignScan *
mysqlGetForeignPlan(PlannerInfo *root, RelOptInfo *foreignrel,
					Oid foreigntableid, ForeignPath *best_path,
					List *tlist, List *scan_clauses, Plan *outer_plan)
{
	MySQLFdwRelationInfo *fpinfo = (MySQLFdwRelationInfo *) foreignrel->fdw_private;
	Index		scan_relid;
	List	   *fdw_private;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *remote_conds = NIL;
	StringInfoData sql;
	List	   *retrieved_attrs;
	ListCell   *lc;
	List	   *scan_var_list;
	List	   *fdw_scan_tlist = NIL;
	List	   *whole_row_lists = NIL;
	bool		has_final_sort = false;
	bool		has_limit = false;

	elog(WARNING, "plan here1");
	/*
	 * Get FDW private data created by mysqlGetForeignUpperPaths(), if any.
	 */
	if (best_path->fdw_private)
	{
		has_final_sort = intVal(list_nth(best_path->fdw_private,
										 FdwPathPrivateHasFinalSort));
		has_limit = intVal(list_nth(best_path->fdw_private,
									FdwPathPrivateHasLimit));
	}

	if (foreignrel->reloptkind == RELOPT_BASEREL ||
		foreignrel->reloptkind == RELOPT_OTHER_MEMBER_REL)
		scan_relid = foreignrel->relid;
	else
	{
		scan_relid = 0;
		Assert(!scan_clauses);

		remote_conds = fpinfo->remote_conds;
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);
	}

	elog(WARNING, "plan here2");

	/*
	 * Separate the scan_clauses into those that can be executed remotely and
	 * those that can't.  baserestrictinfo clauses that were previously
	 * determined to be safe or unsafe are shown in fpinfo->remote_conds and
	 * fpinfo->local_conds.  Anything else in the scan_clauses list will be a
	 * join clause, which we have to check for remote-safety.
	 *
	 * This code must match "extract_actual_clauses(scan_clauses, false)"
	 * except for the additional decision about remote versus local execution.
	 * Note however that we only strip the RestrictInfo nodes from the
	 * local_exprs list, since appendWhereClause expects a list of
	 * RestrictInfos.
	 */
	foreach(lc, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		Assert(IsA(rinfo, RestrictInfo));

		/* Ignore any pseudoconstants, they're dealt with elsewhere */
		if (rinfo->pseudoconstant)
			continue;

		if (list_member_ptr(fpinfo->remote_conds, rinfo))
			remote_conds = lappend(remote_conds, rinfo);
		else if (list_member_ptr(fpinfo->local_conds, rinfo))
			local_exprs = lappend(local_exprs, rinfo->clause);
		else if (mysql_is_foreign_expr(root, foreignrel, rinfo->clause, false))
			remote_conds = lappend(remote_conds, rinfo);
		else
			local_exprs = lappend(local_exprs, rinfo->clause);
	}

	if (IS_UPPER_REL(foreignrel))
		scan_var_list = pull_var_clause((Node *) fpinfo->grouped_tlist,
										PVC_RECURSE_AGGREGATES);
	else
		scan_var_list = pull_var_clause((Node *) foreignrel->reltarget->exprs,
										PVC_RECURSE_PLACEHOLDERS);

	/* System attributes are not allowed. */
	foreach(lc, scan_var_list)
	{
		Var		   *var = lfirst(lc);
		const FormData_pg_attribute *attr;

		Assert(IsA(var, Var));

		if (var->varattno >= 0)
			continue;

		attr = SystemAttributeDefinition(var->varattno);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_COLUMN_NAME_NOT_FOUND),
				 errmsg("system attribute \"%s\" can't be fetched from remote relation",
						attr->attname.data)));
	}

	elog(WARNING, "plan here3");

	if (IS_JOIN_REL(foreignrel))
	{
		scan_var_list = list_concat_unique(NIL, scan_var_list);

		scan_var_list = list_concat_unique(scan_var_list,
										   pull_var_clause((Node *) local_exprs,
														   PVC_RECURSE_PLACEHOLDERS));

		/*
		 * For join relations, planner needs targetlist, which represents the
		 * output of ForeignScan node. Prepare this before we modify
		 * scan_var_list to include Vars required by whole row references, if
		 * any.  Note that base foreign scan constructs the whole-row
		 * reference at the time of projection.  Joins are required to get
		 * them from the underlying base relations.  For a pushed down join
		 * the underlying relations do not exist, hence the whole-row
		 * references need to be constructed separately.
		 */
		fdw_scan_tlist = add_to_flat_tlist(NIL, scan_var_list);

		/*
		 * MySQL does not allow row value constructors to be part of SELECT
		 * list.  Hence, whole row reference in join relations need to be
		 * constructed by combining all the attributes of required base
		 * relations into a tuple after fetching the result from the foreign
		 * server.  So adjust the targetlist to include all attributes for
		 * required base relations.  The function also returns list of Var
		 * node lists required to construct the whole-row references of the
		 * involved relations.
		 */
		scan_var_list = mysql_adjust_whole_row_ref(root, scan_var_list,
												   &whole_row_lists,
												   foreignrel->relids);

		if (outer_plan)
		{
			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins.  Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(!IS_UPPER_REL(foreignrel));

			foreach(lc, local_exprs)
			{
				Node	   *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.  (They might also be
				 * in the mergequals or hashquals, but we can't touch those
				 * without breaking the plan.)
				 */
				if (IsA(outer_plan, NestLoop) ||
					IsA(outer_plan, MergeJoin) ||
					IsA(outer_plan, HashJoin))
				{
					Join	   *join_plan = (Join *) outer_plan;

					if (join_plan->jointype == JOIN_INNER)
						join_plan->joinqual = list_delete(join_plan->joinqual,
														  qual);
				}
			}
		}
	}
	else if (IS_UPPER_REL(foreignrel))
	{
		/*
		 * scan_var_list should have expressions and not TargetEntry nodes.
		 * However grouped_tlist created has TLEs, thus retrieve them into
		 * scan_var_list.
		 */
		scan_var_list = list_concat_unique(NIL,
										   get_tlist_exprs(fpinfo->grouped_tlist,
														   false));

		/*
		 * The targetlist computed while assessing push-down safety represents
		 * the result we expect from the foreign server.
		 */
		fdw_scan_tlist = fpinfo->grouped_tlist;
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);
	}

	elog(WARNING, "plan here4");

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	mysql_deparse_select_stmt_for_rel(&sql, root, foreignrel, scan_var_list,
									  remote_conds, best_path->path.pathkeys,
									  has_final_sort, has_limit, &retrieved_attrs,
									  &params_list);

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwScanPrivateIndex, above.
	 */
	elog(WARNING, "got here4.2  with plan_id = %lu", fpinfo->plan_id);
	fdw_private = list_make3(
		makeString(sql.data),
		retrieved_attrs,
		makeConst(INT8OID, -1, InvalidOid, 8, Int64GetDatum(fpinfo->plan_id), false, true)
	);
	if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
	{
		fdw_private = lappend(fdw_private,
							  makeString(fpinfo->relation_name->data));

		/*
		 * To construct whole row references we need:
		 *
		 * 	1.	The lists of Var nodes required for whole-row references of
		 * 		joining relations
		 * 	2.	targetlist corresponding the result expected from the foreign
		 * 		server.
		 */
		if (whole_row_lists)
		{
			fdw_private = lappend(fdw_private, whole_row_lists);
			fdw_private = lappend(fdw_private,
								  add_to_flat_tlist(NIL, scan_var_list));
		}
	}

	elog(WARNING, "plan here5");

	/*
	 * Create the ForeignScan node from target list, local filtering
	 * expressions, remote parameter expressions, and FDW private information.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist, local_exprs, scan_relid, params_list,
							fdw_private, fdw_scan_tlist, NIL, outer_plan);
}

static List *
mysqlPlanForeignModify(PlannerInfo *root,
					   ModifyTable *plan,
					   Index resultRelation,
					   int subplan_index)
{
	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
	List	   *targetAttrs = NIL;
	Oid			foreignTableId;
	bool		doNothing = false;
	char	   *attname;
	int			attnum;
	mysql_opt  *options;

	elog(WARNING, "planForeignModify begin");

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
#if PG_VERSION_NUM < 130000
	rel = heap_open(rte->relid, NoLock);
#else
	rel = table_open(rte->relid, NoLock);
#endif

	foreignTableId = RelationGetRelid(rel);

	options = mysql_get_options(foreignTableId, true);
	attname = unique_column(options, get_rel_name(foreignTableId));
	attnum = get_attnum(foreignTableId, attname);

	/*
	 * ON CONFLICT DO UPDATE and DO NOTHING case with inference specification
	 * should have already been rejected in the optimizer, as presently there
	 * is no way to recognize an arbiter index on a foreign table.  Only DO
	 * NOTHING is supported without an inference specification.
	 */
	if (plan->onConflictAction == ONCONFLICT_NOTHING)
		doNothing = true;
	else if (plan->onConflictAction != ONCONFLICT_NONE)
		elog(ERROR, "unexpected ON CONFLICT specification: %d",
			 (int) plan->onConflictAction);

	/*
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.  In an UPDATE, if there are BEFORE ROW UPDATE triggers on the
	 * foreign table, we transmit all columns like INSERT; else we transmit
	 * only columns that were explicitly targets of the UPDATE, so as to avoid
	 * unnecessary data transmission.  (We can't do that for INSERT since we
	 * would miss sending default values for columns not listed in the source
	 * statement, and for UPDATE if there are BEFORE ROW UPDATE triggers since
	 * those triggers might change values for non-target columns, in which
	 * case we would miss sending changed values for those columns.)
	 */
	if (operation == CMD_INSERT ||
		(operation == CMD_UPDATE &&
		 rel->trigdesc &&
		 rel->trigdesc->trig_update_before_row))
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			attnum;

		/*
		 * If it is an UPDATE operation, check for row identifier column in
		 * target attribute list by calling getUpdateTargetAttrs().
		 */
		if (operation == CMD_UPDATE)
			getUpdateTargetAttrs(root, rte, attnum);

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{
		targetAttrs = getUpdateTargetAttrs(root, rte, attnum);
		/* We also want the rowid column to be available for the update */
		targetAttrs = lcons_int(attnum, targetAttrs);
	}
	else
		targetAttrs = lcons_int(attnum, targetAttrs);

	if (plan->returningLists)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("RETURNING is not supported by this FDW")));

#if PG_VERSION_NUM < 130000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif

	elog(WARNING, "planForeignModify end");

	elog(WARNING, "attname: %s", attname);

	return list_make2(attname, targetAttrs);

	// return list_make2(makeString(sql.data), targetAttrs);
}

/*
 * mysqlBeginForeignModify
 * 		Begin an insert/update/delete operation on a foreign table
 */
static void
mysqlBeginForeignModify(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo,
						List *fdw_private,
						int subplan_index,
						int eflags)
{
	MySQLFdwExecState *fmstate;
	EState	   *estate = mtstate->ps.state;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	AttrNumber	n_params;
	Oid			typefnoid = InvalidOid;
	bool		isvarlena = false;
	ListCell   *lc;
	Oid			foreignTableId = InvalidOid;
	Oid			userid;
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;
#if PG_VERSION_NUM >= 160000
	ForeignScan *fsplan = (ForeignScan *) mtstate->ps.plan;
#else
	RangeTblEntry *rte;
#endif

	elog(WARNING, "mysqlBeginForeignModify begin");

#if PG_VERSION_NUM >= 160000
	userid = fsplan->checkAsUser ? fsplan->checkAsUser : GetUserId();
#else
	rte = rt_fetch(resultRelInfo->ri_RangeTableIndex, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
#endif

	foreignTableId = RelationGetRelid(rel);

	table = GetForeignTable(foreignTableId);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case. resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Begin constructing MySQLFdwExecState. */
	fmstate = (MySQLFdwExecState *) palloc0(sizeof(MySQLFdwExecState));

	elog(WARNING, "mysqlBeginForeignModify 1");

	fmstate->mysqlFdwOptions = mysql_get_options(foreignTableId, true);

	fmstate->attname = (char *) list_nth(fdw_private, 0); // strVal(list_nth(fdw_private, 0));
	fmstate->retrieved_attrs = (List *) list_nth(fdw_private, 1);

	fmstate->table_name = get_rel_name(foreignTableId);

	elog(WARNING, "mysqlBeginForeignModify 2");

	n_params = list_length(fmstate->retrieved_attrs) + 1;
	fmstate->p_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->p_nums = 0;
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "mysql_fdw temporary data",
											  ALLOCSET_DEFAULT_SIZES);

	elog(WARNING, "mysqlBeginForeignModify 3");

	if (mtstate->operation == CMD_UPDATE)
	{
		// Form_pg_attribute attr;
		// char *attname;
#if PG_VERSION_NUM >= 140000
		Plan	   *subplan = outerPlanState(mtstate)->plan;
#else
		Plan	   *subplan = mtstate->mt_plans[subplan_index]->plan;
#endif

		Assert(subplan != NULL);

		elog(WARNING, "mysqlBeginForeignModify 3.1");
		elog(WARNING, "mysqlBeginForeignModify fmstate attname: %s", fmstate->attname);

		/* Find the rowid resjunk column in the subplan's result */
		fmstate->rowidAttno = ExecFindJunkAttributeInTlist(subplan->targetlist,
														   fmstate->attname);
		if (!AttributeNumberIsValid(fmstate->rowidAttno))
			elog(ERROR, "could not find junk row identifier column");
	}

	elog(WARNING, "mysqlBeginForeignModify 4");

	/* Set up for remaining transmittable parameters */
	foreach(lc, fmstate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc);
		Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel),
											   attnum - 1);

		Assert(!attr->attisdropped);

		getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;
	}
	Assert(fmstate->p_nums <= n_params);

	elog(WARNING, "mysqlBeginForeignModify 5");

	n_params = list_length(fmstate->retrieved_attrs);

	resultRelInfo->ri_FdwState = fmstate;

	elog(WARNING, "mysqlBeginForeignModify end");
}

/*
 * mysqlExecForeignInsert
 * 		Insert one row into a foreign table
 */
static TupleTableSlot *
mysqlExecForeignInsert(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	MySQLFdwExecState *fmstate;
	ListCell   *lc;
	int			n_params;
	MemoryContext oldcontext;
	char		sql_mode[255];
	Oid			foreignTableId = RelationGetRelid(resultRelInfo->ri_RelationDesc);
	int		   *attnums;
	char	  **attnames;
	Datum 	   *values;
	bool	   *nulls;
	Oid		   *pgtypes;
	int32	   *pgtypmods;
	int 		bindnum = 0;

	elog(WARNING, "mysqlExecForeignInsert begin");

	fmstate = (MySQLFdwExecState *) resultRelInfo->ri_FdwState;
	n_params = list_length(fmstate->retrieved_attrs);

	fmstate->mysqlFdwOptions = mysql_get_options(foreignTableId, true);

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	attnums = (int *) palloc0(sizeof(int) * n_params);
	attnames = (char **) palloc0(sizeof(char *) * n_params);
	values = (Datum *) palloc0(sizeof(Datum) * n_params);
	nulls = (bool *) palloc0(sizeof(bool) * n_params);
	pgtypes = (Oid *) palloc0(sizeof(Oid) * n_params);
	pgtypmods = (int32 *) palloc0(sizeof(int32) * n_params);

	foreach(lc, fmstate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc) - 1;
		Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, attnum);
		Oid			type = attr->atttypid;
		int32		typmod = attr->atttypmod;
		char 	   *column_name = NameStr(attr->attname);
		Datum		value;
			
		value = slot_getattr(slot, attnum + 1, &nulls[bindnum]);

		attnums[bindnum] = attnum;
		attnames[bindnum] = column_name;
		values[bindnum] = value;
		pgtypes[bindnum] = type;
		pgtypmods[bindnum] = typmod;

		bindnum++;
	}

	elog(WARNING, "mysqlExecForeignInsert 1 num_columns: %d", bindnum);

	insert_row(fmstate->mysqlFdwOptions, fmstate->table_name, bindnum, attnums, attnames, values, nulls, pgtypes, pgtypmods);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextReset(fmstate->temp_cxt);

	elog(WARNING, "mysqlExecForeignInsert end");

	return slot;
}

static TupleTableSlot *
mysqlExecForeignUpdate(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	MySQLFdwExecState *fmstate = (MySQLFdwExecState *) resultRelInfo->ri_FdwState;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	
	int		   *attnums;
	char	  **attnames;
	Datum 	   *values;
	bool	   *nulls;
	Oid		   *pgtypes;
	int32	   *pgtypmods;
	Oid			foreignTableId = RelationGetRelid(rel);
	bool		is_null = false;
	ListCell   *lc;
	int			bindnum = 0;
	Oid			typeoid;
	Datum		value;
	int			n_params;
	Datum		new_value;
	Oid 		new_pgtype;
	int32 		new_pgtypmod;
	HeapTuple	tuple;
	Form_pg_attribute attr;
	bool		found_row_id_col = false;
	int 		unique_attnum;
#if PG_VERSION_NUM >= 140000
	TupleDesc	tupdesc = RelationGetDescr(rel);
#endif

	unique_attnum = get_attnum(foreignTableId, fmstate->attname);
	elog(WARNING, "unique_attnum: %d", unique_attnum);

	n_params = list_length(fmstate->retrieved_attrs);

	attnums = (int *) palloc0(sizeof(int) * n_params);
	attnames = (char **) palloc0(sizeof(char *) * n_params);
	values = (Datum *) palloc0(sizeof(Datum) * n_params);
	nulls = (bool *) palloc0(sizeof(bool) * n_params);
	pgtypes = (Oid *) palloc0(sizeof(Oid) * n_params);
	pgtypmods = (int32 *) palloc0(sizeof(int32) * n_params);

	/* Bind the values */
	foreach(lc, fmstate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc) - 1;
		Oid			type;
		int32		typmod;
		Form_pg_attribute attr;
		char	   *column_name;

		elog(WARNING, "attnum: %d", attnum);

		/*
		 * The first attribute cannot be in the target list attribute.  Set
		 * the found_row_id_col to true once we find it so that we can fetch
		 * the value later.
		 */
		if (attnum == (unique_attnum - 1))
		{
			found_row_id_col = true;
			elog(WARNING, "found_row_id_col: %d", found_row_id_col);
			continue;
		}

#if PG_VERSION_NUM >= 140000
		/* Ignore generated columns; they are set to DEFAULT. */
		if (TupleDescAttr(tupdesc, attnum)->attgenerated)
			continue;
#endif

		attr = TupleDescAttr(slot->tts_tupleDescriptor, attnum);
	
		column_name = NameStr(attr->attname);
		type = attr->atttypid;
		typmod = attr->atttypmod;
		value = slot_getattr(slot, attnum + 1, &nulls[bindnum]);

		// mysql_bind_sql_var(type, bindnum, value, mysql_bind_buffer,
		// 				   &isnull[bindnum]);

		attnums[bindnum] = attnum;
		attnames[bindnum] = column_name;
		values[bindnum] = value;
		pgtypes[bindnum] = type;
		pgtypmods[bindnum] = typmod;

		bindnum++;
	}

	/*
	 * Since we add a row identifier column in the target list always, so
	 * found_row_id_col flag should be true.
	 */
	if (!found_row_id_col)
		elog(ERROR, "missing row identifier column value in UPDATE");

	new_value = slot_getattr(slot, unique_attnum, &is_null);
	new_pgtype = TupleDescAttr(slot->tts_tupleDescriptor, unique_attnum - 1)->atttypid;
	new_pgtypmod = TupleDescAttr(slot->tts_tupleDescriptor, unique_attnum - 1)->atttypmod;

	/*
	 * Get the row identifier column value that was passed up as a resjunk
	 * column and compare that value with the new value to identify if that
	 * value is changed.
	 */
	value = ExecGetJunkAttribute(planSlot, fmstate->rowidAttno, &is_null);

	tuple = SearchSysCache2(ATTNUM,
							ObjectIdGetDatum(foreignTableId),
							Int16GetDatum(1));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for attribute %d of relation %u",
			 1, foreignTableId);

	attr = (Form_pg_attribute) GETSTRUCT(tuple);
	typeoid = attr->atttypid;

	if (DatumGetPointer(new_value) != NULL && DatumGetPointer(value) != NULL)
	{
		Datum		n_value = new_value;
		Datum		o_value = value;

		/* If the attribute type is varlena then need to detoast the datums. */
		if (attr->attlen == -1)
		{
			n_value = PointerGetDatum(PG_DETOAST_DATUM(new_value));
			o_value = PointerGetDatum(PG_DETOAST_DATUM(value));
		}

		if (!datumIsEqual(o_value, n_value, attr->attbyval, attr->attlen))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("row identifier column update is not supported")));

		/* Free memory if it's a copy made above */
		if (DatumGetPointer(n_value) != DatumGetPointer(new_value))
			pfree(DatumGetPointer(n_value));
		if (DatumGetPointer(o_value) != DatumGetPointer(value))
			pfree(DatumGetPointer(o_value));
	}
	else if (!(DatumGetPointer(new_value) == NULL &&
			   DatumGetPointer(value) == NULL))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("row identifier column update is not supported")));

	ReleaseSysCache(tuple);

	update_row(
		fmstate->mysqlFdwOptions, fmstate->table_name,
		new_value, new_pgtype, new_pgtypmod,
	 	bindnum, attnums, attnames, values, nulls, pgtypes, pgtypmods
	);


	/* Return NULL if nothing was updated on the remote end */
	return slot;
}

/*
 * mysqlAddForeignUpdateTargets
 * 		Add column(s) needed for update/delete on a foreign table, we are
 * 		using first column as row identification column, so we are adding
 * 		that into target list.
 */
#if PG_VERSION_NUM >= 140000
static void
mysqlAddForeignUpdateTargets(PlannerInfo *root,
							 Index rtindex,
							 RangeTblEntry *target_rte,
							 Relation target_relation)
#else
static void
mysqlAddForeignUpdateTargets(Query *parsetree,
							 RangeTblEntry *target_rte,
							 Relation target_relation)
#endif
{
	Var		   *var;
	const char *attrname;
	int			attnum;
	char	   *attname; // (msb): I think this is the same as attrname, but I'm resolving it again
	mysql_opt  *options;
	Oid 		foreignTableId = RelationGetRelid(target_relation);
#if PG_VERSION_NUM < 140000
	TargetEntry *tle;
#endif

	elog(WARNING, "mysqlAddForeignUpdateTargets begin");

	options = mysql_get_options(foreignTableId, true);
	attname = unique_column(options, get_rel_name(foreignTableId));
	attnum = get_attnum(foreignTableId, attname);

	/*
	 * What we need is the rowid which is the first column
	 */
	Form_pg_attribute attr =
		TupleDescAttr(RelationGetDescr(target_relation), attnum - 1);

	/* Make a Var representing the desired value */
#if PG_VERSION_NUM >= 140000
	var = makeVar(rtindex,
#else
	var = makeVar(parsetree->resultRelation,
#endif
				  attnum,
				  attr->atttypid,
				  attr->atttypmod,
				  InvalidOid,
				  0);

	/* Get name of the row identifier column */
	attrname = NameStr(attr->attname);

#if PG_VERSION_NUM >= 140000
	/* Register it as a row-identity column needed by this target rel */
	add_row_identity_var(root, var, rtindex, attrname);
#else
	/* Wrap it in a TLE with the right name ... */
	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname), true);

	/* ... and add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);
#endif

	elog(WARNING, "mysqlAddForeignUpdateTargets end");
}

int32 get_attribute_typmod(Oid foreignTableId, AttrNumber unique_attnum)
{
    HeapTuple attTuple;
    Form_pg_attribute attForm;
    int32 typmod;

    // Search the pg_attribute cache for the specific attribute
    attTuple = SearchSysCache2(ATTNUM,
                               ObjectIdGetDatum(foreignTableId),
                               Int16GetDatum(unique_attnum));

    if (!HeapTupleIsValid(attTuple))
    {
        ereport(ERROR,
                (errmsg("cache lookup failed for attribute %d of relation %u",
                        unique_attnum, foreignTableId)));
    }

    // Get the attribute form from the tuple
    attForm = (Form_pg_attribute) GETSTRUCT(attTuple);

    // Extract the typmod
    typmod = attForm->atttypmod;

    // Release the cache
    ReleaseSysCache(attTuple);

    return typmod;
}

/*
 * mysqlExecForeignDelete
 * 		Delete one row from a foreign table
 */
static TupleTableSlot *
mysqlExecForeignDelete(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	MySQLFdwExecState *fmstate = (MySQLFdwExecState *) resultRelInfo->ri_FdwState;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	// MYSQL_BIND *mysql_bind_buffer;
	Oid			foreignTableId = RelationGetRelid(rel);
	bool		is_null = false;
	Oid			typeoid;
	Datum		value;
	int 		unique_attnum;
	int32		typemod;

	unique_attnum = get_attnum(foreignTableId, fmstate->attname);

	// mysql_bind_buffer = (MYSQL_BIND *) palloc(sizeof(MYSQL_BIND));

	/* Get the id that was passed up as a resjunk column */
	value = ExecGetJunkAttribute(planSlot, unique_attnum, &is_null);
	typeoid = get_atttype(foreignTableId, unique_attnum);
	typemod = get_attribute_typmod(foreignTableId, unique_attnum);

	elog(WARNING, "is_null: %d value: %d", is_null, DatumGetInt32(value));

	delete_row(fmstate->mysqlFdwOptions, fmstate->table_name, value, typeoid, typemod);

	/* Return NULL if nothing was updated on the remote end */
	return slot;
}

/*
 * mysqlEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
static void
mysqlEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo)
{
}

/*
 * mysqlImportForeignSchema
 * 		Import a foreign schema (9.5+)
 */
static List *
mysqlImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	List	   *commands = NIL;
	bool		import_default = false;
	bool		import_not_null = true;
	bool		import_enum_as_text = false;
	ForeignServer *server;
	UserMapping *user;
	mysql_opt  *options;
	DAS *das;
	StringInfoData buf;
	ListCell   *lc;
#if PG_VERSION_NUM >= 140000
	bool		import_generated = true;
#endif

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	server = GetForeignServer(serverOid);
	user = GetUserMapping(GetUserId(), server->serverid);

	elog(WARNING, "starting1");
	/* Fetch the options. */
    options = mysql_get_options(serverOid, false);
	elog(WARNING, "starting2");

	/* If DAS ID is not defined, first register it. */
	if (options->das_id == NULL) {
		elog(WARNING, "das_id is not defined");
		elog(WARNING, "starting2.1");
		// Convert List* to const char**
		// List* opt_keys = options->option_keys;
		// List* opt_values = options->option_values;
		// int num_options = list_length(opt_keys);
		// elog(WARNING, "num_options: %d", num_options);
		// const char** option_keys = (const char**) palloc(num_options * sizeof(const char*));
		// const char** option_values = (const char**) palloc(num_options * sizeof(const char*));
		// int i = 0;
		// foreach(lc, opt_keys)
		// {
		// 	option_keys[i] = lfirst(lc);
		// 	option_values[i] = list_nth(opt_values, i);
		// 	elog(WARNING, "key: %s", option_keys[i]);
		// 	elog(WARNING, "value: %s", option_values[i]);
		// 	i++;
		// }
		
		elog(WARNING, "starting2.2");
		char* das_id = register_das(options);
		elog(WARNING, "starting2.3");
		options->das_id = das_id;
	}

    /* Establish connection to DAS (Data Access Service). */
    // das = das_get_connection(server, user, options);
	elog(WARNING, "starting3");

	// options = mysql_get_options(serverOid, false);
	// conn = mysql_get_connection(server, user, options);

	/* Create workspace for strings */
	initStringInfo(&buf);
	elog(WARNING, "starting4");
	int ntables;
	char **tables;
	int i;

	elog(WARNING, "das_url: %s", options->das_url);
	tables = get_table_definitions(options, quote_identifier(server->servername), &ntables);
	elog(WARNING, "ntables: %d", ntables);
	for (int i = 0; i < ntables; i++)
	{
		char *table = tables[i];
		elog(WARNING, "table: %s", table);

		commands = lappend(commands, pstrdup(table));
	}

	return commands;
}

/*
 * mysqlBeginForeignInsert
 * 		Prepare for an insert operation triggered by partition routing
 * 		or COPY FROM.
 *
 * This is not yet supported, so raise an error.
 */
static void
mysqlBeginForeignInsert(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo)
{
	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			 errmsg("COPY and foreign partition routing not supported in mysql_fdw")));
}

/*
 * mysqlEndForeignInsert
 * 		BeginForeignInsert() is not yet implemented, hence we do not
 * 		have anything to cleanup as of now. We throw an error here just
 * 		to make sure when we do that we do not forget to cleanup
 * 		resources.
 */
static void
mysqlEndForeignInsert(EState *estate, ResultRelInfo *resultRelInfo)
{
	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			 errmsg("COPY and foreign partition routing not supported in mysql_fdw")));
}

/*
 * Prepare for processing of parameters used in remote query.
 */
static void
prepare_query_params(PlanState *node,
					 List *fdw_exprs,
					 int numParams,
					 FmgrInfo **param_flinfo,
					 List **param_exprs,
					 const char ***param_values,
					 Oid **param_types)
{
	int			i;
	ListCell   *lc;

	Assert(numParams > 0);

	/* Prepare for output conversion of parameters used in remote query. */
	*param_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * numParams);

	*param_types = (Oid *) palloc0(sizeof(Oid) * numParams);

	i = 0;
	foreach(lc, fdw_exprs)
	{
		Node	   *param_expr = (Node *) lfirst(lc);
		Oid			typefnoid;
		bool		isvarlena;

		(*param_types)[i] = exprType(param_expr);

		getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &(*param_flinfo)[i]);
		i++;
	}

	/*
	 * Prepare remote-parameter expressions for evaluation.  (Note: in
	 * practice, we expect that all these expressions will be just Params, so
	 * we could possibly do something more efficient than using the full
	 * expression-eval machinery for this.  But probably there would be little
	 * benefit, and it'd require postgres_fdw to know more than is desirable
	 * about Param evaluation.)
	 */
	*param_exprs = ExecInitExprList(fdw_exprs, node);

	/* Allocate buffer for text form of query parameters. */
	*param_values = (const char **) palloc0(numParams * sizeof(char *));
}

Datum
mysql_fdw_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(CODE_VERSION);
}

/*
 * getUpdateTargetAttrs
 * 		Returns the list of attribute numbers of the columns being updated.
 */
static List *
getUpdateTargetAttrs(PlannerInfo *root, RangeTblEntry *rte, int unique_attnum)
{
	List	   *targetAttrs = NIL;
	Bitmapset  *tmpset;
	AttrNumber	col;
#if PG_VERSION_NUM >= 160000
	RTEPermissionInfo *perminfo;
	int			attidx = -1;

	perminfo = getRTEPermissionInfo(root->parse->rteperminfos, rte);
	tmpset = bms_copy(perminfo->updatedCols);
#else
	tmpset = bms_copy(rte->updatedCols);
#endif

#if PG_VERSION_NUM >= 160000
	while ((attidx = bms_next_member(tmpset, attidx)) >= 0)
#else
	while ((col = bms_first_member(tmpset)) >= 0)
#endif
	{
#if PG_VERSION_NUM >= 160000
		col = attidx + FirstLowInvalidHeapAttributeNumber;
#else
		col += FirstLowInvalidHeapAttributeNumber;
#endif
		if (col <= InvalidAttrNumber)	/* shouldn't happen */
			elog(ERROR, "system-column update is not supported");

		/* We also disallow updates to the first column */
		if (col == unique_attnum)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("row identifier column update is not supported")));

		targetAttrs = lappend_int(targetAttrs, col);
	}

	return targetAttrs;
}

/*
 * mysqlGetForeignJoinPaths
 *		Add possible ForeignPath to joinrel, if join is safe to push down.
 */
static void
mysqlGetForeignJoinPaths(PlannerInfo *root, RelOptInfo *joinrel,
						 RelOptInfo *outerrel, RelOptInfo *innerrel,
						 JoinType jointype, JoinPathExtraData *extra)
{
	MySQLFdwRelationInfo *fpinfo;
	ForeignPath *joinpath;
	Cost		startup_cost;
	Cost		total_cost;
	Path	   *epq_path = NULL;	/* Path to create plan to be executed when
									 * EvalPlanQual gets triggered. */

	elog(WARNING, "join here1");

	/*
	 * Skip if this join combination has been considered already.
	 */
	if (joinrel->fdw_private)
		return;

	/*
	 * Create unfinished MySQLFdwRelationInfo entry which is used to indicate
	 * that the join relation is already considered, so that we won't waste
	 * time in judging safety of join pushdown and adding the same paths again
	 * if found safe.  Once we know that this join can be pushed down, we fill
	 * the entry.
	 */
	fpinfo = (MySQLFdwRelationInfo *) palloc0(sizeof(MySQLFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	joinrel->fdw_private = fpinfo;
	/* attrs_used is only for base relations. */
	fpinfo->attrs_used = NULL;
	elog(WARNING, "join here1.1");
	// what tthis is a CLEAR LOOP :-) It's borken
	fpinfo->options = ((MySQLFdwRelationInfo *) outerrel->fdw_private)->options;
	elog(WARNING, "join here1.2");
	elog(WARNING, "das_url: %s", fpinfo->options->das_url);

//yeah this needs to re-read.

	fpinfo->das = ((MySQLFdwRelationInfo *) outerrel->fdw_private)->das;
	elog(WARNING, "join here1.3");

	elog(WARNING, "join supported: %d", fpinfo->das->join_supported);
	elog(WARNING, "das_url: %s", fpinfo->das->das_url);

	if (!fpinfo->das->join_supported)
		return;

	elog(WARNING, "join here1.4");

	elog(WARNING, "join here2");

	/*
	 * In case there is a possibility that EvalPlanQual will be executed, we
	 * should be able to reconstruct the row, from base relations applying all
	 * the conditions.  We create a local plan from a suitable local path
	 * available in the path list.  In case such a path doesn't exist, we can
	 * not push the join to the foreign server since we won't be able to
	 * reconstruct the row for EvalPlanQual().  Find an alternative local path
	 * before we add ForeignPath, lest the new path would kick possibly the
	 * only local path.  Do this before calling mysql_foreign_join_ok(), since
	 * that function updates fpinfo and marks it as pushable if the join is
	 * found to be pushable.
	 */
	if (root->parse->commandType == CMD_DELETE ||
		root->parse->commandType == CMD_UPDATE ||
		root->rowMarks)
	{
		epq_path = GetExistingLocalJoinPath(joinrel);
		if (!epq_path)
		{
			elog(DEBUG3, "could not push down foreign join because a local path suitable for EPQ checks was not found");
			return;
		}
	}
	else
		epq_path = NULL;

	elog(WARNING, "join here3");

	if (!mysql_foreign_join_ok(root, joinrel, jointype, outerrel, innerrel,
							   extra))
	{
		/* Free path required for EPQ if we copied one; we don't need it now */
		if (epq_path)
			pfree(epq_path);
		return;
	}

	elog(WARNING, "join here4");

	/* TODO: Put accurate estimates here */
	startup_cost = 15.0;
	total_cost = 20 + startup_cost;

	/*
	 * Create a new join path and add it to the joinrel which represents a
	 * join between foreign tables.
	 */
#if PG_VERSION_NUM >= 170000
	joinpath = create_foreign_join_path(root,
										joinrel,
										NULL,	/* default pathtarget */
										joinrel->rows,
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										joinrel->lateral_relids,
										epq_path,
										extra->restrictlist,
										NIL);	/* no fdw_private */
#else
	joinpath = create_foreign_join_path(root,
										joinrel,
										NULL,	/* default pathtarget */
										joinrel->rows,
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										joinrel->lateral_relids,
										epq_path,
										NIL);	/* no fdw_private */
#endif

	elog(WARNING, "join here5");

	/* Add generated path into joinrel by add_path(). */
	add_path(joinrel, (Path *) joinpath);

	if (fpinfo->das->orderby_supported) {
		/* Add paths with pathkeys */
#if PG_VERSION_NUM >= 170000
		mysql_add_paths_with_pathkeys(root, joinrel, epq_path, startup_cost,
									  total_cost, extra->restrictlist);
#else
		mysql_add_paths_with_pathkeys(root, joinrel, epq_path, startup_cost,
									  total_cost);
#endif
	}
	/* XXX Consider parameterized paths for the join relation */
}

/*
 * mysql_foreign_join_ok
 * 		Assess whether the join between inner and outer relations can be
 * 		pushed down to the foreign server.
 *
 * As a side effect, save information we obtain in this function to
 * MySQLFdwRelationInfo passed in.
 */
static bool
mysql_foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel,
					  JoinType jointype, RelOptInfo *outerrel,
					  RelOptInfo *innerrel, JoinPathExtraData *extra)
{
	MySQLFdwRelationInfo *fpinfo;
	MySQLFdwRelationInfo *fpinfo_o;
	MySQLFdwRelationInfo *fpinfo_i;
	ListCell   *lc;
	List	   *joinclauses;

	/*
	 * We support pushing down INNER, LEFT and RIGHT joins. Constructing
	 * queries representing SEMI and ANTI joins is hard, hence not considered
	 * right now.
	 */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT &&
		jointype != JOIN_RIGHT)
		return false;

	/*
	 * If either of the joining relations is marked as unsafe to pushdown, the
	 * join cannot be pushed down.
	 */
	fpinfo = (MySQLFdwRelationInfo *) joinrel->fdw_private;
	fpinfo_o = (MySQLFdwRelationInfo *) outerrel->fdw_private;
	fpinfo_i = (MySQLFdwRelationInfo *) innerrel->fdw_private;
	if (!fpinfo_o || !fpinfo_o->pushdown_safe ||
		!fpinfo_i || !fpinfo_i->pushdown_safe)
		return false;

	/*
	 * If joining relations have local conditions, those conditions are
	 * required to be applied before joining the relations.  Hence the join
	 * can not be pushed down.
	 */
	if (fpinfo_o->local_conds || fpinfo_i->local_conds)
		return false;

	/*
	 * Separate restrict list into join quals and pushed-down (other) quals.
	 *
	 * Join quals belonging to an outer join must all be shippable, else we
	 * cannot execute the join remotely.  Add such quals to 'joinclauses'.
	 *
	 * Add other quals to fpinfo->remote_conds if they are shippable, else to
	 * fpinfo->local_conds.  In an inner join it's okay to execute conditions
	 * either locally or remotely; the same is true for pushed-down conditions
	 * at an outer join.
	 *
	 * Note we might return failure after having already scribbled on
	 * fpinfo->remote_conds and fpinfo->local_conds.  That's okay because we
	 * won't consult those lists again if we deem the join unshippable.
	 */
	joinclauses = NIL;
	foreach(lc, extra->restrictlist)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		bool		is_remote_clause = mysql_is_foreign_expr(root, joinrel,
															 rinfo->clause,
															 true);

		if (IS_OUTER_JOIN(jointype) &&
			!RINFO_IS_PUSHED_DOWN(rinfo, joinrel->relids))
		{
			if (!is_remote_clause)
				return false;
			joinclauses = lappend(joinclauses, rinfo);
		}
		else
		{
			if (is_remote_clause)
			{
				/*
				 * Unlike postgres_fdw, don't append the join clauses to
				 * remote_conds, instead keep the join clauses separate.
				 * Currently, we are providing limited operator pushability
				 * support for join pushdown, hence we keep those clauses
				 * separate to avoid INNER JOIN not getting pushdown if any of
				 * the WHERE clause is not shippable as per join pushdown
				 * shippability.
				 */
				if (jointype == JOIN_INNER)
					joinclauses = lappend(joinclauses, rinfo);
				else
					fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			}
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * mysqlDeparseExplicitTargetList() isn't smart enough to handle anything
	 * other than a Var.  In particular, if there's some PlaceHolderVar that
	 * would need to be evaluated within this join tree (because there's an
	 * upper reference to a quantity that may go to NULL as a result of an
	 * outer join), then we can't try to push the join down because we'll fail
	 * when we get to mysqlDeparseExplicitTargetList().  However, a
	 * PlaceHolderVar that needs to be evaluated *at the top* of this join
	 * tree is OK, because we can do that locally after fetching the results
	 * from the remote side.
	 */
	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = lfirst(lc);
		Relids		relids;

		/* PlaceHolderInfo refers to parent relids, not child relids. */
		relids = IS_OTHER_REL(joinrel) ?
			joinrel->top_parent_relids : joinrel->relids;

		if (bms_is_subset(phinfo->ph_eval_at, relids) &&
			bms_nonempty_difference(relids, phinfo->ph_eval_at))
			return false;
	}

	/* Save the join clauses, for later use. */
	fpinfo->joinclauses = joinclauses;

	/*
	 * Pull the other remote conditions from the joining relations into join
	 * clauses or other remote clauses (remote_conds) of this relation.  This
	 * avoids building subqueries at every join step.
	 *
	 * For an inner join, clauses from both the relations are added to the
	 * other remote clauses.  For an OUTER join, the clauses from the outer
	 * side are added to remote_conds since those can be evaluated after the
	 * join is evaluated.  The clauses from inner side are added to the
	 * joinclauses, since they need to evaluated while constructing the join.
	 *
	 * The joining sides cannot have local conditions, thus no need to test
	 * shippability of the clauses being pulled up.
	 */
	switch (jointype)
	{
		case JOIN_INNER:
			fpinfo->remote_conds = mysql_list_concat(fpinfo->remote_conds,
													 fpinfo_i->remote_conds);
			fpinfo->remote_conds = mysql_list_concat(fpinfo->remote_conds,
													 fpinfo_o->remote_conds);
			break;

		case JOIN_LEFT:
			/* Check that clauses from the inner side are pushable or not. */
			foreach(lc, fpinfo_i->remote_conds)
			{
				RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

				if (!mysql_is_foreign_expr(root, joinrel, ri->clause, true))
					return false;
			}

			fpinfo->joinclauses = mysql_list_concat(fpinfo->joinclauses,
													fpinfo_i->remote_conds);
			fpinfo->remote_conds = mysql_list_concat(fpinfo->remote_conds,
													 fpinfo_o->remote_conds);
			break;

		case JOIN_RIGHT:
			/* Check that clauses from the outer side are pushable or not. */
			foreach(lc, fpinfo_o->remote_conds)
			{
				RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

				if (!mysql_is_foreign_expr(root, joinrel, ri->clause, true))
					return false;
			}

			fpinfo->joinclauses = mysql_list_concat(fpinfo->joinclauses,
													fpinfo_o->remote_conds);
			fpinfo->remote_conds = mysql_list_concat(fpinfo->remote_conds,
													 fpinfo_i->remote_conds);
			break;

		default:
			/* Should not happen, we have just check this above */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	fpinfo->outerrel = outerrel;
	fpinfo->innerrel = innerrel;
	fpinfo->jointype = jointype;

	/* Mark that this join can be pushed down safely */
	fpinfo->pushdown_safe = true;

	/*
	 * Set the string describing this join relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();
	appendStringInfo(fpinfo->relation_name, "(%s) %s JOIN (%s)",
					 fpinfo_o->relation_name->data,
					 mysql_get_jointype_name(fpinfo->jointype),
					 fpinfo_i->relation_name->data);

	return true;
}

/*
 * mysqlRecheckForeignScan
 *		Execute a local join execution plan for a foreign join.
 */
static bool
mysqlRecheckForeignScan(ForeignScanState *node, TupleTableSlot *slot)
{
	Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;
	PlanState  *outerPlan = outerPlanState(node);
	TupleTableSlot *result;

	/* For base foreign relations, it suffices to set fdw_recheck_quals */
	if (scanrelid > 0)
		return true;

	Assert(outerPlan != NULL);

	/* Execute a local join execution plan */
	result = ExecProcNode(outerPlan);
	if (TupIsNull(result))
		return false;

	/* Store result in the given slot */
	ExecCopySlot(slot, result);

	return true;
}

/*
 * mysql_adjust_whole_row_ref
 * 		If the given list of Var nodes has whole-row reference, add Var
 * 		nodes corresponding to all the attributes of the corresponding
 * 		base relation.
 *
 * The function also returns an array of lists of var nodes.  The array is
 * indexed by the RTI and entry there contains the list of Var nodes which
 * make up the whole-row reference for corresponding base relation.
 * The relations not covered by given join and the relations which do not
 * have whole-row references will have NIL entries.
 *
 * If there are no whole-row references in the given list, the given list is
 * returned unmodified and the other list is NIL.
 */
static List *
mysql_adjust_whole_row_ref(PlannerInfo *root, List *scan_var_list,
						   List **whole_row_lists, Bitmapset *relids)
{
	ListCell   *lc;
	bool		has_whole_row = false;
	List	  **wr_list_array = NULL;
	int			cnt_rt;
	List	   *wr_scan_var_list = NIL;
#if PG_VERSION_NUM >= 160000
	ListCell   *cell;
#endif

	*whole_row_lists = NIL;

	/* Check if there exists at least one whole row reference. */
	foreach(lc, scan_var_list)
	{
		Var		   *var = (Var *) lfirst(lc);

		Assert(IsA(var, Var));

		if (var->varattno == 0)
		{
			has_whole_row = true;
			break;
		}
	}

	if (!has_whole_row)
		return scan_var_list;

	/*
	 * Allocate large enough memory to hold whole-row Var lists for all the
	 * relations.  This array will then be converted into a list of lists.
	 * Since all the base relations are marked by range table index, it's easy
	 * to keep track of the ones whose whole-row references have been taken
	 * care of.
	 */
	wr_list_array = (List **) palloc0(sizeof(List *) *
									  list_length(root->parse->rtable));

	/* Adjust the whole-row references as described in the prologue. */
	foreach(lc, scan_var_list)
	{
		Var		   *var = (Var *) lfirst(lc);

		Assert(IsA(var, Var));

		if (var->varattno == 0 && !wr_list_array[var->varno - 1])
		{
			List	   *wr_var_list;
			List	   *retrieved_attrs;
			RangeTblEntry *rte = rt_fetch(var->varno, root->parse->rtable);
			Bitmapset  *attrs_used;

			Assert(OidIsValid(rte->relid));

			/*
			 * Get list of Var nodes for all undropped attributes of the base
			 * relation.
			 */
			attrs_used = bms_make_singleton(0 -
											FirstLowInvalidHeapAttributeNumber);

			/*
			 * If the whole-row reference falls on the nullable side of the
			 * outer join and that side is null in a given result row, the
			 * whole row reference should be set to NULL.  In this case, all
			 * the columns of that relation will be NULL, but that does not
			 * help since those columns can be genuinely NULL in a row.
			 */
			wr_var_list =
				mysql_build_scan_list_for_baserel(rte->relid, var->varno,
												  attrs_used,
												  &retrieved_attrs);
			wr_list_array[var->varno - 1] = wr_var_list;
#if PG_VERSION_NUM >= 160000
			foreach(cell, wr_var_list)
			{
				Var		   *tlvar = (Var *) lfirst(cell);

				wr_scan_var_list = mysql_varlist_append_unique_var(wr_scan_var_list,
																   tlvar);
			}
#else
			wr_scan_var_list = list_concat_unique(wr_scan_var_list,
												  wr_var_list);
#endif

			bms_free(attrs_used);
			list_free(retrieved_attrs);
		}
		else
#if PG_VERSION_NUM >= 160000
			wr_scan_var_list = mysql_varlist_append_unique_var(wr_scan_var_list,
															   var);
#else
			wr_scan_var_list = list_append_unique(wr_scan_var_list, var);
#endif
	}

	/*
	 * Collect the required Var node lists into a list of lists ordered by the
	 * base relations' range table indexes.
	 */
	cnt_rt = -1;
	while ((cnt_rt = bms_next_member(relids, cnt_rt)) >= 0)
		*whole_row_lists = lappend(*whole_row_lists, wr_list_array[cnt_rt - 1]);

	pfree(wr_list_array);
	return wr_scan_var_list;
}

/*
 * mysql_build_scan_list_for_baserel
 * 		Build list of nodes corresponding to the attributes requested for
 * 		given base relation.
 *
 * The list contains Var nodes corresponding to the attributes specified in
 * attrs_used.  If whole-row reference is required, the functions adds Var
 * nodes corresponding to all the attributes in the relation.
 */
static List *
mysql_build_scan_list_for_baserel(Oid relid, Index varno,
								  Bitmapset *attrs_used,
								  List **retrieved_attrs)
{
	int			attno;
	List	   *tlist = NIL;
	Node	   *node;
	bool		wholerow_requested = false;
	Relation	relation;
	TupleDesc	tupdesc;

	Assert(OidIsValid(relid));

	*retrieved_attrs = NIL;

	/* Planner must have taken a lock, so request no lock here */
#if PG_VERSION_NUM < 130000
	relation = heap_open(relid, NoLock);
#else
	relation = table_open(relid, NoLock);
#endif

	tupdesc = RelationGetDescr(relation);

	/* Is whole-row reference requested? */
	wholerow_requested = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
									   attrs_used);

	/* Handle user defined attributes. */
	for (attno = 1; attno <= tupdesc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		/*
		 * For a required attribute create a Var node and add corresponding
		 * attribute number to the retrieved_attrs list.
		 */
		if (wholerow_requested ||
			bms_is_member(attno - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			node = (Node *) makeVar(varno, attno, attr->atttypid,
									attr->atttypmod, attr->attcollation, 0);
			tlist = lappend(tlist, node);

			*retrieved_attrs = lappend_int(*retrieved_attrs, attno);
		}
	}

#if PG_VERSION_NUM < 130000
	heap_close(relation, NoLock);
#else
	table_close(relation, NoLock);
#endif

	return tlist;
}

/*
 * mysql_build_whole_row_constr_info
 *		Calculate and save the information required to construct whole row
 *		references of base foreign relations involved in the pushed down join.
 *
 * tupdesc is the tuple descriptor describing the result returned by the
 * ForeignScan node.  It is expected to be same as
 * ForeignScanState::ss::ss_ScanTupleSlot, which is constructed using
 * fdw_scan_tlist.
 *
 * relids is the the set of relations participating in the pushed down join.
 *
 * max_relid is the maximum number of relation index expected.
 *
 * whole_row_lists is the list of Var node lists constituting the whole-row
 * reference for base relations in the relids in the same order.
 *
 * scan_tlist is the targetlist representing the result fetched from the
 * foreign server.
 *
 * fdw_scan_tlist is the targetlist representing the result returned by the
 * ForeignScan node.
 */
static void
mysql_build_whole_row_constr_info(MySQLFdwExecState *festate,
								  TupleDesc tupdesc, Bitmapset *relids,
								  int max_relid, List *whole_row_lists,
								  List *scan_tlist, List *fdw_scan_tlist)
{
	int			cnt_rt;
	int			cnt_vl;
	int			cnt_attr;
	ListCell   *lc;
	int		   *fs_attr_pos = NULL;
	MySQLWRState **mysqlwrstates = NULL;
	int			fs_num_atts;

	/*
	 * Allocate memory to hold whole-row reference state for each relation.
	 * Indexing by the range table index is faster than maintaining an
	 * associative map.
	 */
	mysqlwrstates = (MySQLWRState **) palloc0(sizeof(MySQLWRState *) * max_relid);

	/*
	 * Set the whole-row reference state for the relations whose whole-row
	 * reference needs to be constructed.
	 */
	cnt_rt = -1;
	cnt_vl = 0;
	while ((cnt_rt = bms_next_member(relids, cnt_rt)) >= 0)
	{
		MySQLWRState *wr_state = (MySQLWRState *) palloc0(sizeof(MySQLWRState));
		List	   *var_list = list_nth(whole_row_lists, cnt_vl++);
		int			natts;

		/* Skip the relations without whole-row references. */
		if (list_length(var_list) <= 0)
			continue;

		natts = list_length(var_list);
		wr_state->attr_pos = (int *) palloc(sizeof(int) * natts);

		/*
		 * Create a map of attributes required for whole-row reference to
		 * their positions in the result fetched from the foreign server.
		 */
		cnt_attr = 0;
		foreach(lc, var_list)
		{
			Var		   *var = lfirst(lc);
			TargetEntry *tle_sl;

			Assert(IsA(var, Var) && var->varno == cnt_rt);

#if PG_VERSION_NUM >= 160000
			tle_sl = mysql_tlist_member_match_var(var, scan_tlist);
#else
			tle_sl = tlist_member((Expr *) var, scan_tlist);
#endif

			Assert(tle_sl);

			wr_state->attr_pos[cnt_attr++] = tle_sl->resno - 1;
		}
		Assert(natts == cnt_attr);

		/* Build rest of the state */
		wr_state->tupdesc = ExecTypeFromExprList(var_list);
		Assert(natts == wr_state->tupdesc->natts);
		wr_state->values = (Datum *) palloc(sizeof(Datum) * natts);
		wr_state->nulls = (bool *) palloc(sizeof(bool) * natts);
		BlessTupleDesc(wr_state->tupdesc);
		mysqlwrstates[cnt_rt - 1] = wr_state;
	}

	/*
	 * Construct the array mapping columns in the ForeignScan node output to
	 * their positions in the result fetched from the foreign server. Positive
	 * values indicate the locations in the result and negative values
	 * indicate the range table indexes of the base table whose whole-row
	 * reference values are requested in that place.
	 */
	fs_num_atts = list_length(fdw_scan_tlist);
	fs_attr_pos = (int *) palloc(sizeof(int) * fs_num_atts);
	cnt_attr = 0;
	foreach(lc, fdw_scan_tlist)
	{
		TargetEntry *tle_fsl = lfirst(lc);
		Var		   *var = (Var *) tle_fsl->expr;

		Assert(IsA(var, Var));
		if (var->varattno == 0)
			fs_attr_pos[cnt_attr] = -var->varno;
		else
		{
#if PG_VERSION_NUM >= 160000
			TargetEntry *tle_sl = mysql_tlist_member_match_var(var, scan_tlist);
#else
			TargetEntry *tle_sl = tlist_member((Expr *) var, scan_tlist);
#endif

			Assert(tle_sl);
			fs_attr_pos[cnt_attr] = tle_sl->resno - 1;
		}
		cnt_attr++;
	}

	/*
	 * The tuple descriptor passed in should have same number of attributes as
	 * the entries in fdw_scan_tlist.
	 */
	Assert(fs_num_atts == tupdesc->natts);

	festate->mysqlwrstates = mysqlwrstates;
	festate->wr_attrs_pos = fs_attr_pos;
	festate->wr_tupdesc = tupdesc;
	festate->wr_values = (Datum *) palloc(sizeof(Datum) * tupdesc->natts);
	festate->wr_nulls = (bool *) palloc(sizeof(bool) * tupdesc->natts);

	return;
}

/*
 * mysql_get_tuple_with_whole_row
 *		Construct the result row with whole-row references.
 */
static HeapTuple
mysql_get_tuple_with_whole_row(MySQLFdwExecState *festate, Datum *values,
							   bool *nulls)
{
	TupleDesc	tupdesc = festate->wr_tupdesc;
	Datum	   *wr_values = festate->wr_values;
	bool	   *wr_nulls = festate->wr_nulls;
	int			cnt_attr;
	HeapTuple	tuple = NULL;

	for (cnt_attr = 0; cnt_attr < tupdesc->natts; cnt_attr++)
	{
		int			attr_pos = festate->wr_attrs_pos[cnt_attr];

		if (attr_pos >= 0)
		{
			wr_values[cnt_attr] = values[attr_pos];
			wr_nulls[cnt_attr] = nulls[attr_pos];
		}
		else
		{
			/*
			 * The RTI of relation whose whole row reference is to be
			 * constructed is stored as -ve attr_pos.
			 */
			MySQLWRState *wr_state = festate->mysqlwrstates[-attr_pos - 1];

			wr_nulls[cnt_attr] = nulls[wr_state->wr_null_ind_pos];
			if (!wr_nulls[cnt_attr])
			{
				HeapTuple	wr_tuple = mysql_form_whole_row(wr_state,
															values,
															nulls);

				wr_values[cnt_attr] = HeapTupleGetDatum(wr_tuple);
			}
		}
	}

	tuple = heap_form_tuple(tupdesc, wr_values, wr_nulls);
	return tuple;
}

/*
 * mysql_form_whole_row
 * 		The function constructs whole-row reference for a base relation
 * 		with the information given in wr_state.
 *
 * wr_state contains the information about which attributes from values and
 * nulls are to be used and in which order to construct the whole-row
 * reference.
 */
static HeapTuple
mysql_form_whole_row(MySQLWRState *wr_state, Datum *values, bool *nulls)
{
	int			cnt_attr;

	for (cnt_attr = 0; cnt_attr < wr_state->tupdesc->natts; cnt_attr++)
	{
		int			attr_pos = wr_state->attr_pos[cnt_attr];

		wr_state->values[cnt_attr] = values[attr_pos];
		wr_state->nulls[cnt_attr] = nulls[attr_pos];
	}
	return heap_form_tuple(wr_state->tupdesc, wr_state->values,
						   wr_state->nulls);
}

/*
 * mysql_foreign_grouping_ok
 * 		Assess whether the aggregation, grouping and having operations can
 * 		be pushed down to the foreign server.  As a side effect, save
 * 		information we obtain in this function to MySQLFdwRelationInfo of
 * 		the input relation.
 */
static bool
mysql_foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
						  Node *havingQual)
{
	Query	   *query = root->parse;
	PathTarget *grouping_target = grouped_rel->reltarget;
	MySQLFdwRelationInfo *fpinfo = (MySQLFdwRelationInfo *) grouped_rel->fdw_private;
	MySQLFdwRelationInfo *ofpinfo;
	ListCell   *lc;
	int			i;
	List	   *tlist = NIL;

	/* Grouping Sets are not pushable */
	if (query->groupingSets)
		return false;

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (MySQLFdwRelationInfo *) fpinfo->outerrel->fdw_private;

	/*
	 * If underneath input relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * Evaluate grouping targets and check whether they are safe to push down
	 * to the foreign side.  All GROUP BY expressions will be part of the
	 * grouping target and thus there is no need to evaluate it separately.
	 * While doing so, add required expressions into target list which can
	 * then be used to pass to foreign server.
	 */
	i = 0;
	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell   *l;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
		{
			TargetEntry *tle;

			/*
			 * If any of the GROUP BY expression is not shippable we can not
			 * push down aggregation to the foreign server.
			 */
			if (!mysql_is_foreign_expr(root, grouped_rel, expr, true))
				return false;

			/*
			 * If it would be a foreign param, we can't put it into the tlist,
			 * so we have to fail.
			 */
			if (mysql_is_foreign_param(root, grouped_rel, expr))
				return false;

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		}
		else
		{
			/* Check entire expression whether it is pushable or not */
			if (mysql_is_foreign_expr(root, grouped_rel, expr, true) &&
				!mysql_is_foreign_param(root, grouped_rel, expr))
			{
				/* Pushable, add to tlist */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{
				List	   *aggvars;

				/* Not matched exactly, pull the var with aggregates then */
				aggvars = pull_var_clause((Node *) expr,
										  PVC_INCLUDE_AGGREGATES);

				/*
				 * If any aggregate expression is not shippable, then we
				 * cannot push down aggregation to the foreign server.  (We
				 * don't have to check is_foreign_param, since that certainly
				 * won't return true for any such expression.)
				 */
				if (!mysql_is_foreign_expr(root, grouped_rel, (Expr *) aggvars, true))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain var
				 * nodes should be either same as some GROUP BY expression or
				 * part of some GROUP BY expression. In later case, the query
				 * cannot refer plain var nodes without the surrounding
				 * expression.  In both the cases, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * adding pulled plain var nodes in SELECT clause will cause
				 * an error on the foreign server if they are not same as some
				 * GROUP BY expression.
				 */
				foreach(l, aggvars)
				{
					Expr	   *aggref = (Expr *) lfirst(l);

					if (IsA(aggref, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(aggref));
				}
			}
		}

		i++;
	}

	/*
	 * Classify the pushable and non-pushable having clauses and save them in
	 * remote_conds and local_conds of the grouped rel's fpinfo.
	 */
	if (havingQual)
	{
		foreach(lc, (List *) havingQual)
		{
			Expr	   *expr = (Expr *) lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));
#if PG_VERSION_NUM >= 160000
			rinfo = make_restrictinfo(root,
									  expr,
									  true,
									  false,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
#elif PG_VERSION_NUM >= 140000
			rinfo = make_restrictinfo(root,
									  expr,
									  true,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
#else
			rinfo = make_restrictinfo(expr,
									  true,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
#endif

			if (!mysql_is_foreign_expr(root, grouped_rel, expr, true))
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
			else
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		List	   *aggvars = NIL;

		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_INCLUDE_AGGREGATES));
		}

		foreach(lc, aggvars)
		{
			Expr	   *expr = (Expr *) lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.  Again, we need not check
			 * is_foreign_param for a foreign aggregate.
			 */
			if (IsA(expr, Aggref))
			{
				if (!mysql_is_foreign_expr(root, grouped_rel, expr, true))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();
	appendStringInfo(fpinfo->relation_name, "Aggregate on (%s)",
					 ofpinfo->relation_name->data);

	return true;
}

/*
 * mysqlGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 *
 * Right now, we only support aggregate, grouping and having clause pushdown.
 */
static void
mysqlGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
						  RelOptInfo *input_rel, RelOptInfo *output_rel,
						  void *extra)
{
	MySQLFdwRelationInfo *fpinfo;

	elog(WARNING, "mysql_fdw: mysqlGetForeignUpperPaths 1");

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!input_rel->fdw_private ||
		!((MySQLFdwRelationInfo *) input_rel->fdw_private)->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls. */
	if ((stage != UPPERREL_GROUP_AGG && stage != UPPERREL_ORDERED &&
		 stage != UPPERREL_FINAL) ||
		output_rel->fdw_private)
		return;

	fpinfo = (MySQLFdwRelationInfo *) palloc0(sizeof(MySQLFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	fpinfo->stage = stage;
	fpinfo->options = ((MySQLFdwRelationInfo *) input_rel->fdw_private)->options;
	fpinfo->das = ((MySQLFdwRelationInfo *) input_rel->fdw_private)->das;
	output_rel->fdw_private = fpinfo;

	if (!fpinfo->das->aggregation_supported)
		return;

	elog(WARNING, "mysql_fdw: mysqlGetForeignUpperPaths 2");

	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
			mysql_add_foreign_grouping_paths(root, input_rel, output_rel,
											 (GroupPathExtraData *) extra);
			break;
		case UPPERREL_ORDERED:
			mysql_add_foreign_ordered_paths(root, input_rel, output_rel);
			break;
		case UPPERREL_FINAL:
			mysql_add_foreign_final_paths(root, input_rel, output_rel,
										  (FinalPathExtraData *) extra);
			break;
		default:
			elog(ERROR, "unexpected upper relation: %d", (int) stage);
			break;
	}

	elog(WARNING, "mysql_fdw: mysqlGetForeignUpperPaths 2");
}

/*
 * mysql_add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
mysql_add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
								 RelOptInfo *grouped_rel,
								 GroupPathExtraData *extra)
{
	Query	   *parse = root->parse;
	MySQLFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	Cost		startup_cost;
	Cost		total_cost;
	double		num_groups;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
		!root->hasHavingQual)
		return;

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/* Assess if it is safe to push down aggregation and grouping. */
	if (!mysql_foreign_grouping_ok(root, grouped_rel, extra->havingQual))
		return;

	/*
	 * TODO: Put accurate estimates here.
	 *
	 * Cost used here is minimum of the cost estimated for base and join
	 * relation.
	 */
	startup_cost = 15;
	total_cost = 10 + startup_cost;

	/* Estimate output tuples which should be same as number of groups */
#if PG_VERSION_NUM >= 140000
	num_groups = estimate_num_groups(root,
									 get_sortgrouplist_exprs(root->parse->groupClause,
															 fpinfo->grouped_tlist),
									 input_rel->rows, NULL, NULL);
#else
	num_groups = estimate_num_groups(root,
									 get_sortgrouplist_exprs(root->parse->groupClause,
															 fpinfo->grouped_tlist),
									 input_rel->rows, NULL);
#endif

	/* Create and add foreign path to the grouping relation. */
#if PG_VERSION_NUM >= 170000
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  num_groups,
										  startup_cost,
										  total_cost,
										  NIL,	/* no pathkeys */
										  NULL,
										  NIL,	/* no fdw_restrictinfo list */
										  NIL);	/* no fdw_private */
#else
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  num_groups,
										  startup_cost,
										  total_cost,
										  NIL,	/* no pathkeys */
										  NULL,
										  NIL);	/* no fdw_private */
#endif

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *) grouppath);
}

/*
 * mysql_get_useful_ecs_for_relation
 *		Determine which EquivalenceClasses might be involved in useful
 *		orderings of this relation.
 *
 * This function is in some respects a mirror image of the core function
 * pathkeys_useful_for_merging: for a regular table, we know what indexes
 * we have and want to test whether any of them are useful.  For a foreign
 * table, we don't know what indexes are present on the remote side but
 * want to speculate about which ones we'd like to use if they existed.
 *
 * This function returns a list of potentially-useful equivalence classes,
 * but it does not guarantee that an EquivalenceMember exists which contains
 * Vars only from the given relation.  For example, given ft1 JOIN t1 ON
 * ft1.x + t1.x = 0, this function will say that the equivalence class
 * containing ft1.x + t1.x is potentially useful.  Supposing ft1 is remote and
 * t1 is local (or on a different server), it will turn out that no useful
 * ORDER BY clause can be generated.  It's not our job to figure that out
 * here; we're only interested in identifying relevant ECs.
 */
static List *
mysql_get_useful_ecs_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_eclass_list = NIL;
	ListCell   *lc;
	Relids		relids;

	/*
	 * First, consider whether any active EC is potentially useful for a merge
	 * join against this relation.
	 */
	if (rel->has_eclass_joins)
	{
		foreach(lc, root->eq_classes)
		{
			EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc);

			if (eclass_useful_for_merging(root, cur_ec, rel))
				useful_eclass_list = lappend(useful_eclass_list, cur_ec);
		}
	}

	/*
	 * Next, consider whether there are any non-EC derivable join clauses that
	 * are merge-joinable.  If the joininfo list is empty, we can exit
	 * quickly.
	 */
	if (rel->joininfo == NIL)
		return useful_eclass_list;

	/* If this is a child rel, we must use the topmost parent rel to search. */
	if (IS_OTHER_REL(rel))
	{
		Assert(!bms_is_empty(rel->top_parent_relids));
		relids = rel->top_parent_relids;
	}
	else
		relids = rel->relids;

	/* Check each join clause in turn. */
	foreach(lc, rel->joininfo)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/* Consider only mergejoinable clauses */
		if (restrictinfo->mergeopfamilies == NIL)
			continue;

		/* Make sure we've got canonical ECs. */
		update_mergeclause_eclasses(root, restrictinfo);

		/*
		 * restrictinfo->mergeopfamilies != NIL is sufficient to guarantee
		 * that left_ec and right_ec will be initialized, per comments in
		 * distribute_qual_to_rels.
		 *
		 * We want to identify which side of this merge-joinable clause
		 * contains columns from the relation produced by this RelOptInfo. We
		 * test for overlap, not containment, because there could be extra
		 * relations on either side.  For example, suppose we've got something
		 * like ((A JOIN B ON A.x = B.x) JOIN C ON A.y = C.y) LEFT JOIN D ON
		 * A.y = D.y.  The input rel might be the joinrel between A and B, and
		 * we'll consider the join clause A.y = D.y. relids contains a
		 * relation not involved in the join class (B) and the equivalence
		 * class for the left-hand side of the clause contains a relation not
		 * involved in the input rel (C).  Despite the fact that we have only
		 * overlap and not containment in either direction, A.y is potentially
		 * useful as a sort column.
		 *
		 * Note that it's even possible that relids overlaps neither side of
		 * the join clause.  For example, consider A LEFT JOIN B ON A.x = B.x
		 * AND A.x = 1.  The clause A.x = 1 will appear in B's joininfo list,
		 * but overlaps neither side of B.  In that case, we just skip this
		 * join clause, since it doesn't suggest a useful sort order for this
		 * relation.
		 */
		if (bms_overlap(relids, restrictinfo->right_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->right_ec);
		else if (bms_overlap(relids, restrictinfo->left_ec->ec_relids))
			useful_eclass_list = list_append_unique_ptr(useful_eclass_list,
														restrictinfo->left_ec);
	}

	return useful_eclass_list;
}

/*
 * mysql_get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
mysql_get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_pathkeys_list = NIL;
	List	   *useful_eclass_list;
	MySQLFdwRelationInfo *fpinfo = (MySQLFdwRelationInfo *) rel->fdw_private;
	EquivalenceClass *query_ec = NULL;
	ListCell   *lc;

	/*
	 * Pushing the query_pathkeys to the remote server is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	fpinfo->qp_is_pushdown_safe = false;
	if (root->query_pathkeys)
	{
		bool		query_pathkeys_ok = true;

		foreach(lc, root->query_pathkeys)
		{
			PathKey    *pathkey = (PathKey *) lfirst(lc);

			/*
			 * The planner and executor don't have any clever strategy for
			 * taking data sorted by a prefix of the query's pathkeys and
			 * getting it to be sorted by all of those pathkeys. We'll just
			 * end up resorting the entire data set.  So, unless we can push
			 * down all of the query pathkeys, forget it.
			 */
			if (!mysql_is_foreign_pathkey(root, rel, pathkey))
			{
				query_pathkeys_ok = false;
				break;
			}
		}

		if (query_pathkeys_ok)
		{
			useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
			fpinfo->qp_is_pushdown_safe = true;
		}
	}

	/* Get the list of interesting EquivalenceClasses. */
	useful_eclass_list = mysql_get_useful_ecs_for_relation(root, rel);

	/* Extract unique EC for query, if any, so we don't consider it again. */
	if (list_length(root->query_pathkeys) == 1)
	{
		PathKey    *query_pathkey = linitial(root->query_pathkeys);

		query_ec = query_pathkey->pk_eclass;
	}

	/*
	 * As a heuristic, the only pathkeys we consider here are those of length
	 * one.  It's surely possible to consider more, but since each one we
	 * choose to consider will generate a round-trip to the remote side, we
	 * need to be a bit cautious here.  It would sure be nice to have a local
	 * cache of information about remote index definitions...
	 */
	foreach(lc, useful_eclass_list)
	{
		EquivalenceMember *em = NULL;
		EquivalenceClass *cur_ec = lfirst(lc);
		PathKey    *pathkey;

		/* If redundant with what we did above, skip it. */
		if (cur_ec == query_ec)
			continue;

		em = mysql_find_em_for_rel(root, cur_ec, rel);

		/* Can't push down the sort if the EC's opfamily is not shippable. */
		if (!mysql_is_builtin(linitial_oid(cur_ec->ec_opfamilies)))
			continue;

		/* Looks like we can generate a pathkey, so let's do it. */
		pathkey = make_canonical_pathkey(root, cur_ec,
										 linitial_oid(cur_ec->ec_opfamilies),
										 BTLessStrategyNumber,
										 false);

		/* Check for sort operator pushability. */
		if (mysql_get_sortby_direction_string(em, pathkey, fpinfo->das->pushability_hash) == NULL)
			continue;

		useful_pathkeys_list = lappend(useful_pathkeys_list,
									   list_make1(pathkey));
	}

	return useful_pathkeys_list;
}

/*
 * mysql_add_paths_with_pathkeys
 *		 Add path with root->query_pathkeys if that's pushable.
 *
 * Pushing down query_pathkeys to the foreign server might let us avoid a
 * local sort.
 */
#if PG_VERSION_NUM >= 170000
static void
mysql_add_paths_with_pathkeys(PlannerInfo *root, RelOptInfo *rel,
							  Path *epq_path, Cost base_startup_cost,
							  Cost base_total_cost, List *restrictlist)
#else
static void
mysql_add_paths_with_pathkeys(PlannerInfo *root, RelOptInfo *rel,
							  Path *epq_path, Cost base_startup_cost,
							  Cost base_total_cost)
#endif
{
	ListCell   *lc;
	List	   *useful_pathkeys_list = NIL; /* List of all pathkeys */

	useful_pathkeys_list = mysql_get_useful_pathkeys_for_relation(root, rel);

	/* Create one path for each set of pathkeys we found above. */
	foreach(lc, useful_pathkeys_list)
	{
		Cost		startup_cost;
		Cost		total_cost;
		List	   *useful_pathkeys = lfirst(lc);
		Path	   *sorted_epq_path;

		/* TODO put accurate estimates. */
		startup_cost = base_startup_cost * DEFAULT_MYSQL_SORT_MULTIPLIER;
		total_cost = base_total_cost * DEFAULT_MYSQL_SORT_MULTIPLIER;


		/*
		 * The EPQ path must be at least as well sorted as the path itself, in
		 * case it gets used as input to a mergejoin.
		 */
		sorted_epq_path = epq_path;
		if (sorted_epq_path != NULL &&
			!pathkeys_contained_in(useful_pathkeys,
								   sorted_epq_path->pathkeys))
			sorted_epq_path = (Path *)
				create_sort_path(root,
								 rel,
								 sorted_epq_path,
								 useful_pathkeys,
								 -1.0);

		if (IS_SIMPLE_REL(rel))
#if PG_VERSION_NUM >= 170000
			add_path(rel, (Path *)
					 create_foreignscan_path(root, rel,
											 NULL,
											 rel->rows,
											 startup_cost,
											 total_cost,
											 useful_pathkeys,
											 rel->lateral_relids,
											 sorted_epq_path,
											 NIL,	/* no fdw_restrictinfo list */
											 NIL));	/* no fdw_private list */
#else
		add_path(rel, (Path *)
					 create_foreignscan_path(root, rel,
											 NULL,
											 rel->rows,
											 startup_cost,
											 total_cost,
											 useful_pathkeys,
											 rel->lateral_relids,
											 sorted_epq_path,
											 NIL));	/* no fdw_private list */
#endif
		else
#if PG_VERSION_NUM >= 170000
			add_path(rel, (Path *)
					 create_foreign_join_path(root, rel,
											  NULL,
											  rel->rows,
											  startup_cost,
											  total_cost,
											  useful_pathkeys,
											  rel->lateral_relids,
											  sorted_epq_path,
											  restrictlist,
											  NIL));	/* no fdw_private */
#else
			add_path(rel, (Path *)
					 create_foreign_join_path(root, rel,
											  NULL,
											  rel->rows,
											  startup_cost,
											  total_cost,
											  useful_pathkeys,
											  rel->lateral_relids,
											  sorted_epq_path,
											  NIL));	/* no fdw_private */
#endif
	}
}

/*
 * Given an EquivalenceClass and a foreign relation, find an EC member
 * that can be used to sort the relation remotely according to a pathkey
 * using this EC.
 *
 * If there is more than one suitable candidate, return an arbitrary
 * one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given
 * rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember *
mysql_find_em_for_rel(PlannerInfo *root, EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell   *lc;

	foreach(lc, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);

		/*
		 * Note we require !bms_is_empty, else we'd accept constant
		 * expressions which are not suitable for the purpose.
		 */
		if (bms_is_subset(em->em_relids, rel->relids) &&
			!bms_is_empty(em->em_relids) &&
			mysql_is_foreign_expr(root, rel, em->em_expr, true))
			return em;
	}

	return NULL;
}

/*
 * mysql_add_foreign_ordered_paths
 *		Add foreign paths for performing the final sort remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given ordered_rel.
 */
static void
mysql_add_foreign_ordered_paths(PlannerInfo *root, RelOptInfo *input_rel,
								RelOptInfo *ordered_rel)
{
	Query	   *parse = root->parse;
	MySQLFdwRelationInfo *ifpinfo = input_rel->fdw_private;
	MySQLFdwRelationInfo *fpinfo = ordered_rel->fdw_private;
	double		rows;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *fdw_private;
	ForeignPath *ordered_path;
	ListCell   *lc;

	/* Shouldn't get here unless the query has ORDER BY */
	Assert(parse->sortClause);

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * If the input_rel is a base or join relation, we would already have
	 * considered pushing down the final sort to the remote server when
	 * creating pre-sorted foreign paths for that relation, because the
	 * query_pathkeys is set to the root->sort_pathkeys in that case (see
	 * standard_qp_callback()).
	 */
	if (input_rel->reloptkind == RELOPT_BASEREL ||
		input_rel->reloptkind == RELOPT_JOINREL)
	{
		Assert(root->query_pathkeys == root->sort_pathkeys);

		/* Safe to push down if the query_pathkeys is safe to push down */
		fpinfo->pushdown_safe = ifpinfo->qp_is_pushdown_safe;

		return;
	}

	/* The input_rel should be a grouping relation */
	Assert(input_rel->reloptkind == RELOPT_UPPER_REL &&
		   ifpinfo->stage == UPPERREL_GROUP_AGG);

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying grouping relation to perform the final sort remotely,
	 * which is stored into the fdw_private list of the resulting path.
	 */

	/* Assess if it is safe to push down the final sort */
	foreach(lc, root->sort_pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lc);
		EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
		EquivalenceMember *em;

		/*
		 * mysql_is_foreign_expr would detect volatile expressions as well,
		 * but checking ec_has_volatile here saves some cycles.
		 */
		if (pathkey_ec->ec_has_volatile)
			return;

		/*
		 * The EC must contain a shippable EM that is computed in input_rel's
		 * reltarget, else we can't push down the sort.
		 */
		em = mysql_find_em_for_rel_target(root, pathkey_ec, input_rel);

		if (mysql_get_sortby_direction_string(em, pathkey, ifpinfo->das->pushability_hash) == NULL)
			return;
	}

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* TODO: Put accurate estimates */
	startup_cost = 15;
	total_cost = 10 + startup_cost;
	rows = 10;

	/*
	 * Build the fdw_private list that will be used by mysqlGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeInteger(true), makeInteger(false));

	/* Create foreign ordering path */
#if PG_VERSION_NUM >= 170000
	ordered_path = create_foreign_upper_path(root,
											 input_rel,
											 root->upper_targets[UPPERREL_ORDERED],
											 rows,
											 startup_cost,
											 total_cost,
											 root->sort_pathkeys,
											 NULL,	/* no extra plan */
											 NIL,	/* no fdw_restrictinfo list */
											 fdw_private);
#else
	ordered_path = create_foreign_upper_path(root,
											 input_rel,
											 root->upper_targets[UPPERREL_ORDERED],
											 rows,
											 startup_cost,
											 total_cost,
											 root->sort_pathkeys,
											 NULL,	/* no extra plan */
											 fdw_private);
#endif

	/* and add it to the ordered_rel */
	add_path(ordered_rel, (Path *) ordered_path);
}

/*
 * mysql_find_em_for_rel_target
 *
 * Find an EquivalenceClass member that is to be computed as a sort column
 * in the given rel's reltarget, and is shippable.
 *
 * If there is more than one suitable candidate, return an arbitrary
 * one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given
 * rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember *
mysql_find_em_for_rel_target(PlannerInfo *root, EquivalenceClass *ec,
							 RelOptInfo *rel)
{
	PathTarget *target = rel->reltarget;
	ListCell   *lc1;
	int			i;

	i = 0;
	foreach(lc1, target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc1);
		Index		sgref = get_pathtarget_sortgroupref(target, i);
		ListCell   *lc2;

		/* Ignore non-sort expressions */
		if (sgref == 0 ||
			get_sortgroupref_clause_noerr(sgref,
										  root->parse->sortClause) == NULL)
		{
			i++;
			continue;
		}

		/* We ignore binary-compatible relabeling on both ends */
		while (expr && IsA(expr, RelabelType))
			expr = ((RelabelType *) expr)->arg;

		/* Locate an EquivalenceClass member matching this expr, if any */
		foreach(lc2, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc2);
			Expr	   *em_expr;

			/* Don't match constants */
			if (em->em_is_const)
				continue;

			/* Ignore child members */
			if (em->em_is_child)
				continue;

			/* Match if same expression (after stripping relabel) */
			em_expr = em->em_expr;
			while (em_expr && IsA(em_expr, RelabelType))
				em_expr = ((RelabelType *) em_expr)->arg;

			if (!equal(em_expr, expr))
				continue;

			/* Check that expression (including relabels!) is shippable */
			if (mysql_is_foreign_expr(root, rel, em->em_expr, true))
				return em;
		}

		i++;
	}

	return NULL;
}

/*
 * mysql_add_foreign_final_paths
 *		Add foreign paths for performing the final processing remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given final_rel.
 */
static void
mysql_add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel,
							  RelOptInfo *final_rel, FinalPathExtraData *extra)
{
	Query	   *parse = root->parse;
	MySQLFdwRelationInfo *ifpinfo = (MySQLFdwRelationInfo *) input_rel->fdw_private;
	MySQLFdwRelationInfo *fpinfo = (MySQLFdwRelationInfo *) final_rel->fdw_private;
	bool		has_final_sort = false;
	List	   *pathkeys = NIL;
	double		rows;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *fdw_private;
	ForeignPath *final_path;

	/*
	 * Currently, we only support this for SELECT commands
	 */
	if (parse->commandType != CMD_SELECT)
		return;

	/*
	 * No work if there is no FOR UPDATE/SHARE clause and if there is no need
	 * to add a LIMIT node
	 */
	if (!parse->rowMarks && !extra->limit_needed)
		return;

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* MySQL does not support only OFFSET clause in a SELECT command. */
	if (parse->limitOffset && !parse->limitCount)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * If there is no need to add a LIMIT node, there might be a ForeignPath
	 * in the input_rel's pathlist that implements all behavior of the query.
	 * Note: we would already have accounted for the query's FOR UPDATE/SHARE
	 * (if any) before we get here.
	 */
	if (!extra->limit_needed)
	{
		ListCell   *lc;

		Assert(parse->rowMarks);

		/*
		 * Grouping and aggregation are not supported with FOR UPDATE/SHARE,
		 * so the input_rel should be a base, join, or ordered relation; and
		 * if it's an ordered relation, its input relation should be a base or
		 * join relation.
		 */
		Assert(input_rel->reloptkind == RELOPT_BASEREL ||
			   input_rel->reloptkind == RELOPT_JOINREL ||
			   (input_rel->reloptkind == RELOPT_UPPER_REL &&
				ifpinfo->stage == UPPERREL_ORDERED &&
				(ifpinfo->outerrel->reloptkind == RELOPT_BASEREL ||
				 ifpinfo->outerrel->reloptkind == RELOPT_JOINREL)));

		foreach(lc, input_rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);

			/*
			 * apply_scanjoin_target_to_paths() uses create_projection_path()
			 * to adjust each of its input paths if needed, whereas
			 * create_ordered_paths() uses apply_projection_to_path() to do
			 * that.  So the former might have put a ProjectionPath on top of
			 * the ForeignPath; look through ProjectionPath and see if the
			 * path underneath it is ForeignPath.
			 */
			if (IsA(path, ForeignPath) ||
				(IsA(path, ProjectionPath) &&
				 IsA(((ProjectionPath *) path)->subpath, ForeignPath)))
			{
				/*
				 * Create foreign final path; this gets rid of a
				 * no-longer-needed outer plan (if any), which makes the
				 * EXPLAIN output look cleaner
				 */
#if PG_VERSION_NUM >= 170000
				final_path = create_foreign_upper_path(root,
													   path->parent,
													   path->pathtarget,
													   path->rows,
													   path->startup_cost,
													   path->total_cost,
													   path->pathkeys,
													   NULL,	/* no extra plan */
													   NIL,		/* no fdw_restrictinfo list */
													   NIL);	/* no fdw_private */
#else
				final_path = create_foreign_upper_path(root,
													   path->parent,
													   path->pathtarget,
													   path->rows,
													   path->startup_cost,
													   path->total_cost,
													   path->pathkeys,
													   NULL,	/* no extra plan */
													   NIL);	/* no fdw_private */
#endif

				/* and add it to the final_rel */
				add_path(final_rel, (Path *) final_path);

				/* Safe to push down */
				fpinfo->pushdown_safe = true;

				return;
			}
		}

		/*
		 * If we get here it means no ForeignPaths; since we would already
		 * have considered pushing down all operations for the query to the
		 * remote server, give up on it.
		 */
		return;
	}

	Assert(extra->limit_needed);

	/*
	 * If the input_rel is an ordered relation, replace the input_rel with its
	 * input relation
	 */
	if (input_rel->reloptkind == RELOPT_UPPER_REL &&
		ifpinfo->stage == UPPERREL_ORDERED)
	{
		input_rel = ifpinfo->outerrel;
		ifpinfo = (MySQLFdwRelationInfo *) input_rel->fdw_private;
		has_final_sort = true;
		pathkeys = root->sort_pathkeys;
	}

	/* The input_rel should be a base, join, or grouping relation */
	Assert(input_rel->reloptkind == RELOPT_BASEREL ||
		   input_rel->reloptkind == RELOPT_JOINREL ||
		   (input_rel->reloptkind == RELOPT_UPPER_REL &&
			ifpinfo->stage == UPPERREL_GROUP_AGG));

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying base, join, or grouping relation to perform the final
	 * sort (if has_final_sort) and the LIMIT restriction remotely, which is
	 * stored into the fdw_private list of the resulting path.  (We
	 * re-estimate the costs of sorting the underlying relation, if
	 * has_final_sort.)
	 */

	/*
	 * Assess if it is safe to push down the LIMIT and OFFSET to the remote
	 * server
	 */

	/*
	 * If the underlying relation has any local conditions, the LIMIT/OFFSET
	 * cannot be pushed down.
	 */
	if (ifpinfo->local_conds)
		return;

	/*
	 * Support only Const and Param nodes as expressions are NOT suported.
	 * MySQL doesn't support LIMIT/OFFSET NULL/ALL syntax, so check for the
	 * same.  If limitCount const node is null then do not pushdown
	 * limit/offset clause and if limitOffset const node is null and
	 * limitCount const node is not null then pushdown only limit clause.
	 */
	if (parse->limitCount)
	{
		if (nodeTag(parse->limitCount) != T_Const &&
			nodeTag(parse->limitCount) != T_Param)
			return;

		if (nodeTag(parse->limitCount) == T_Const &&
			((Const *) parse->limitCount)->constisnull)
			return;
	}
	if (parse->limitOffset)
	{
		if (nodeTag(parse->limitOffset) != T_Const &&
			nodeTag(parse->limitOffset) != T_Param)
			return;
	}

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* TODO: Put accurate estimates */
	startup_cost = 1;
	total_cost = 1 + startup_cost;
	rows = 1;

	/*
	 * Build the fdw_private list that will be used by mysqlGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeInteger(has_final_sort),
							 makeInteger(extra->limit_needed));

	/*
	 * Create foreign final path; this gets rid of a no-longer-needed outer
	 * plan (if any), which makes the EXPLAIN output look cleaner
	 */
#if PG_VERSION_NUM >= 170000
	final_path = create_foreign_upper_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
										   startup_cost,
										   total_cost,
										   pathkeys,
										   NULL,	/* no extra plan */
										   NIL,		/* no fdw_restrictinfo list */
										   fdw_private);
#else
	final_path = create_foreign_upper_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
										   startup_cost,
										   total_cost,
										   pathkeys,
										   NULL,	/* no extra plan */
										   fdw_private);
#endif

	/* and add it to the final_rel */
	add_path(final_rel, (Path *) final_path);
}

#if PG_VERSION_NUM >= 140000
/*
 * mysql_remove_quotes
 *
 * Return the string by replacing back-tick (`) characters with double quotes
 * (").  If there are two consecutive back-ticks, the first is the escape
 * character which is removed.  Caller should free the allocated memory.
 */
static char *
mysql_remove_quotes(char *s1)
{
	int			i,
				j;
	char	   *s2;

	if (s1 == NULL)
		return NULL;

	s2 = palloc0(strlen(s1) * 2);

	for (i = 0, j = 0; s1[i] != '\0'; i++, j++)
	{
		if (s1[i] == '`' && s1[i + 1] == '`')
		{
			s2[j] = '`';
			i++;
		}
		else if (s1[i] == '`')
			s2[j] = '"';
		else if (s1[i] == '"')
		{
			/* Double the inner double quotes for PG compatibility. */
			s2[j] = '"';
			s2[j + 1] = '"';
			j++;
		}
		else
			s2[j] = s1[i];
	}

	s2[j] = '\0';

	return s2;
}
#endif

/*
 * mysql_get_sortby_direction_string
 *		Fetch the operator oid from the operator family and datatype, and check
 *		whether the operator is the default for sort expr's datatype. If it is,
 *		then return ASC or DESC accordingly; NULL otherwise.
 */
char *
mysql_get_sortby_direction_string(EquivalenceMember *em, PathKey *pathkey, HTAB* pushability_hash)
{
	Oid			oprid;
	TypeCacheEntry *typentry;

	if (em == NULL)
		return NULL;

	/* Can't push down the sort if pathkey's opfamily is not shippable. */
	if (!mysql_is_builtin(pathkey->pk_opfamily))
		return NULL;

	oprid = get_opfamily_member(pathkey->pk_opfamily, em->em_datatype,
								em->em_datatype, pathkey->pk_strategy);

	if (!OidIsValid(oprid))
		elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
			 pathkey->pk_strategy, em->em_datatype, em->em_datatype,
			 pathkey->pk_opfamily);

	/* Can't push down the sort if the operator is not shippable. */
	if (!mysql_check_remote_pushability(pushability_hash, oprid))
		return NULL;

	/*
	 * See whether the operator is default < or > for sort expr's datatype.
	 * Here we need to use the expression's actual type to discover whether
	 * the desired operator will be the default or not.
	 */
	typentry = lookup_type_cache(exprType((Node *) em->em_expr),
								 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	if (oprid == typentry->lt_opr)
		return "ASC";
	else if (oprid == typentry->gt_opr)
		return "DESC";

	return NULL;
}

#if PG_VERSION_NUM >= 160000
/*
 * mysql_tlist_member_match_var
 *	  Finds the (first) member of the given tlist whose Var is same as the
 *	  given Var.  Result is NULL if no such member.
 */
static TargetEntry *
mysql_tlist_member_match_var(Var *var, List *targetlist)
{
	ListCell   *temp;

	foreach(temp, targetlist)
	{
		TargetEntry *tlentry = (TargetEntry *) lfirst(temp);
		Var		   *tlvar = (Var *) tlentry->expr;

		if (!tlvar || !IsA(tlvar, Var))
			continue;
		if (var->varno == tlvar->varno &&
			var->varattno == tlvar->varattno &&
			var->varlevelsup == tlvar->varlevelsup &&
			var->vartype == tlvar->vartype)
			return tlentry;
	}

	return NULL;
}

/*
 * mysql_varlist_append_unique_var
 * 		Append var to var list, but only if it isn't already in the list.
 *
 * Whether a var is already a member of list is determined using varno and
 * varattno.
 */
static List *
mysql_varlist_append_unique_var(List *varlist, Var *var)
{
	ListCell   *lc;

	foreach(lc, varlist)
	{
		Var		   *tlvar = (Var *) lfirst(lc);

		if (IsA(tlvar, Var) &&
			tlvar->varno == var->varno &&
			tlvar->varattno == var->varattno)
			return varlist;
	}

	return lappend(varlist, var);
}
#endif
