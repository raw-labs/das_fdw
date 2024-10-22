/*-------------------------------------------------------------------------
 *
 * mysql_pushability.h
 *		prototypes for mysql_pushability.c
 *
 * Portions Copyright (c) 2022-2024, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 *		mysql_pushability.h
 *-------------------------------------------------------------------------
 */
#ifndef MYSQL_PUSHABILITY_H
#define MYSQL_PUSHABILITY_H

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"

/*
 * NB: Module name must be the same as the MODULE_big configure in the Makefile
 * of FDW contrib module. Otherwise, the pushdown object configuration file will
 * not be located correctly.
 */
#define FDW_MODULE_NAME "mysql_fdw"

/* Structure to help hold the pushdown object in the hash table */
typedef struct FDWPushdownObject
{
	Oid			objectId;
	ObjectType	objectType;
} FDWPushdownObject;

extern bool mysql_check_remote_pushability(HTAB *pushability_hash, Oid object_oid);
extern HTAB *populate_pushability_hash();
extern HTAB *populate_pushability_hash_from_list(MemoryContext htab_ctx, char **pushability_list, int pushability_len);
// extern List * mysql_get_configured_pushdown_objects();

#endif							/* MYSQL_PUSHABILITY_H */
