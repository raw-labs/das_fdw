/*-------------------------------------------------------------------------
 *
 * das_pushability.h
 *		prototypes for das_pushability.c
 *
 * Portions Copyright (c) 2022-2024, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 *		das_pushability.h
 *-------------------------------------------------------------------------
 */
#ifndef das_PUSHABILITY_H
#define das_PUSHABILITY_H

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"

/*
 * NB: Module name must be the same as the MODULE_big configure in the Makefile
 * of FDW contrib module. Otherwise, the pushdown object configuration file will
 * not be located correctly.
 */
#define FDW_MODULE_NAME "das_fdw"

/* Structure to help hold the pushdown object in the hash table */
typedef struct FDWPushdownObject
{
	Oid			objectId;
	ObjectType	objectType;
} FDWPushdownObject;

extern bool das_check_remote_pushability(HTAB *pushability_hash, Oid object_oid);
extern HTAB *populate_pushability_hash();
extern HTAB *populate_pushability_hash_from_list(MemoryContext htab_ctx, char **pushability_list, int pushability_len);

#endif							/* das_PUSHABILITY_H */
