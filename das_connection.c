#include "postgres.h"

#if PG_VERSION_NUM >= 130000
#include "common/hashfn.h"
#endif
#include "das_fdw.h"
#include "grpc_client.h"
#include "das_pushability.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

/* Length of host */
#define HOST_LEN 256

/*
 * Connection cache hash table entry
 *
 * The lookup key in this hash table is the foreign server OID plus the user
 * mapping OID.  (We use just one connection per user per foreign server,
 * so that we can ensure all scans use the same snapshot during a query.)
 */
typedef struct ConnCacheKey
{
	Oid			serverid;		/* OID of foreign server */
	Oid			userid;			/* OID of local user whose mapping we use */
} ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;			/* hash key (must be first) */
	DAS  	   *conn;			/* connection to foreign server, or NULL */
	bool		invalidated;	/* true if reconnect is pending */
	uint32		server_hashvalue;	/* hash value of foreign server OID */
	uint32		mapping_hashvalue;	/* hash value of user mapping OID */
} ConnCacheEntry;

/*
 * Connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

static DAS* das_init(das_opt* opts);
static void das_close(DAS *conn);
static void das_inval_callback(Datum arg, int cacheid, uint32 hashvalue);

/*
 * das_get_connection:
 * 		Get a connection which can be used to execute queries on the remote
 * 		DAS server with the user's authorization.  A new connection is
 * 		established if we don't already have a suitable one.
 */
DAS *
das_get_connection(ForeignServer *server, UserMapping *user, das_opt *opt)
{
	bool		found;
	ConnCacheEntry *entry;
	ConnCacheKey key;

	/* First time through, initialize connection cache hashtable */
	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
		ctl.hash = tag_hash;

		/* Allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("das_fdw connections", 8,
									 &ctl,
									 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

		/*
		 * Register some callback functions that manage connection cleanup.
		 * This should be done just once in each backend.
		 */
		CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
									  das_inval_callback, (Datum) 0);
		CacheRegisterSyscacheCallback(USERMAPPINGOID,
									  das_inval_callback, (Datum) 0);
	}

	/* Create hash key for the entry.  Assume no pad bytes in key struct */
	key.serverid = server->serverid;
	key.userid = user->userid;

	/*
	 * Find or create cached entry for requested connection.
	 */
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		/* Initialize new hashtable entry (key is already filled in) */
		entry->conn = NULL;
	}

	/* If an existing entry has invalid connection then release it */
	if (entry->conn != NULL && entry->invalidated)
	{
		elog(DEBUG3, "disconnecting das_fdw connection %p for option changes to take effect",
			 entry->conn);
        // Nothing to actually disconnect here...
		//das_close(entry->conn);
		entry->conn = NULL;
	}

	if (entry->conn == NULL)
	{
		entry->conn = das_fdw_connect(opt);
		elog(DEBUG3, "new das_fdw connection %p for server \"%s\"",
			 entry->conn, server->servername);

		/*
		 * Once the connection is established, then set the connection
		 * invalidation flag to false, also set the server and user mapping
		 * hash values.
		 */
		entry->invalidated = false;
		entry->server_hashvalue =
			GetSysCacheHashValue1(FOREIGNSERVEROID,
								  ObjectIdGetDatum(server->serverid));

		entry->mapping_hashvalue =
			GetSysCacheHashValue1(USERMAPPINGOID,
								  ObjectIdGetDatum(user->umid));
	}
	return entry->conn;
}

/*
 * das_cleanup_connection:
 * 		Delete all the cache entries on backend exists.
 */
void
das_cleanup_connection(void)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	if (ConnectionHash == NULL)
		return;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		if (entry->conn == NULL)
			continue;

		elog(DEBUG3, "disconnecting das_fdw connection %p", entry->conn);
        // Nothing to actually disconnect here...
		//das_close(entry->conn);
		entry->conn = NULL;
	}
}

/*
 * Release connection created by calling das_get_connection.
 */
void
das_release_connection(DAS *conn)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	if (ConnectionHash == NULL)
		return;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		if (entry->conn == NULL)
			continue;

		if (entry->conn == conn)
		{
			elog(DEBUG3, "disconnecting das_fdw connection %p", entry->conn);
            // Nothing to actually disconnect here...
	    	//das_close(entry->conn);
        	entry->conn = NULL;
			hash_seq_term(&scan);
			break;
		}
	}
}

DAS *
das_fdw_connect(das_opt *opt)
{
	DAS 	   *conn;

	/* Connect to the server */
	conn = das_init(opt);
	if (!conn)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				 errmsg("failed to initialise the DAS connection object")));

	return conn;
}

/*
 * Connection invalidation callback function for DAS.
 *
 * After a change to a pg_foreign_server or pg_user_mapping catalog entry,
 * mark connections depending on that entry as needing to be remade. This
 * implementation is similar as pgfdw_inval_callback.
 */
static void
das_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	Assert(cacheid == FOREIGNSERVEROID || cacheid == USERMAPPINGOID);

	/* ConnectionHash must exist already, if we're registered */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore invalid entries */
		if (entry->conn == NULL)
			continue;

		/* hashvalue == 0 means a cache reset, must clear all state */
		if (hashvalue == 0 ||
			(cacheid == FOREIGNSERVEROID &&
			 entry->server_hashvalue == hashvalue) ||
			(cacheid == USERMAPPINGOID &&
			 entry->mapping_hashvalue == hashvalue))
			entry->invalidated = true;
	}
}

static DAS *
das_init(das_opt* opts)
{
    DAS *conn;
	char   **pushability_list;
	int		pushability_len;
	HTAB   *pushability_hash;
    bool    orderby_supported;
	bool	join_supported;
	bool	aggregation_supported;

    elog(WARNING, "das_url is %s", opts->das_url);

    pushability_list = get_operations_supported(opts, &orderby_supported, &join_supported, &aggregation_supported, &pushability_len);

	pushability_hash = populate_pushability_hash_from_list(CacheMemoryContext, pushability_list, pushability_len);
	// pushability_hash = populate_pushability_hash();
    free_operations_supported(pushability_list, pushability_len);

    conn = (DAS *) palloc0(sizeof(DAS));
    conn->das_url = pstrdup(opts->das_url);
    conn->das_type = pstrdup(opts->das_type);
	conn->das_id = pstrdup(opts->das_id);
    conn->pushability_hash = pushability_hash;
	conn->orderby_supported = orderby_supported;
	conn->join_supported = join_supported;
	conn->aggregation_supported = aggregation_supported;

    return conn;
}

/*
 * Close the DAS connection.
 */
static void
das_close(DAS *conn)
{
    pfree(conn);
}