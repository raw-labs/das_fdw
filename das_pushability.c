/*-------------------------------------------------------------------------
 *
 * das_pushability.c
 *		routines for FDW pushability
 *
 * Portions Copyright (c) 2022-2024, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 *		das_pushability.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/string.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "das_pushability.h"
#include "storage/fd.h"
#include "utils/fmgrprotos.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

static char *get_config_filename(void);
static void config_invalid_error_callback(void *arg);
static bool get_line_buf(FILE *stream, StringInfo buf);

/*
 * get_config_filename
 * 		Returns the path for the pushdown object configuration file for the
 * 		foreign-data wrapper.
 */
static char *
get_config_filename(void)
{
	char		sharepath[MAXPGPATH];
	char	   *result;

	get_share_path(my_exec_path, sharepath);
	result = (char *) palloc(MAXPGPATH);
	snprintf(result, MAXPGPATH, "%s/extension/%s_pushdown.config", sharepath,
			 FDW_MODULE_NAME);

	return result;
}

/*
 * das_check_remote_pushability
 * 		Lookups into hash table by forming the hash key from provided object
 * 		oid.
 */
bool
das_check_remote_pushability(HTAB *pushability_hash, Oid object_oid)
{
	bool		found = false;

	Assert(pushability_hash != NULL);

	hash_search(pushability_hash, &object_oid, HASH_FIND, &found);

	return found;
}

/*
 * populate_pushability_hash
 * 		Creates the hash table and populates the hash entries by reading the
 * 		pushdown object configuration file.
 */
HTAB *
populate_pushability_hash()
{
	FILE	   *file = NULL;
	char	   *config_filename;
	HASHCTL		ctl;
	ErrorContextCallback errcallback;
	unsigned int line_no = 0;
	StringInfoData linebuf;
	HTAB	   *hash;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(FDWPushdownObject);
	ctl.hcxt = CurrentMemoryContext;

	/* Create the hash table */
	hash = hash_create("das_fdw push elements hash", 256,
					   &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Get the config file name */
	config_filename = get_config_filename();

	file = AllocateFile(config_filename, PG_BINARY_R);

	if (file == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open \"%s\": %m", config_filename)));

	initStringInfo(&linebuf);

	/*
	 * Read the pushdown object configuration file and push object information
	 * to the in-memory hash table for a faster lookup.
	 */
	while (get_line_buf(file, &linebuf))
	{
		FDWPushdownObject *entry;
		Oid			objectId;
		ObjectType	objectType;
		bool		found;
		char	   *str;

		line_no++;

		/* If record starts with #, then consider as comment. */
		if (linebuf.data[0] == '#')
			continue;

		/* Ignore if all blank */
		if (strspn(linebuf.data, " \t\r\n") == linebuf.len)
			continue;

		/* Strip trailing newline, including \r in case we're on Windows */
		while (linebuf.len > 0 && (linebuf.data[linebuf.len - 1] == '\n' ||
								   linebuf.data[linebuf.len - 1] == '\r'))
			linebuf.data[--linebuf.len] = '\0';

		/* Strip leading whitespaces. */
		str = linebuf.data;
		while (isspace(*str))
			str++;

		if (pg_strncasecmp(str, "ROUTINE", 7) == 0)
		{
			/* Move over ROUTINE */
			str = str + 7;

			/* Move over any whitespace  */
			while (isspace(*str))
				str++;

			objectType = OBJECT_FUNCTION;
			objectId =
				DatumGetObjectId(DirectFunctionCall1(regprocedurein,
													 CStringGetDatum(str)));
		}
		else if (pg_strncasecmp(str, "OPERATOR", 8) == 0)
		{
			/* Move over OPERATOR */
			str = str + 8;

			/* Move over any whitespace  */
			while (isspace(*str))
				str++;

			objectType = OBJECT_OPERATOR;
			objectId =
				DatumGetObjectId(DirectFunctionCall1(regoperatorin,
													 CStringGetDatum(str)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid object type in configuration file at line number: %d",
							line_no),
					 errhint("Valid values are: \"ROUTINE\", \"OPERATOR\".")));

		/* Insert the new element to the hash table */
		entry = hash_search(hash, &objectId, HASH_ENTER, &found);

		/* Two different objects cannot have the same system object id */
		if (found && entry->objectType != objectType)
			elog(ERROR, "different pushdown objects have the same oid \"%d\"",
				 objectId);

		entry->objectType = objectType;
	}

	if (ferror(file))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", config_filename)));

	error_context_stack = errcallback.previous;

	pfree(linebuf.data);

	FreeFile(file);

	return hash;
}

/*
 * populate_pushability_hash
 * 		Creates the hash table and populates the hash entries by receiving a
 * 		list of pushdown object strings and their count.
 */
HTAB *
populate_pushability_hash_from_list(MemoryContext htab_ctx, char **pushability_list, int pushability_len)
{
	HASHCTL		ctl;
	unsigned int line_no = 0;
	HTAB	   *hash;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(FDWPushdownObject);
	ctl.hcxt = htab_ctx;

	/* Create the hash table */
	hash = hash_create("das_fdw push elements hash", 256,
					   &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/*
	 * Loop over the pushability_list and process each entry as was done
	 * previously when reading from the file.
	 */
	for (line_no = 0; line_no < pushability_len; line_no++)
	{
		FDWPushdownObject *entry;
		Oid			objectId;
		ObjectType	objectType;
		bool		found;
		char	   *str;

		char *line = pushability_list[line_no];

		/* If record starts with #, then consider as comment. */
		if (line[0] == '#')
			continue;

		/* Ignore if all blank */
		if (strspn(line, " \t\r\n") == strlen(line))
			continue;

		/* Strip trailing newline or carriage return characters */
		size_t len = strlen(line);
		while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
			line[--len] = '\0';

		/* Strip leading whitespaces. */
		str = line;
		while (isspace(*str))
			str++;

		elog(WARNING, "Processing line: '%s'", str);

		if (pg_strncasecmp(str, "ROUTINE", 7) == 0)
		{
			/* Move over ROUTINE */
			str = str + 7;

			/* Move over any whitespace  */
			while (isspace(*str))
				str++;

			objectType = OBJECT_FUNCTION;
			objectId =
				DatumGetObjectId(DirectFunctionCall1(regprocedurein,
													 CStringGetDatum(str)));
		}
		else if (pg_strncasecmp(str, "OPERATOR", 8) == 0)
		{
			/* Move over OPERATOR */
			str = str + 8;

			/* Move over any whitespace  */
			while (isspace(*str))
				str++;

			objectType = OBJECT_OPERATOR;
			objectId =
				DatumGetObjectId(DirectFunctionCall1(regoperatorin,
													 CStringGetDatum(str)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid object type in pushability list at line number: %d",
							line_no + 1),
					 errhint("Valid values are: \"ROUTINE\", \"OPERATOR\".")));

		/* Insert the new element to the hash table */
		entry = hash_search(hash, &objectId, HASH_ENTER, &found);

		/* Two different objects cannot have the same system object id */
		if (found && entry->objectType != objectType)
			elog(ERROR, "different pushdown objects have the same oid \"%d\"",
				 objectId);

		entry->objectType = objectType;
	}

	return hash;
}

/*
 * get_line_buf
 * 		Returns true if a line was successfully collected (including
 * 		the case of a non-newline-terminated line at EOF).
 *
 * Returns false if there was an I/O error or no data was available
 * before EOF. In the false-result case, buf is reset to empty.
 * (Borrowed the code from pg_get_line_buf().)
 */
static bool
get_line_buf(FILE *stream, StringInfo buf)
{
	int			orig_len;

	/* We just need to drop any data from the previous call */
	resetStringInfo(buf);

	orig_len = buf->len;

	/* Read some data, appending it to whatever we already have */
	while (fgets(buf->data + buf->len, buf->maxlen - buf->len, stream) != NULL)
	{
		buf->len += strlen(buf->data + buf->len);

		/* Done if we have collected a newline */
		if (buf->len > orig_len && buf->data[buf->len - 1] == '\n')
			return true;

		/* Make some more room in the buffer, and loop to read more data */
		enlargeStringInfo(buf, 128);
	}

	/* Check for I/O errors and EOF */
	if (ferror(stream) || buf->len == orig_len)
	{
		/* Discard any data we collected before detecting error */
		buf->len = orig_len;
		buf->data[orig_len] = '\0';
		return false;
	}

	/* No newline at EOF, but we did collect some data */
	return true;
}