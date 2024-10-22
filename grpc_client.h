#ifndef GRPC_CLIENT_H
#define GRPC_CLIENT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <postgres.h>
#include "nodes/pg_list.h"

/*
 * Options structure to store the MySQL
 * server information
 */
typedef struct mysql_opt
{
	List* option_keys;
	List* option_values;

	/* DAS options */
	char       *das_url;
    char       *das_type;
	char       *das_id;
} mysql_opt;

//////////////////////////////////////////////////////////////////////////////////////////
// Registration Service
//////////////////////////////////////////////////////////////////////////////////////////

/* Register DAS */
char* register_das(mysql_opt* opts);

/* Unregister DAS */
void unregister_das(mysql_opt* opts);

/* Get supported operations */
char** get_operations_supported(mysql_opt* opts, bool* orderby_supported, bool* join_supported, bool* aggregation_supported, int* pushability_len);
void free_operations_supported(char** pushability_list, int pushability_len);

//////////////////////////////////////////////////////////////////////////////////////////
// Table Definitions Service
//////////////////////////////////////////////////////////////////////////////////////////

/* Get table definitions */
char** get_table_definitions(mysql_opt* opts, const char* server_name, int* num_tables);

//////////////////////////////////////////////////////////////////////////////////////////
// SQL Query Service
//////////////////////////////////////////////////////////////////////////////////////////

void get_query_estimate(mysql_opt* opts, const char* sql, double* rows, double* width);

typedef struct SqlQueryIterator SqlQueryIterator;

/* Initialize the iterator */
SqlQueryIterator* sql_query_iterator_init(mysql_opt* opts, const char* sql, const char* plan_id);

/* Get the next Rows */
bool sql_query_iterator_next(SqlQueryIterator* iterator, int* attnums, Datum* dvalues, bool* nulls, Oid* pgtypes, int32* pgtypmods);

/* Close the iterator */
void sql_query_iterator_close(SqlQueryIterator* iterator);

//////////////////////////////////////////////////////////////////////////////////////////
// Table Update Service
//////////////////////////////////////////////////////////////////////////////////////////

// add das_type and option_keys and option_values to all methods. so that we can re-register in case of failure.
// maybe create a structure to hold all that.
// remembering that das_type is somewhat optional.
// it must take the thing that register_das needs

/* Get the unique column name for modify operations */
char* unique_column(mysql_opt* opts, const char* table_name);

/* Insert a row into the table */
void insert_row(mysql_opt* opts, const char* table_name, int num_columns, int* attnums, char **attnames, Datum* dvalues, bool* nulls, Oid* pgtypes, int32* pgtypmods);

/* Update a row in the table */
void update_row(mysql_opt* opts, const char* table_name, Datum k_value, Oid k_pgtype, int32 k_pgtypmods, int num_columns, int* attnums, char **attnames, Datum* dvalues, bool* nulls, Oid* pgtypes, int32* pgtypmods);

/* Delete a row from the table */
void delete_row(mysql_opt* opts, const char* table_name, Datum k_value, Oid k_pgtype, int32 k_pgtypmods);

#ifdef __cplusplus
}
#endif

#endif  /* GRPC_CLIENT_H */