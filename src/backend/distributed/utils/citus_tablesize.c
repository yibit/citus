/*
 * citus_tablesize.c
 *   Functions which return the sizes of distributed tables
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 */
#include "postgres.h"
#include "fmgr.h"

#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"

#include "distributed/multi_planner.h"
#include "distributed/metadata_cache.h"

static Node * replace_pg_table_size_mutator(Node *node, void *context);

PG_FUNCTION_INFO_V1(citus_table_size);

Datum
citus_table_size(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	if (!IsDistributedTable(relationId))
	{
		return DirectFunctionCall1(pg_table_size, relationId);
	}

	ereport(WARNING, (errmsg("citus_table_size does not support distributed tables")));
	return Int64GetDatum(10);
}


/*
 * walks the query and replaces all calls to pg_catalog.pg_table_size with calls
 * to the ablve citus_table_size
 */
Query *
replace_pg_table_size_calls(Query *query)
{
	return query_tree_mutator(query, replace_pg_table_size_mutator, NULL,
							  QTW_DONT_COPY_QUERY | QTW_EXAMINE_RTES);
}


static Node *
replace_pg_table_size_mutator(Node *node, void *context)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, Query))
	{
		return (Node *) replace_pg_table_size_calls((Query *) node);
	}

	if (IsA(node, FuncExpr))
	{
		FuncExpr *expr = (FuncExpr *) node;

		/* hard code pg_table_size's Oid for improved performance */
		if (expr->funcid == 2997)
		{
			Value *schema = makeString(pstrdup("pg_catalog"));
			Value *func = makeString(pstrdup("citus_table_size"));
			List *funcname = list_make2(schema, func);
			Oid regClass = REGCLASSOID;
			Oid citusFunc = LookupFuncName(funcname, 1, &regClass, true);

			if (citusFunc == InvalidOid)
			{
				ereport(WARNING, (errmsg("citus_table_size does not exist")));
			}
			else
			{
				expr->funcid = citusFunc;
			}
		}

		/* fall through so we continue walking things like the args */
	}

	return expression_tree_mutator(node, replace_pg_table_size_mutator, context);
}
